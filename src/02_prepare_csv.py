from pathlib import Path
import pandas as pd
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IMPORT_DIR = PROJECT_ROOT / "neo4j" / "import"
CLEAN_DIR = PROJECT_ROOT / "neo4j" / "load_csv"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def read_csv_safe(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def clean_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def normalize_int_like(x):
    x = clean_str(x)
    if not x:
        return ""
    try:
        return str(int(float(x)))
    except ValueError:
        return ""


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates().reset_index(drop=True)


def is_valid_paper_id(paper_id: str) -> bool:
    paper_id = clean_str(paper_id)
    if not paper_id:
        return False
    if paper_id.startswith("dblpnote/"):
        return False
    return True


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df[cols].copy()


def write_clean_csv(df: pd.DataFrame, path: Path):
    df.to_csv(
        path,
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
    )


def collapse_by_id_keep_non_empty(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    df = df.fillna("").copy()

    def pick(series):
        for v in series:
            if str(v).strip() != "":
                return v
        return ""

    agg_map = {col: pick for col in df.columns if col != id_col}
    collapsed = df.groupby(id_col, as_index=False).agg(agg_map)
    return collapsed.reset_index(drop=True)


def main():
    print("Reading intermediate CSV files...")

    papers = read_csv_safe(IMPORT_DIR / "papers.csv")
    authors = read_csv_safe(IMPORT_DIR / "authors.csv")
    journals = read_csv_safe(IMPORT_DIR / "journals.csv")
    volumes = read_csv_safe(IMPORT_DIR / "volumes.csv")
    editions = read_csv_safe(IMPORT_DIR / "editions.csv")
    events = read_csv_safe(IMPORT_DIR / "events.csv")

    wrote = read_csv_safe(IMPORT_DIR / "wrote.csv")
    published_in_volume = read_csv_safe(IMPORT_DIR / "published_in_volume.csv")
    published_in_edition = read_csv_safe(IMPORT_DIR / "published_in_edition.csv")
    belongs_to = read_csv_safe(IMPORT_DIR / "belongs_to.csv")
    is_edition_of = read_csv_safe(IMPORT_DIR / "is_edition_of.csv")

    print("Original sizes:")
    print(f"  papers: {len(papers)}")
    print(f"  authors: {len(authors)}")
    print(f"  journals: {len(journals)}")
    print(f"  volumes: {len(volumes)}")
    print(f"  editions: {len(editions)}")
    print(f"  events: {len(events)}")
    print(f"  wrote: {len(wrote)}")
    print(f"  published_in_volume: {len(published_in_volume)}")
    print(f"  published_in_edition: {len(published_in_edition)}")
    print(f"  belongs_to: {len(belongs_to)}")
    print(f"  is_edition_of: {len(is_edition_of)}")

    # -------------------------
    # 1. Clean papers
    # -------------------------
    print("Cleaning papers...")
    papers = papers.rename(columns={
        "paperId:ID(Paper)": "paperId",
        "year:int": "year",
    })

    paper_cols = ["paperId", "title", "year", "pages", "doi", "paperType", "dblpUrl"]
    papers = ensure_columns(papers, paper_cols)

    for col in paper_cols:
        papers[col] = papers[col].map(clean_str)

    papers["year"] = papers["year"].map(normalize_int_like)
    papers["title"] = papers["title"].str.replace("\n", " ", regex=False)
    papers["title"] = papers["title"].str.replace("\r", " ", regex=False)
    papers["title"] = papers["title"].str.replace("\t", " ", regex=False)
    papers["title"] = papers["title"].str.replace("\u2028", " ", regex=False)
    papers["title"] = papers["title"].str.replace("\u2029", " ", regex=False)
    papers["title"] = papers["title"].str.replace('"', "'", regex=False)

    papers = papers[papers["paperId"].map(is_valid_paper_id)]
    papers = papers[papers["title"] != ""]
    papers = dedup(papers)

    valid_paper_ids = set(papers["paperId"].tolist())

    # -------------------------
    # 2. Clean authors
    # -------------------------
    print("Cleaning authors...")
    authors = authors.rename(columns={
        "authorName:ID(Author)": "authorName",
    })

    authors = ensure_columns(authors, ["authorName"])
    authors["authorName"] = authors["authorName"].map(clean_str)
    authors = authors[authors["authorName"] != ""]
    authors = dedup(authors)

    valid_author_names = set(authors["authorName"].tolist())

    # -------------------------
    # 3. Clean journals
    # -------------------------
    print("Cleaning journals...")
    journals = journals.rename(columns={
        "journalName:ID(Journal)": "journalName",
    })

    journals = ensure_columns(journals, ["journalName"])
    journals["journalName"] = journals["journalName"].map(clean_str)
    journals = journals[journals["journalName"] != ""]
    journals = dedup(journals)

    valid_journal_names = set(journals["journalName"].tolist())

    # -------------------------
    # 4. Clean volumes
    # -------------------------
    print("Cleaning volumes...")
    volumes = volumes.rename(columns={
        "volumeId:ID(Volume)": "volumeId",
        "year:int": "year",
    })

    volume_cols = ["volumeId", "volume", "number", "year"]
    volumes = ensure_columns(volumes, volume_cols)

    for col in ["volumeId", "volume", "number"]:
        volumes[col] = volumes[col].map(clean_str)
    volumes["year"] = volumes["year"].map(normalize_int_like)

    volumes = volumes[volumes["volumeId"] != ""]
    volumes = dedup(volumes)

    valid_volume_ids = set(volumes["volumeId"].tolist())

    # -------------------------
    # 5. Clean editions
    # -------------------------
    print("Cleaning editions...")
    editions = editions.rename(columns={
        "editionId:ID(Edition)": "editionId",
        "year:int": "year",
    })

    edition_cols = ["editionId", "booktitle", "title", "year", "city", "publisher", "isbn", "ee"]
    editions = ensure_columns(editions, edition_cols)

    for col in ["editionId", "booktitle", "title", "city", "publisher", "isbn", "ee"]:
        editions[col] = editions[col].map(clean_str)
    editions["year"] = editions["year"].map(normalize_int_like)

    editions = editions[editions["editionId"] != ""]
    editions = collapse_by_id_keep_non_empty(editions, "editionId")
    editions = dedup(editions)

    valid_edition_ids = set(editions["editionId"].tolist())

    # -------------------------
    # 6. Clean events
    # -------------------------
    print("Cleaning events...")
    events = events.rename(columns={
        "eventName:ID(Event)": "eventName",
    })

    event_cols = ["eventName", "type"]
    events = ensure_columns(events, event_cols)

    for col in event_cols:
        events[col] = events[col].map(clean_str)

    events = events[events["eventName"] != ""]
    events = dedup(events)

    valid_event_names = set(events["eventName"].tolist())

    # -------------------------
    # 7. Clean wrote relationships
    # -------------------------
    print("Cleaning wrote relationships...")
    wrote = wrote.rename(columns={
        ":START_ID(Author)": "authorName",
        ":END_ID(Paper)": "paperId",
        "authorOrder:int": "authorOrder",
    })

    wrote = ensure_columns(wrote, ["authorName", "paperId", "authorOrder"])
    wrote["authorName"] = wrote["authorName"].map(clean_str)
    wrote["paperId"] = wrote["paperId"].map(clean_str)
    wrote["authorOrder"] = wrote["authorOrder"].map(normalize_int_like)

    wrote = wrote[
        (wrote["authorName"] != "")
        & (wrote["paperId"] != "")
        & (wrote["authorName"].isin(valid_author_names))
        & (wrote["paperId"].isin(valid_paper_ids))
    ]
    wrote = dedup(wrote)

    # Remove papers without authors
    papers_with_authors = set(wrote["paperId"].tolist())
    papers = papers[papers["paperId"].isin(papers_with_authors)].copy()
    papers = dedup(papers)

    valid_paper_ids = set(papers["paperId"].tolist())
    wrote = wrote[wrote["paperId"].isin(valid_paper_ids)].copy()
    wrote = dedup(wrote)

    used_authors = set(wrote["authorName"].tolist())
    authors = authors[authors["authorName"].isin(used_authors)].copy()
    authors = dedup(authors)

    valid_author_names = set(authors["authorName"].tolist())
    wrote = wrote[wrote["authorName"].isin(valid_author_names)].copy()
    wrote = dedup(wrote)

    # -------------------------
    # 8. Clean published_in_volume
    # -------------------------
    print("Cleaning published_in_volume relationships...")
    published_in_volume = published_in_volume.rename(columns={
        ":START_ID(Paper)": "paperId",
        ":END_ID(Volume)": "volumeId",
    })

    published_in_volume = ensure_columns(published_in_volume, ["paperId", "volumeId", "pages"])
    published_in_volume["paperId"] = published_in_volume["paperId"].map(clean_str)
    published_in_volume["volumeId"] = published_in_volume["volumeId"].map(clean_str)
    published_in_volume["pages"] = published_in_volume["pages"].map(clean_str)

    published_in_volume = published_in_volume[
        (published_in_volume["paperId"] != "")
        & (published_in_volume["volumeId"] != "")
        & (published_in_volume["paperId"].isin(valid_paper_ids))
        & (published_in_volume["volumeId"].isin(valid_volume_ids))
    ]
    published_in_volume = dedup(published_in_volume)

    used_volume_ids = set(published_in_volume["volumeId"].tolist())
    article_paper_ids = set(published_in_volume["paperId"].tolist())

    # -------------------------
    # 9. Clean published_in_edition
    # -------------------------
    print("Cleaning published_in_edition relationships...")
    published_in_edition = published_in_edition.rename(columns={
        ":START_ID(Paper)": "paperId",
        ":END_ID(Edition)": "editionId",
    })

    published_in_edition = ensure_columns(published_in_edition, ["paperId", "editionId", "pages"])
    published_in_edition["paperId"] = published_in_edition["paperId"].map(clean_str)
    published_in_edition["editionId"] = published_in_edition["editionId"].map(clean_str)
    published_in_edition["pages"] = published_in_edition["pages"].map(clean_str)

    published_in_edition = published_in_edition[
        (published_in_edition["paperId"] != "")
        & (published_in_edition["editionId"] != "")
        & (published_in_edition["paperId"].isin(valid_paper_ids))
        & (published_in_edition["editionId"].isin(valid_edition_ids))
    ]
    published_in_edition = dedup(published_in_edition)

    used_edition_ids = set(published_in_edition["editionId"].tolist())
    inproc_paper_ids = set(published_in_edition["paperId"].tolist())

    # -------------------------
    # 10. Validate paperType against venue side
    # -------------------------
    print("Validating paper types...")
    papers = papers[
        (
            (papers["paperType"] == "article")
            & (papers["paperId"].isin(article_paper_ids))
        )
        |
        (
            (papers["paperType"] == "inproceedings")
            & (papers["paperId"].isin(inproc_paper_ids))
        )
    ].copy()
    papers = dedup(papers)

    valid_paper_ids = set(papers["paperId"].tolist())

    wrote = wrote[wrote["paperId"].isin(valid_paper_ids)].copy()
    wrote = dedup(wrote)

    published_in_volume = published_in_volume[
        published_in_volume["paperId"].isin(valid_paper_ids)
    ].copy()
    published_in_volume = dedup(published_in_volume)

    published_in_edition = published_in_edition[
        published_in_edition["paperId"].isin(valid_paper_ids)
    ].copy()
    published_in_edition = dedup(published_in_edition)

    used_authors = set(wrote["authorName"].tolist())
    authors = authors[authors["authorName"].isin(used_authors)].copy()
    authors = dedup(authors)

    # -------------------------
    # 11. Clean belongs_to
    # -------------------------
    print("Cleaning belongs_to relationships...")
    belongs_to = belongs_to.rename(columns={
        ":START_ID(Volume)": "volumeId",
        ":END_ID(Journal)": "journalName",
    })

    belongs_to = ensure_columns(belongs_to, ["volumeId", "journalName"])
    belongs_to["volumeId"] = belongs_to["volumeId"].map(clean_str)
    belongs_to["journalName"] = belongs_to["journalName"].map(clean_str)

    belongs_to = belongs_to[
        (belongs_to["volumeId"] != "")
        & (belongs_to["journalName"] != "")
        & (belongs_to["volumeId"].isin(used_volume_ids))
        & (belongs_to["journalName"].isin(valid_journal_names))
    ]
    belongs_to = dedup(belongs_to)

    used_journal_names = set(belongs_to["journalName"].tolist())
    used_volume_ids = set(belongs_to["volumeId"].tolist())

    volumes = volumes[volumes["volumeId"].isin(used_volume_ids)].copy()
    volumes = dedup(volumes)

    journals = journals[journals["journalName"].isin(used_journal_names)].copy()
    journals = dedup(journals)

    published_in_volume = published_in_volume[
        published_in_volume["volumeId"].isin(used_volume_ids)
    ].copy()
    published_in_volume = dedup(published_in_volume)

    # -------------------------
    # 12. Clean is_edition_of
    # -------------------------
    print("Cleaning is_edition_of relationships...")
    is_edition_of = is_edition_of.rename(columns={
        ":START_ID(Edition)": "editionId",
        ":END_ID(Event)": "eventName",
    })

    is_edition_of = ensure_columns(is_edition_of, ["editionId", "eventName"])
    is_edition_of["editionId"] = is_edition_of["editionId"].map(clean_str)
    is_edition_of["eventName"] = is_edition_of["eventName"].map(clean_str)

    is_edition_of = is_edition_of[
        (is_edition_of["editionId"] != "")
        & (is_edition_of["eventName"] != "")
        & (is_edition_of["editionId"].isin(used_edition_ids))
        & (is_edition_of["eventName"].isin(valid_event_names))
    ]
    is_edition_of = dedup(is_edition_of)

    used_edition_ids = set(is_edition_of["editionId"].tolist())
    used_event_names = set(is_edition_of["eventName"].tolist())

    editions = editions[editions["editionId"].isin(used_edition_ids)].copy()
    editions = dedup(editions)

    events = events[events["eventName"].isin(used_event_names)].copy()
    events = dedup(events)

    published_in_edition = published_in_edition[
        published_in_edition["editionId"].isin(used_edition_ids)
    ].copy()
    published_in_edition = dedup(published_in_edition)

    # -------------------------
    # 13. Final consistency refresh
    # -------------------------
    valid_paper_ids = set(papers["paperId"].tolist())
    valid_author_names = set(authors["authorName"].tolist())
    valid_volume_ids = set(volumes["volumeId"].tolist())
    valid_journal_names = set(journals["journalName"].tolist())
    valid_edition_ids = set(editions["editionId"].tolist())
    valid_event_names = set(events["eventName"].tolist())

    wrote = wrote[
        wrote["paperId"].isin(valid_paper_ids)
        & wrote["authorName"].isin(valid_author_names)
    ].copy()
    wrote = dedup(wrote)

    published_in_volume = published_in_volume[
        published_in_volume["paperId"].isin(valid_paper_ids)
        & published_in_volume["volumeId"].isin(valid_volume_ids)
    ].copy()
    published_in_volume = dedup(published_in_volume)

    belongs_to = belongs_to[
        belongs_to["volumeId"].isin(valid_volume_ids)
        & belongs_to["journalName"].isin(valid_journal_names)
    ].copy()
    belongs_to = dedup(belongs_to)

    published_in_edition = published_in_edition[
        published_in_edition["paperId"].isin(valid_paper_ids)
        & published_in_edition["editionId"].isin(valid_edition_ids)
    ].copy()
    published_in_edition = dedup(published_in_edition)

    is_edition_of = is_edition_of[
        is_edition_of["editionId"].isin(valid_edition_ids)
        & is_edition_of["eventName"].isin(valid_event_names)
    ].copy()
    is_edition_of = dedup(is_edition_of)

    # -------------------------
    # 14. Restore Neo4j import-style headers
    # -------------------------
    papers_out = papers.rename(columns={
        "paperId": "paperId:ID(Paper)",
        "year": "year:int",
    })

    authors_out = authors.rename(columns={
        "authorName": "authorName:ID(Author)",
    })

    journals_out = journals.rename(columns={
        "journalName": "journalName:ID(Journal)",
    })

    volumes_out = volumes.rename(columns={
        "volumeId": "volumeId:ID(Volume)",
        "year": "year:int",
    })

    editions_out = editions.rename(columns={
        "editionId": "editionId:ID(Edition)",
        "year": "year:int",
    })

    events_out = events.rename(columns={
        "eventName": "eventName:ID(Event)",
    })

    wrote_out = wrote.rename(columns={
        "authorName": ":START_ID(Author)",
        "paperId": ":END_ID(Paper)",
        "authorOrder": "authorOrder:int",
    })

    published_in_volume_out = published_in_volume.rename(columns={
        "paperId": ":START_ID(Paper)",
        "volumeId": ":END_ID(Volume)",
    })

    belongs_to_out = belongs_to.rename(columns={
        "volumeId": ":START_ID(Volume)",
        "journalName": ":END_ID(Journal)",
    })

    published_in_edition_out = published_in_edition.rename(columns={
        "paperId": ":START_ID(Paper)",
        "editionId": ":END_ID(Edition)",
    })

    is_edition_of_out = is_edition_of.rename(columns={
        "editionId": ":START_ID(Edition)",
        "eventName": ":END_ID(Event)",
    })

    # -------------------------
    # 15. Write clean files
    # -------------------------
    print("Writing clean LOAD CSV files...")

    write_clean_csv(papers_out, CLEAN_DIR / "papers_clean.csv")
    write_clean_csv(authors_out, CLEAN_DIR / "authors_clean.csv")
    write_clean_csv(journals_out, CLEAN_DIR / "journals_clean.csv")
    write_clean_csv(volumes_out, CLEAN_DIR / "volumes_clean.csv")
    write_clean_csv(editions_out, CLEAN_DIR / "editions_clean.csv")
    write_clean_csv(events_out, CLEAN_DIR / "events_clean.csv")

    write_clean_csv(wrote_out, CLEAN_DIR / "wrote_clean.csv")
    write_clean_csv(published_in_volume_out, CLEAN_DIR / "published_in_volume_clean.csv")
    write_clean_csv(published_in_edition_out, CLEAN_DIR / "published_in_edition_clean.csv")
    write_clean_csv(belongs_to_out, CLEAN_DIR / "belongs_to_clean.csv")
    write_clean_csv(is_edition_of_out, CLEAN_DIR / "is_edition_of_clean.csv")

    print("Done.")
    print("Clean files written to neo4j/load_csv/")
    print("Final sizes:")
    print(f"  papers_clean: {len(papers_out)}")
    print(f"  authors_clean: {len(authors_out)}")
    print(f"  journals_clean: {len(journals_out)}")
    print(f"  volumes_clean: {len(volumes_out)}")
    print(f"  editions_clean: {len(editions_out)}")
    print(f"  events_clean: {len(events_out)}")
    print(f"  wrote_clean: {len(wrote_out)}")
    print(f"  published_in_volume_clean: {len(published_in_volume_out)}")
    print(f"  published_in_edition_clean: {len(published_in_edition_out)}")
    print(f"  belongs_to_clean: {len(belongs_to_out)}")
    print(f"  is_edition_of_clean: {len(is_edition_of_out)}")


if __name__ == "__main__":
    main()