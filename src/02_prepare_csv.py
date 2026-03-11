from pathlib import Path
import pandas as pd
import csv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
IMPORT_DIR = PROJECT_ROOT / "neo4j" / "import"
CLEAN_DIR = PROJECT_ROOT / "neo4j" / "load_csv"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def read_csv_safe(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def clean_str(x: str) -> str:
    if x is None:
        return ""
    return str(x).strip()


def normalize_year(x: str):
    x = clean_str(x)
    if not x:
        return ""
    try:
        return str(int(float(x)))
    except ValueError:
        return ""


def is_valid_paper_id(paper_id: str) -> bool:
    paper_id = clean_str(paper_id)
    if not paper_id:
        return False
    if paper_id.startswith("dblpnote/"):
        return False
    return True


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates().reset_index(drop=True)


def main():
    print("Reading intermediate CSV files...")

    authors = read_csv_safe(IMPORT_DIR / "authors.csv")
    papers = read_csv_safe(IMPORT_DIR / "papers.csv")
    wrote = read_csv_safe(IMPORT_DIR / "wrote.csv")
    events = read_csv_safe(IMPORT_DIR / "events.csv")
    journals = read_csv_safe(IMPORT_DIR / "journals.csv")
    published_in = read_csv_safe(IMPORT_DIR / "published_in.csv")

    print("Original sizes:")
    print(f"  authors: {len(authors)}")
    print(f"  papers: {len(papers)}")
    print(f"  wrote: {len(wrote)}")
    print(f"  events: {len(events)}")
    print(f"  journals: {len(journals)}")
    print(f"  published_in: {len(published_in)}")

    # -------------------------
    # 1. Clean papers
    # -------------------------
    print("Cleaning papers...")
    papers = papers.rename(
        columns={
            "paperId:ID(Paper)": "paperId",
            "year:int": "year",
        }
    )

    required_paper_cols = ["paperId", "title", "year", "pages", "doi", "paperType"]
    for col in required_paper_cols:
        if col not in papers.columns:
            papers[col] = ""

    papers = papers[required_paper_cols].copy()
    papers["paperId"] = papers["paperId"].map(clean_str)
    papers["title"] = papers["title"].map(clean_str)
    papers["year"] = papers["year"].map(normalize_year)
    papers["pages"] = papers["pages"].map(clean_str)
    papers["doi"] = papers["doi"].map(clean_str)
    papers["paperType"] = papers["paperType"].map(clean_str)

    papers = papers[papers["paperId"].map(is_valid_paper_id)]
    papers = papers[papers["title"] != ""]
    papers = dedup(papers)

    valid_paper_ids = set(papers["paperId"].tolist())

    # -------------------------
    # 2. Clean authors
    # -------------------------
    print("Cleaning authors...")
    authors = authors.rename(columns={"authorName:ID(Author)": "authorName"})
    if "authorName" not in authors.columns:
        authors["authorName"] = ""

    authors = authors[["authorName"]].copy()
    authors["authorName"] = authors["authorName"].map(clean_str)
    authors = authors[authors["authorName"] != ""]
    authors = dedup(authors)

    valid_author_names = set(authors["authorName"].tolist())

    # -------------------------
    # 3. Clean wrote
    # -------------------------
    print("Cleaning wrote relationships...")
    wrote = wrote.rename(
        columns={
            ":START_ID(Author)": "authorName",
            ":END_ID(Paper)": "paperId",
        }
    )

    for col in ["authorName", "paperId"]:
        if col not in wrote.columns:
            wrote[col] = ""

    wrote = wrote[["authorName", "paperId"]].copy()
    wrote["authorName"] = wrote["authorName"].map(clean_str)
    wrote["paperId"] = wrote["paperId"].map(clean_str)

    wrote = wrote[
        (wrote["authorName"] != "")
        & (wrote["paperId"] != "")
        & (wrote["authorName"].isin(valid_author_names))
        & (wrote["paperId"].isin(valid_paper_ids))
    ]
    wrote = dedup(wrote)

    # Optional: reduce authors to only those appearing in wrote
    used_authors = set(wrote["authorName"].tolist())
    authors = authors[authors["authorName"].isin(used_authors)].copy()
    authors = dedup(authors)

    # -------------------------
    # 4. Clean events
    # -------------------------
    print("Cleaning events...")
    events = events.rename(columns={"eventName:ID(Event)": "eventName"})
    if "eventName" not in events.columns:
        events["eventName"] = ""

    events = events[["eventName"]].copy()
    events["eventName"] = events["eventName"].map(clean_str)
    events = events[events["eventName"] != ""]
    events = dedup(events)

    valid_event_names = set(events["eventName"].tolist())

    # -------------------------
    # 5. Clean journals
    # -------------------------
    print("Cleaning journals...")
    journals = journals.rename(columns={"journalName:ID(Journal)": "journalName"})
    if "journalName" not in journals.columns:
        journals["journalName"] = ""

    journals = journals[["journalName"]].copy()
    journals["journalName"] = journals["journalName"].map(clean_str)
    journals = journals[journals["journalName"] != ""]
    journals = dedup(journals)

    valid_journal_names = set(journals["journalName"].tolist())

    # -------------------------
    # 6. Split published_in
    # -------------------------
    print("Cleaning and splitting published_in relationships...")
    # Current intermediate file has article-style headers
    published_in = published_in.rename(
        columns={
            ":START_ID(Paper)": "paperId",
            ":END_ID(Journal)": "venueName",
            "venueType": "venueType",
        }
    )

    for col in ["paperId", "venueName", "venueType"]:
        if col not in published_in.columns:
            published_in[col] = ""

    published_in = published_in[["paperId", "venueName", "venueType"]].copy()
    published_in["paperId"] = published_in["paperId"].map(clean_str)
    published_in["venueName"] = published_in["venueName"].map(clean_str)
    published_in["venueType"] = published_in["venueType"].map(clean_str)

    published_in = published_in[
        (published_in["paperId"] != "")
        & (published_in["venueName"] != "")
        & (published_in["paperId"].isin(valid_paper_ids))
    ]
    published_in = dedup(published_in)

    published_in_journal = published_in[published_in["venueType"] == "journal"].copy()
    published_in_event = published_in[published_in["venueType"] == "conference_or_workshop"].copy()

    published_in_journal = published_in_journal.rename(columns={"venueName": "journalName"})
    published_in_event = published_in_event.rename(columns={"venueName": "eventName"})

    published_in_journal = published_in_journal[["paperId", "journalName"]]
    published_in_event = published_in_event[["paperId", "eventName"]]

    published_in_journal = published_in_journal[
        published_in_journal["journalName"].isin(valid_journal_names)
    ]
    published_in_event = published_in_event[
        published_in_event["eventName"].isin(valid_event_names)
    ]

    published_in_journal = dedup(published_in_journal)
    published_in_event = dedup(published_in_event)

    # Optional: reduce venue tables to only used venues
    used_journals = set(published_in_journal["journalName"].tolist())
    used_events = set(published_in_event["eventName"].tolist())

    journals = journals[journals["journalName"].isin(used_journals)].copy()
    events = events[events["eventName"].isin(used_events)].copy()

    journals = dedup(journals)
    events = dedup(events)

    # -------------------------
    # 7. Write clean files
    # -------------------------
    authors.to_csv(
        CLEAN_DIR / "authors_clean.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    papers.to_csv(
        CLEAN_DIR / "papers_clean.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    wrote.to_csv(
        CLEAN_DIR / "wrote_clean.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    events.to_csv(
        CLEAN_DIR / "events_clean.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    journals.to_csv(
        CLEAN_DIR / "journals_clean.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    published_in_journal.to_csv(
        CLEAN_DIR / "published_in_journal.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    published_in_event.to_csv(
        CLEAN_DIR / "published_in_event.csv",
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\"
    )

    print("Done.")
    print("Clean files written to neo4j/load_csv/")
    print("Final sizes:")
    print(f"  authors_clean: {len(authors)}")
    print(f"  papers_clean: {len(papers)}")
    print(f"  wrote_clean: {len(wrote)}")
    print(f"  events_clean: {len(events)}")
    print(f"  journals_clean: {len(journals)}")
    print(f"  published_in_journal: {len(published_in_journal)}")
    print(f"  published_in_event: {len(published_in_event)}")


if __name__ == "__main__":
    main()