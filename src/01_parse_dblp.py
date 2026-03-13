from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "processed"
IMPORT_DIR = PROJECT_ROOT / "neo4j" / "import"

ARTICLE_FILE = RAW_DIR / "output_article"
INPROC_FILE = RAW_DIR / "output_inproceedings"
PROC_FILE = RAW_DIR / "output_proceedings"

IMPORT_DIR.mkdir(parents=True, exist_ok=True)

CHUNKSIZE = 50000

# Target size for the real graph
MAX_ARTICLES = 12000
MAX_INPROCS = 18000

EDITION_COLUMNS = [
    "editionId:ID(Edition)",
    "booktitle",
    "title",
    "year:int",
    "city",
    "publisher",
    "isbn",
    "ee",
]


def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def safe_int(value):
    text = safe_str(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def split_multi_value(cell: str, sep: str = "|") -> list[str]:
    text = safe_str(cell)
    if not text:
        return []
    return [x.strip() for x in text.split(sep) if x.strip()]


def read_dblp_chunks(path: Path):
    return pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        engine="python",
        on_bad_lines="skip",
        chunksize=CHUNKSIZE,
        keep_default_na=False,
    )


def align_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df[columns]


def write_csv(df: pd.DataFrame, path: Path, first_write: bool):
    mode = "w" if first_write else "a"
    header = first_write
    df.to_csv(path, mode=mode, header=header, index=False)


def make_volume_id(journal: str, volume: str, year: str, number: str) -> str:
    journal = safe_str(journal)
    volume = safe_str(volume) or "UNKNOWN"
    year = safe_str(year) or "UNKNOWN"
    number = safe_str(number) or "UNKNOWN"
    return f"{journal}|{volume}|{year}|{number}"


def make_event_id(series: str, title: str, booktitle: str) -> str:
    series = safe_str(series)
    title = safe_str(title)
    booktitle = safe_str(booktitle)

    if series:
        return series
    if booktitle:
        return booktitle
    return title


def process_article_chunk(chunk: pd.DataFrame, remaining: int):
    papers = []
    authors = []
    wrote = []
    journals = []
    volumes = []
    published_in_volume = []
    belongs_to = []

    kept = 0

    for _, row in chunk.iterrows():
        if kept >= remaining:
            break

        paper_id = safe_str(row.get("key"))
        title = safe_str(row.get("title"))
        year = safe_str(row.get("year"))
        pages = safe_str(row.get("pages"))
        doi = safe_str(row.get("ee"))
        journal = safe_str(row.get("journal"))
        volume = safe_str(row.get("volume"))
        number = safe_str(row.get("number"))

        if not paper_id or not title or not journal:
            continue

        volume_id = make_volume_id(journal, volume, year, number)

        papers.append({
            "paperId:ID(Paper)": paper_id,
            "title": title,
            "year:int": safe_int(year),
            "pages": pages,
            "doi": doi,
            "paperType": "article",
            "dblpUrl": safe_str(row.get("url")),
        })

        for author_name in split_multi_value(row.get("author")):
            authors.append({
                "authorName:ID(Author)": author_name,
            })
            wrote.append({
                ":START_ID(Author)": author_name,
                ":END_ID(Paper)": paper_id,
            })

        journals.append({
            "journalName:ID(Journal)": journal,
        })

        volumes.append({
            "volumeId:ID(Volume)": volume_id,
            "volume": volume,
            "number": number,
            "year:int": safe_int(year),
        })

        published_in_volume.append({
            ":START_ID(Paper)": paper_id,
            ":END_ID(Volume)": volume_id,
            "pages": pages,
        })

        belongs_to.append({
            ":START_ID(Volume)": volume_id,
            ":END_ID(Journal)": journal,
        })

        kept += 1

    return (
        pd.DataFrame(papers).drop_duplicates(),
        pd.DataFrame(authors).drop_duplicates(),
        pd.DataFrame(wrote).drop_duplicates(),
        pd.DataFrame(journals).drop_duplicates(),
        pd.DataFrame(volumes).drop_duplicates(),
        pd.DataFrame(published_in_volume).drop_duplicates(),
        pd.DataFrame(belongs_to).drop_duplicates(),
        kept,
    )


def process_inproc_chunk(chunk: pd.DataFrame, remaining: int):
    papers = []
    authors = []
    wrote = []
    editions = []
    published_in_edition = []

    kept = 0

    for _, row in chunk.iterrows():
        if kept >= remaining:
            break

        paper_id = safe_str(row.get("key"))
        title = safe_str(row.get("title"))
        year = safe_str(row.get("year"))
        pages = safe_str(row.get("pages"))
        doi = safe_str(row.get("ee"))
        booktitle = safe_str(row.get("booktitle"))
        crossref = safe_str(row.get("crossref"))

        if not paper_id or not title:
            continue

        edition_id = crossref if crossref else booktitle
        if not edition_id:
            continue

        papers.append({
            "paperId:ID(Paper)": paper_id,
            "title": title,
            "year:int": safe_int(year),
            "pages": pages,
            "doi": doi,
            "paperType": "inproceedings",
            "dblpUrl": safe_str(row.get("url")),
        })

        for author_name in split_multi_value(row.get("author")):
            authors.append({
                "authorName:ID(Author)": author_name,
            })
            wrote.append({
                ":START_ID(Author)": author_name,
                ":END_ID(Paper)": paper_id,
            })

        editions.append({
            "editionId:ID(Edition)": edition_id,
            "booktitle": booktitle,
            "title": "",
            "year:int": safe_int(year),
            "city": "",
            "publisher": "",
            "isbn": "",
            "ee": "",
        })

        published_in_edition.append({
            ":START_ID(Paper)": paper_id,
            ":END_ID(Edition)": edition_id,
            "pages": pages,
        })

        kept += 1

    df_editions = pd.DataFrame(editions).drop_duplicates()
    if not df_editions.empty:
        df_editions = align_columns(df_editions, EDITION_COLUMNS)

    return (
        pd.DataFrame(papers).drop_duplicates(),
        pd.DataFrame(authors).drop_duplicates(),
        pd.DataFrame(wrote).drop_duplicates(),
        df_editions,
        pd.DataFrame(published_in_edition).drop_duplicates(),
        kept,
    )


def process_proceedings_chunk(chunk: pd.DataFrame, selected_edition_ids: set[str]):
    editions = []
    events = []
    is_edition_of = []

    for _, row in chunk.iterrows():
        edition_id = safe_str(row.get("key"))
        if not edition_id or edition_id not in selected_edition_ids:
            continue

        title = safe_str(row.get("title"))
        year = safe_str(row.get("year"))
        address = safe_str(row.get("address"))
        series = safe_str(row.get("series"))
        booktitle = safe_str(row.get("booktitle"))

        event_id = make_event_id(series, title, booktitle)
        if not event_id:
            continue

        editions.append({
            "editionId:ID(Edition)": edition_id,
            "booktitle": booktitle,
            "title": title,
            "year:int": safe_int(year),
            "city": address,
            "publisher": safe_str(row.get("publisher")),
            "isbn": safe_str(row.get("isbn")),
            "ee": safe_str(row.get("ee")),
        })

        events.append({
            "eventName:ID(Event)": event_id,
            "type": "conference_or_workshop",
        })

        is_edition_of.append({
            ":START_ID(Edition)": edition_id,
            ":END_ID(Event)": event_id,
        })

    df_editions = pd.DataFrame(editions).drop_duplicates()
    if not df_editions.empty:
        df_editions = align_columns(df_editions, EDITION_COLUMNS)

    return (
        df_editions,
        pd.DataFrame(events).drop_duplicates(),
        pd.DataFrame(is_edition_of).drop_duplicates(),
    )


def main():
    output_files = [
        IMPORT_DIR / "papers.csv",
        IMPORT_DIR / "authors.csv",
        IMPORT_DIR / "journals.csv",
        IMPORT_DIR / "volumes.csv",
        IMPORT_DIR / "editions.csv",
        IMPORT_DIR / "events.csv",
        IMPORT_DIR / "wrote.csv",
        IMPORT_DIR / "published_in_volume.csv",
        IMPORT_DIR / "published_in_edition.csv",
        IMPORT_DIR / "belongs_to.csv",
        IMPORT_DIR / "is_edition_of.csv",
    ]

    for f in output_files:
        if f.exists():
            f.unlink()

    print("Processing article chunks...")
    first_papers = True
    first_authors = True
    first_journals = True
    first_volumes = True
    first_editions = True
    first_events = True
    first_wrote = True
    first_pubin_volume = True
    first_pubin_edition = True
    first_belongs_to = True
    first_is_edition_of = True

    article_count = 0
    article_chunks = 0

    for chunk in read_dblp_chunks(ARTICLE_FILE):
        if article_count >= MAX_ARTICLES:
            break

        article_chunks += 1
        remaining = MAX_ARTICLES - article_count
        print(f"  article chunk {article_chunks} (remaining target: {remaining})")

        (
            df_papers,
            df_authors,
            df_wrote,
            df_journals,
            df_volumes,
            df_pubin_volume,
            df_belongs_to,
            kept,
        ) = process_article_chunk(chunk, remaining)

        if not df_papers.empty:
            write_csv(df_papers, IMPORT_DIR / "papers.csv", first_papers)
            first_papers = False

        if not df_authors.empty:
            write_csv(df_authors, IMPORT_DIR / "authors.csv", first_authors)
            first_authors = False

        if not df_wrote.empty:
            write_csv(df_wrote, IMPORT_DIR / "wrote.csv", first_wrote)
            first_wrote = False

        if not df_journals.empty:
            write_csv(df_journals, IMPORT_DIR / "journals.csv", first_journals)
            first_journals = False

        if not df_volumes.empty:
            write_csv(df_volumes, IMPORT_DIR / "volumes.csv", first_volumes)
            first_volumes = False

        if not df_pubin_volume.empty:
            write_csv(
                df_pubin_volume,
                IMPORT_DIR / "published_in_volume.csv",
                first_pubin_volume,
            )
            first_pubin_volume = False

        if not df_belongs_to.empty:
            write_csv(df_belongs_to, IMPORT_DIR / "belongs_to.csv", first_belongs_to)
            first_belongs_to = False

        article_count += kept

    print(f"Selected journal articles: {article_count}")

    print("Processing inproceedings chunks...")
    inproc_count = 0
    inproc_chunks = 0
    selected_edition_ids = set()

    for chunk in read_dblp_chunks(INPROC_FILE):
        if inproc_count >= MAX_INPROCS:
            break

        inproc_chunks += 1
        remaining = MAX_INPROCS - inproc_count
        print(f"  inproceedings chunk {inproc_chunks} (remaining target: {remaining})")

        (
            df_papers,
            df_authors,
            df_wrote,
            df_editions,
            df_pubin_edition,
            kept,
        ) = process_inproc_chunk(chunk, remaining)

        if not df_papers.empty:
            write_csv(df_papers, IMPORT_DIR / "papers.csv", first_papers)
            first_papers = False

        if not df_authors.empty:
            write_csv(df_authors, IMPORT_DIR / "authors.csv", first_authors)
            first_authors = False

        if not df_wrote.empty:
            write_csv(df_wrote, IMPORT_DIR / "wrote.csv", first_wrote)
            first_wrote = False

        if not df_editions.empty:
            write_csv(df_editions, IMPORT_DIR / "editions.csv", first_editions)
            first_editions = False
            selected_edition_ids.update(
                df_editions["editionId:ID(Edition)"].dropna().astype(str).tolist()
            )

        if not df_pubin_edition.empty:
            write_csv(
                df_pubin_edition,
                IMPORT_DIR / "published_in_edition.csv",
                first_pubin_edition,
            )
            first_pubin_edition = False

        inproc_count += kept

    print(f"Selected inproceedings: {inproc_count}")
    print(f"Selected edition IDs from inproceedings: {len(selected_edition_ids)}")

    print("Processing proceedings chunks...")
    proc_chunks = 0

    for chunk in read_dblp_chunks(PROC_FILE):
        proc_chunks += 1
        print(f"  proceedings chunk {proc_chunks}")

        df_editions, df_events, df_is_edition_of = process_proceedings_chunk(
            chunk, selected_edition_ids
        )

        if not df_editions.empty:
            write_csv(df_editions, IMPORT_DIR / "editions.csv", first_editions)
            first_editions = False

        if not df_events.empty:
            write_csv(df_events, IMPORT_DIR / "events.csv", first_events)
            first_events = False

        if not df_is_edition_of.empty:
            write_csv(df_is_edition_of, IMPORT_DIR / "is_edition_of.csv", first_is_edition_of)
            first_is_edition_of = False

    print("Done.")
    print("Files written to neo4j/import/")
    print("Created:")
    print("  papers.csv")
    print("  authors.csv")
    print("  journals.csv")
    print("  volumes.csv")
    print("  editions.csv")
    print("  events.csv")
    print("  wrote.csv")
    print("  published_in_volume.csv")
    print("  published_in_edition.csv")
    print("  belongs_to.csv")
    print("  is_edition_of.csv")


if __name__ == "__main__":
    main()