from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
IMPORT_DIR = PROJECT_ROOT / "neo4j" / "import"

ARTICLE_FILE = RAW_DIR / "output_article"
INPROC_FILE = RAW_DIR / "output_inproceedings"

IMPORT_DIR.mkdir(parents=True, exist_ok=True)

CHUNKSIZE = 50000


def safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


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
    )


def write_csv(df: pd.DataFrame, path: Path, first_write: bool):
    mode = "w" if first_write else "a"
    header = first_write
    df.to_csv(path, mode=mode, header=header, index=False)


def process_article_chunk(chunk: pd.DataFrame):
    papers = []
    authors = []
    wrote = []
    journals = []
    published_in = []

    for _, row in chunk.iterrows():
        paper_id = safe_str(row.get("key"))
        title = safe_str(row.get("title"))
        year = safe_str(row.get("year"))
        pages = safe_str(row.get("pages"))
        doi = safe_str(row.get("ee"))
        journal = safe_str(row.get("journal"))

        if not paper_id or not title:
            continue

        papers.append({
            "paperId:ID(Paper)": paper_id,
            "title": title,
            "year:int": int(year) if year.isdigit() else None,
            "pages": pages,
            "doi": doi,
            "paperType": "article",
        })

        for author_name in split_multi_value(row.get("author")):
            authors.append({"authorName:ID(Author)": author_name})
            wrote.append({
                ":START_ID(Author)": author_name,
                ":END_ID(Paper)": paper_id,
            })

        if journal:
            journals.append({"journalName:ID(Journal)": journal})
            published_in.append({
                ":START_ID(Paper)": paper_id,
                ":END_ID(Journal)": journal,
                "venueType": "journal",
            })

    return (
        pd.DataFrame(papers).drop_duplicates(),
        pd.DataFrame(authors).drop_duplicates(),
        pd.DataFrame(wrote).drop_duplicates(),
        pd.DataFrame(journals).drop_duplicates(),
        pd.DataFrame(published_in).drop_duplicates(),
    )


def process_inproc_chunk(chunk: pd.DataFrame):
    papers = []
    authors = []
    wrote = []
    events = []
    published_in = []

    for _, row in chunk.iterrows():
        paper_id = safe_str(row.get("key"))
        title = safe_str(row.get("title"))
        year = safe_str(row.get("year"))
        pages = safe_str(row.get("pages"))
        doi = safe_str(row.get("ee"))
        event = safe_str(row.get("booktitle"))

        if not paper_id or not title:
            continue

        papers.append({
            "paperId:ID(Paper)": paper_id,
            "title": title,
            "year:int": int(year) if year.isdigit() else None,
            "pages": pages,
            "doi": doi,
            "paperType": "inproceedings",
        })

        for author_name in split_multi_value(row.get("author")):
            authors.append({"authorName:ID(Author)": author_name})
            wrote.append({
                ":START_ID(Author)": author_name,
                ":END_ID(Paper)": paper_id,
            })

        if event:
            events.append({"eventName:ID(Event)": event})
            published_in.append({
                ":START_ID(Paper)": paper_id,
                ":END_ID(Event)": event,
                "venueType": "conference_or_workshop",
            })

    return (
        pd.DataFrame(papers).drop_duplicates(),
        pd.DataFrame(authors).drop_duplicates(),
        pd.DataFrame(wrote).drop_duplicates(),
        pd.DataFrame(events).drop_duplicates(),
        pd.DataFrame(published_in).drop_duplicates(),
    )


def main():
    output_files = [
        IMPORT_DIR / "papers.csv",
        IMPORT_DIR / "authors.csv",
        IMPORT_DIR / "wrote.csv",
        IMPORT_DIR / "journals.csv",
        IMPORT_DIR / "events.csv",
        IMPORT_DIR / "published_in.csv",
    ]

    for f in output_files:
        if f.exists():
            f.unlink()

    print("Processing article chunks...")
    first_papers = first_authors = first_wrote = True
    first_journals = True
    first_events = True
    first_published_in = True

    article_chunks = 0
    for chunk in read_dblp_chunks(ARTICLE_FILE):
        article_chunks += 1
        print(f"  article chunk {article_chunks}")

        df_papers, df_authors, df_wrote, df_journals, df_pubin = process_article_chunk(chunk)

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

        if not df_pubin.empty:
            write_csv(df_pubin, IMPORT_DIR / "published_in.csv", first_published_in)
            first_published_in = False

    print("Processing inproceedings chunks...")
    inproc_chunks = 0
    for chunk in read_dblp_chunks(INPROC_FILE):
        inproc_chunks += 1
        print(f"  inproceedings chunk {inproc_chunks}")

        df_papers, df_authors, df_wrote, df_events, df_pubin = process_inproc_chunk(chunk)

        if not df_papers.empty:
            write_csv(df_papers, IMPORT_DIR / "papers.csv", first_papers)
            first_papers = False

        if not df_authors.empty:
            write_csv(df_authors, IMPORT_DIR / "authors.csv", first_authors)
            first_authors = False

        if not df_wrote.empty:
            write_csv(df_wrote, IMPORT_DIR / "wrote.csv", first_wrote)
            first_wrote = False

        if not df_events.empty:
            write_csv(df_events, IMPORT_DIR / "events.csv", first_events)
            first_events = False

        if not df_pubin.empty:
            write_csv(df_pubin, IMPORT_DIR / "published_in.csv", first_published_in)
            first_published_in = False

    print("Done.")
    print("Files written to neo4j/import/")


if __name__ == "__main__":
    main()