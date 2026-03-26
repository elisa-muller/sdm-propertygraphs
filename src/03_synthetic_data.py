from pathlib import Path
import pandas as pd
import random
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOAD_DIR = PROJECT_ROOT / "neo4j" / "load_csv"

random.seed(42)


def read_csv(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_csv(df, path):
    df.to_csv(path, index=False, quoting=csv.QUOTE_ALL, escapechar="\\")


def safe_int(x):
    try:
        return int(float(str(x).strip()))
    except Exception:
        return None


def normalize_text(x):
    return str(x).strip().lower() if x is not None else ""


def main():
    print("Loading clean data...")

    papers = read_csv(LOAD_DIR / "papers_clean.csv")
    authors = read_csv(LOAD_DIR / "authors_clean.csv")
    wrote = read_csv(LOAD_DIR / "wrote_clean.csv")

    # Needed for venue-aware keyword generation
    published_in_edition = read_csv(LOAD_DIR / "published_in_edition_clean.csv")
    published_in_volume = read_csv(LOAD_DIR / "published_in_volume_clean.csv")
    editions = read_csv(LOAD_DIR / "editions_clean.csv")
    events = read_csv(LOAD_DIR / "events_clean.csv")
    volumes = read_csv(LOAD_DIR / "volumes_clean.csv")
    journals = read_csv(LOAD_DIR / "journals_clean.csv")
    belongs_to = read_csv(LOAD_DIR / "belongs_to_clean.csv")
    is_edition_of = read_csv(LOAD_DIR / "is_edition_of_clean.csv")

    paper_ids = papers["paperId:ID(Paper)"].tolist()
    author_ids = authors["authorName:ID(Author)"].tolist()

    # ------------------------------------------------
    # 1. MAIN_AUTHOR
    # ------------------------------------------------
    print("Generating MAIN_AUTHOR...")

    wrote["authorOrderNum"] = wrote["authorOrder:int"].map(safe_int)
    wrote_sorted = wrote.sort_values(
        by=[":END_ID(Paper)", "authorOrderNum", ":START_ID(Author)"],
        na_position="last"
    )

    main_author = (
        wrote_sorted
        .groupby(":END_ID(Paper)", as_index=False)
        .first()[[":START_ID(Author)", ":END_ID(Paper)"]]
    ).drop_duplicates()

    print("Main authors:", len(main_author))

    # ------------------------------------------------
    # 2. REVIEWED
    # ------------------------------------------------
    print("Generating REVIEWED relationships...")

    paper_to_authors = (
        wrote.groupby(":END_ID(Paper)")[":START_ID(Author)"]
        .apply(set)
        .to_dict()
    )

    reviewed_rows = []
    used_review_pairs = set()

    for paper in paper_ids:
        paper_authors = paper_to_authors.get(paper, set())
        candidate_reviewers = [a for a in author_ids if a not in paper_authors]

        if len(candidate_reviewers) < 3:
            continue

        reviewers = random.sample(candidate_reviewers, k=3)

        for reviewer in reviewers:
            pair = (reviewer, paper)
            if pair not in used_review_pairs:
                reviewed_rows.append(pair)
                used_review_pairs.add(pair)

    reviewed = pd.DataFrame(
        reviewed_rows,
        columns=[":START_ID(Author)", ":END_ID(Paper)"]
    ).drop_duplicates()

    print("Reviewed edges:", len(reviewed))

    # ------------------------------------------------
    # 3. KEYWORDS
    # ------------------------------------------------
    print("Generating KEYWORDS...")

    keyword_list = [
        "machine learning",
        "deep learning",
        "data processing",
        "graph databases",
        "knowledge graphs",
        "semantic web",
        "information retrieval",
        "data management",
        "data storage",
        "cloud computing",
        "bioinformatics",
        "pattern recognition",
        "big data",
        "neural networks",
        "indexing",
        "data modeling",
        "data querying",
        "computer vision",
        "natural language processing",
        "artificial intelligence",
    ]

    DB_KEYWORDS = [
        "data management",
        "indexing",
        "data modeling",
        "big data",
        "data processing",
        "data storage",
        "data querying",
    ]

    NON_DB_KEYWORDS = [kw for kw in keyword_list if kw not in DB_KEYWORDS]

    keywords = pd.DataFrame(keyword_list, columns=["keywordName:ID(Keyword)"])
    print("Keyword nodes:", len(keywords))

    # ------------------------------------------------
    # 4. HAS_KEYWORD
    # ------------------------------------------------
    print("Generating HAS_KEYWORD...")

    title_map = papers.set_index("paperId:ID(Paper)")["title"].to_dict()

    keyword_rules = {
        "machine learning": ["learning", "classifier", "classification"],
        "deep learning": ["deep", "cnn", "rnn", "transformer"],
        "data processing": ["mining", "pattern discovery"],
        "graph databases": ["graph database", "neo4j"],
        "knowledge graphs": ["knowledge graph", "knowledge base"],
        "semantic web": ["semantic web", "rdf", "owl", "sparql"],
        "information retrieval": ["retrieval", "search", "ranking"],
        "data management": ["database", "dbms", "transaction"],
        "data storage": ["distributed", "parallel", "cluster"],
        "cloud computing": ["cloud", "virtualization"],
        "bioinformatics": ["genome", "protein", "biomedical", "bioinformatics"],
        "pattern recognition": ["pattern recognition", "recognition"],
        "big data": ["big data", "large-scale"],
        "neural networks": ["neural network", "neural"],
        "indexing": ["index", "indexing", "b-tree", "hash"],
        "data modeling": ["integration", "schema matching"],
        "data querying": ["query", "join", "execution plan"],
        "computer vision": ["image", "vision", "object detection"],
        "natural language processing": ["text", "language", "nlp", "token"],
        "artificial intelligence": ["artificial intelligence", "ai", "intelligent"],
    }

    # ----------------------------
    # Venue mapping
    # ----------------------------

    edition_to_event = dict(zip(
        is_edition_of[":START_ID(Edition)"],
        is_edition_of[":END_ID(Event)"]
    ))

    volume_to_journal = dict(zip(
        belongs_to[":START_ID(Volume)"],
        belongs_to[":END_ID(Journal)"]
    ))

    paper_to_venue = {}

    for _, row in published_in_edition.iterrows():
        paper_id = row[":START_ID(Paper)"]
        edition_id = row[":END_ID(Edition)"]
        venue = edition_to_event.get(edition_id, "")
        if venue:
            paper_to_venue[paper_id] = venue

    for _, row in published_in_volume.iterrows():
        paper_id = row[":START_ID(Paper)"]
        volume_id = row[":END_ID(Volume)"]
        venue = volume_to_journal.get(volume_id, "")
        if venue:
            paper_to_venue[paper_id] = venue

    DB_VENUE_HINTS = [
        "sigmod",
        "vldb",
        "pvldb",
        "icde",
        "edbt",
        "pods",
        "cidr",
        "ssdbm",
        "dasfaa",
        "webdb",
        "database",
        "data",
        "information systems",
        "world wide web",
    ]

    def is_db_venue(venue_name: str) -> bool:
        v = normalize_text(venue_name)
        return any(hint in v for hint in DB_VENUE_HINTS)

    has_keyword_rows = []

    db_venue_papers = 0
    non_db_venue_papers = 0

    for paper in paper_ids:
        title = normalize_text(title_map.get(paper, ""))
        venue = paper_to_venue.get(paper, "")
        assigned = []

        # Rule-based keyword detection from title
        for kw, patterns in keyword_rules.items():
            if any(p in title for p in patterns):
                assigned.append(kw)

        # Remove duplicates while preserving order
        assigned = list(dict.fromkeys(assigned))

        if is_db_venue(venue):
            db_venue_papers += 1

            # Ensure at least one DB keyword in DB-like venues
            if not any(kw in DB_KEYWORDS for kw in assigned):
                assigned.append(random.choice(DB_KEYWORDS))

            # Fill to 1..3 keywords, mostly DB
            target_k = random.randint(1, 3)
            while len(assigned) < target_k:
                kw = random.choice(DB_KEYWORDS if random.random() < 0.9 else NON_DB_KEYWORDS)
                if kw not in assigned:
                    assigned.append(kw)

        else:
            non_db_venue_papers += 1

            if not assigned:
                # Mostly non-DB for non-DB venues, but keep a little noise
                pool = NON_DB_KEYWORDS if random.random() < 0.8 else DB_KEYWORDS
                assigned = random.sample(pool, k=random.randint(1, 2))
            else:
                # Keep title-based matches, occasionally add one extra non-DB keyword
                if len(assigned) < 3 and random.random() < 0.4:
                    extra = random.choice(NON_DB_KEYWORDS)
                    if extra not in assigned:
                        assigned.append(extra)

            assigned = list(dict.fromkeys(assigned))[:3]

        for kw in assigned:
            has_keyword_rows.append((paper, kw))

    has_keyword = pd.DataFrame(
        has_keyword_rows,
        columns=[":START_ID(Paper)", ":END_ID(Keyword)"]
    ).drop_duplicates()

    print("HAS_KEYWORD edges:", len(has_keyword))
    print("Papers in DB-like venues:", db_venue_papers)
    print("Papers in non-DB venues:", non_db_venue_papers)

    # ------------------------------------------------
    # 5. CITED
    # ------------------------------------------------
    print("Generating CITED relationships...")

    paper_year_map = {
        row["paperId:ID(Paper)"]: safe_int(row["year:int"])
        for _, row in papers.iterrows()
    }

    cited_rows = []
    used_citations = set()

    for paper in paper_ids:
        citing_year = paper_year_map.get(paper)

        valid_targets = []
        for target in paper_ids:
            if target == paper:
                continue
            target_year = paper_year_map.get(target)
            if citing_year is None or target_year is None or target_year <= citing_year:
                valid_targets.append(target)

        if not valid_targets:
            continue

        k = min(random.randint(1, 4), len(valid_targets))
        targets = random.sample(valid_targets, k=k)

        for target in targets:
            edge = (paper, target)
            if edge not in used_citations:
                cited_rows.append(edge)
                used_citations.add(edge)

    cited = pd.DataFrame(
        cited_rows,
        columns=[":START_ID(Paper)", ":END_ID(Paper)"]
    ).drop_duplicates()

    print("CITED edges:", len(cited))

    # ------------------------------------------------
    # WRITE FILES
    # ------------------------------------------------
    print("Writing synthetic CSV files...")

    write_csv(main_author, LOAD_DIR / "main_author_clean.csv")
    write_csv(reviewed, LOAD_DIR / "reviewed_clean.csv")
    write_csv(keywords, LOAD_DIR / "keywords_clean.csv")
    write_csv(has_keyword, LOAD_DIR / "has_keyword_clean.csv")
    write_csv(cited, LOAD_DIR / "cited_clean.csv")

    print("Done.")
    print("Synthetic files created:")
    print("  main_author_clean.csv")
    print("  reviewed_clean.csv")
    print("  keywords_clean.csv")
    print("  has_keyword_clean.csv")
    print("  cited_clean.csv")


if __name__ == "__main__":
    main()