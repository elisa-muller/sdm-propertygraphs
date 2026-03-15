from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase

# ─────────────────────────────────────────────
# CONNECTION — update password if needed
# ─────────────────────────────────────────────
URI      = "neo4j://127.0.0.1:7687"
AUTH     = ("neo4j", "your_password") 
LOAD_DIR = Path(__file__).resolve().parents[1] / "neo4j" / "load_csv"

driver = GraphDatabase.driver(URI, auth=AUTH)


def read(filename):
    return pd.read_csv(LOAD_DIR / filename, dtype=str, keep_default_na=False).to_dict("records")


def batch(session, query, data, size=1000, msg=None):
    if msg:
        print(f"  {msg} ({len(data)} records)...")
    for i in range(0, len(data), size):
        session.run(query, {"rows": data[i:i+size]})


def main():
    with driver.session(database="neo4j") as session:

        # ─────────────────────────────────────
        # 0. CLEAR EXISTING DATA
        # ─────────────────────────────────────
        print("Clearing existing data...")
        session.run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")

        # ─────────────────────────────────────
        # 1. NODES
        # ─────────────────────────────────────
        print("\nLoading nodes...")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Paper {
                paperId:   row['paperId:ID(Paper)'],
                title:     row.title,
                year:      toInteger(row['year:int']),
                pages:     row.pages,
                doi:       row.doi,
                paperType: row.paperType,
                dblpUrl:   row.dblpUrl
            })
        """, read("papers_clean.csv"), msg="Papers")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Author {authorName: row['authorName:ID(Author)']})
        """, read("authors_clean.csv"), msg="Authors")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Journal {journalName: row['journalName:ID(Journal)']})
        """, read("journals_clean.csv"), msg="Journals")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Volume {
                volumeId: row['volumeId:ID(Volume)'],
                volume:   row.volume,
                number:   row.number,
                year:     toInteger(row['year:int'])
            })
        """, read("volumes_clean.csv"), msg="Volumes")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Edition {
                editionId: row['editionId:ID(Edition)'],
                booktitle: row.booktitle,
                title:     row.title,
                year:      toInteger(row['year:int']),
                city:      row.city,
                publisher: row.publisher,
                isbn:      row.isbn,
                ee:        row.ee
            })
        """, read("editions_clean.csv"), msg="Editions")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Event {
                eventName: row['eventName:ID(Event)'],
                type:      row.type
            })
        """, read("events_clean.csv"), msg="Events")

        batch(session, """
            UNWIND $rows AS row
            CREATE (:Keyword {keywordName: row['keywordName:ID(Keyword)']})
        """, read("keywords_clean.csv"), msg="Keywords")

        # ─────────────────────────────────────
        # 2. INDEXES
        # ─────────────────────────────────────
        print("\nCreating indexes...")
        for label, prop in [
            ("Paper",   "paperId"),
            ("Author",  "authorName"),
            ("Journal", "journalName"),
            ("Volume",  "volumeId"),
            ("Edition", "editionId"),
            ("Event",   "eventName"),
            ("Keyword", "keywordName"),
        ]:
            session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{prop})")
        print("  Done.")

        # ─────────────────────────────────────
        # 3. RELATIONSHIPS
        # ─────────────────────────────────────
        print("\nLoading relationships...")

        batch(session, """
            UNWIND $rows AS row
            MATCH (a:Author {authorName: row[':START_ID(Author)']})
            MATCH (p:Paper  {paperId:    row[':END_ID(Paper)']})
            CREATE (a)-[:WROTE {authorOrder: toInteger(row['authorOrder:int'])}]->(p)
        """, read("wrote_clean.csv"), msg="WROTE")

        batch(session, """
            UNWIND $rows AS row
            MATCH (a:Author {authorName: row[':START_ID(Author)']})
            MATCH (p:Paper  {paperId:    row[':END_ID(Paper)']})
            CREATE (a)-[:MAIN_AUTHOR]->(p)
        """, read("main_author_clean.csv"), msg="MAIN_AUTHOR")

        batch(session, """
            UNWIND $rows AS row
            MATCH (a:Author {authorName: row[':START_ID(Author)']})
            MATCH (p:Paper  {paperId:    row[':END_ID(Paper)']})
            CREATE (a)-[:REVIEWED]->(p)
        """, read("reviewed_clean.csv"), msg="REVIEWED")

        batch(session, """
            UNWIND $rows AS row
            MATCH (p1:Paper {paperId: row[':START_ID(Paper)']})
            MATCH (p2:Paper {paperId: row[':END_ID(Paper)']})
            CREATE (p1)-[:CITED]->(p2)
        """, read("cited_clean.csv"), msg="CITED")

        batch(session, """
            UNWIND $rows AS row
            MATCH (p:Paper   {paperId:     row[':START_ID(Paper)']})
            MATCH (k:Keyword {keywordName: row[':END_ID(Keyword)']})
            CREATE (p)-[:HAS_KEYWORD]->(k)
        """, read("has_keyword_clean.csv"), msg="HAS_KEYWORD")

        batch(session, """
            UNWIND $rows AS row
            MATCH (p:Paper  {paperId:  row[':START_ID(Paper)']})
            MATCH (v:Volume {volumeId: row[':END_ID(Volume)']})
            CREATE (p)-[:PUBLISHED_IN {pages: row.pages}]->(v)
        """, read("published_in_volume_clean.csv"), msg="PUBLISHED_IN (volume)")

        batch(session, """
            UNWIND $rows AS row
            MATCH (v:Volume  {volumeId:    row[':START_ID(Volume)']})
            MATCH (j:Journal {journalName: row[':END_ID(Journal)']})
            CREATE (v)-[:BELONGS_TO]->(j)
        """, read("belongs_to_clean.csv"), msg="BELONGS_TO")

        batch(session, """
            UNWIND $rows AS row
            MATCH (p:Paper   {paperId:   row[':START_ID(Paper)']})
            MATCH (e:Edition {editionId: row[':END_ID(Edition)']})
            CREATE (p)-[:PUBLISHED_IN {pages: row.pages}]->(e)
        """, read("published_in_edition_clean.csv"), msg="PUBLISHED_IN (edition)")

        batch(session, """
            UNWIND $rows AS row
            MATCH (e:Edition {editionId: row[':START_ID(Edition)']})
            MATCH (ev:Event  {eventName: row[':END_ID(Event)']})
            CREATE (e)-[:IS_EDITION_OF]->(ev)
        """, read("is_edition_of_clean.csv"), msg="IS_EDITION_OF")

        # ─────────────────────────────────────
        # 4. SUMMARY
        # ─────────────────────────────────────
        print("\nSummary:")
        result = session.run(
            "MATCH (n) RETURN labels(n) AS label, count(n) AS count ORDER BY count DESC"
        )
        for record in result:
            print(f"  {record['label']}: {record['count']}")

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()