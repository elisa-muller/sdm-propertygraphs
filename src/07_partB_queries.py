from neo4j import GraphDatabase

# ─────────────────────────────────────────────
# CONNECTION — update password if needed
# ─────────────────────────────────────────────
URI  = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "your_password")

driver = GraphDatabase.driver(URI, auth=AUTH)


def run(session, query, msg):
    print(f"\n{'='*60}")
    print(f" {msg}")
    print('='*60)
    result = session.run(query)
    for record in result:
        print(dict(record))


def main():
    with driver.session(database="neo4j") as session:

        # ─────────────────────────────────────
        # B1 — Top 3 most cited papers per conference/workshop
        # ─────────────────────────────────────
        run(session, """
            MATCH (p:Paper)-[:PUBLISHED_IN]->(e:Edition)-[:IS_EDITION_OF]->(ev:Event)
            MATCH (p)<-[:CITED]-(citing:Paper)
            WITH ev.eventName AS event, p.title AS title, count(*) AS citations
            ORDER BY event, citations DESC
            WITH event, collect({title: title, citations: citations}) AS papers
            RETURN event, [p IN papers | p][..3] AS top3
        """, "B1 — Top 3 most cited papers per conference/workshop")

        # ─────────────────────────────────────
        # B2 — Community of each conference/workshop
        # (authors with papers in at least 4 different editions)
        # ─────────────────────────────────────
        run(session, """
            MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->(e:Edition)-[:IS_EDITION_OF]->(ev:Event)
            WITH ev.eventName AS event, a.authorName AS author, count(DISTINCT e) AS editions
            WHERE editions >= 4
            RETURN event, author, editions
            ORDER BY event, editions DESC
        """, "B2 — Community per conference/workshop (4+ editions)")

        # ─────────────────────────────────────
        # B3 — Impact factor of journals
        # Impact factor = citations received in year Y /
        #                 papers published in years Y-1 and Y-2
        # Using 2023 as reference year
        # ─────────────────────────────────────
        run(session, """
            MATCH (p:Paper)-[:PUBLISHED_IN]->(v:Volume)-[:BELONGS_TO]->(j:Journal)
            WHERE p.year >= 2021 AND p.year <= 2022
            MATCH (p)<-[:CITED]-(citing:Paper)
            WHERE citing.year = 2023
            WITH j.journalName AS journal,
                 count(DISTINCT p) AS papers,
                 count(citing) AS citations
            RETURN journal,
                   papers,
                   citations,
                   round(toFloat(citations) / papers, 2) AS impactFactor
            ORDER BY impactFactor DESC
        """, "B3 — Impact factor of journals (reference year: 2023)")

        # ─────────────────────────────────────
        # B4 — H-index of authors
        # H-index = largest h such that the author has
        # at least h papers with at least h citations each
        # ─────────────────────────────────────
        run(session, """
            MATCH (a:Author)-[:WROTE]->(p:Paper)
            OPTIONAL MATCH (p)<-[:CITED]-(citing:Paper)
            WITH a.authorName AS author, p, count(citing) AS citations
            ORDER BY author, citations DESC
            WITH author, collect(citations) AS citationList
            WITH author, [i IN range(0, size(citationList)-1)
                          WHERE citationList[i] >= i+1 | i+1] AS hValues
            RETURN author,
                   CASE WHEN size(hValues) > 0 THEN last(hValues) ELSE 0 END AS hIndex
            ORDER BY hIndex DESC
            LIMIT 20
        """, "B4 — H-index of authors (top 20)")

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()