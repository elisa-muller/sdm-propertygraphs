from neo4j import GraphDatabase

# ─────────────────────────────────────────────
# CONNECTION — update password if needed
# ─────────────────────────────────────────────
URI  = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "your_password")

driver = GraphDatabase.driver(URI, auth=AUTH)


def run(session, query, params=None, msg=None):
    if msg:
        print(f"\n{'='*60}")
        print(f" {msg}")
        print('='*60)
    result = session.run(query, params or {})
    records = list(result)
    for r in records[:20]:
        print(dict(r))
    if len(records) > 20:
        print(f"  ... ({len(records)} total rows)")
    return records


def main():
    with driver.session(database="neo4j") as session:

        # ─────────────────────────────────────────────────────────
        # C1 — Define the database research community
        #
        # Creates a Community node named 'Database' and links it
        # to each of the 7 canonical keywords via HAS_TOPIC edges.
        # The Keyword nodes already exist in the graph (loaded in
        # the data preparation phase).
        # ─────────────────────────────────────────────────────────

        run(session, """
            MERGE (c:Community {communityName: 'Database'})
            WITH c
            UNWIND ['data management', 'indexing', 'data modeling',
                    'big data', 'data processing', 'data storage',
                    'data querying'] AS kw
            MATCH (k:Keyword {keywordName: kw})
            MERGE (c)-[:HAS_TOPIC]->(k)
            RETURN c.communityName AS community, collect(k.keywordName) AS keywords
        """, msg="C1 — Create Database community and link keywords")

        # ─────────────────────────────────────────────────────────
        # C2 — Find venues related to the database community
        #
        # A venue (conference, workshop, or journal) is considered
        # database-related if at least 90% of its papers have one
        # or more keywords belonging to the Database community.
        # Asserts a :RELATED_TO edge from each qualifying venue
        # to the Community node, storing the ratio as a property.
        #
        # Both conferences/workshops (via Event) and journals are
        # handled in a single statement using UNION.
        # ─────────────────────────────────────────────────────────

        run(session, """
            // --- Conferences / Workshops ---
            MATCH (c:Community {communityName: 'Database'})-[:HAS_TOPIC]->(k:Keyword)
            WITH c, collect(DISTINCT k.keywordName) AS communityKws

            MATCH (ev:Event)<-[:IS_EDITION_OF]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)
            WITH c, communityKws, ev, count(DISTINCT p) AS totalPapers

            MATCH (ev)<-[:IS_EDITION_OF]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)
                  -[:HAS_KEYWORD]->(k:Keyword)
            WHERE k.keywordName IN communityKws
            WITH c, ev, totalPapers, count(DISTINCT p) AS dbPapers,
                 toFloat(count(DISTINCT p)) / totalPapers AS ratio
            WHERE ratio >= 0.90

            MERGE (ev)-[:RELATED_TO {ratio: round(ratio, 2)}]->(c)
            RETURN ev.eventName AS venue, 'conference/workshop' AS type,
                   totalPapers, dbPapers, round(ratio * 100, 1) AS pct

            UNION

            // --- Journals ---
            MATCH (c:Community {communityName: 'Database'})-[:HAS_TOPIC]->(k:Keyword)
            WITH c, collect(DISTINCT k.keywordName) AS communityKws

            MATCH (j:Journal)<-[:BELONGS_TO]-(v:Volume)<-[:PUBLISHED_IN]-(p:Paper)
            WITH c, communityKws, j, count(DISTINCT p) AS totalPapers

            MATCH (j)<-[:BELONGS_TO]-(v:Volume)<-[:PUBLISHED_IN]-(p:Paper)
                  -[:HAS_KEYWORD]->(k:Keyword)
            WHERE k.keywordName IN communityKws
            WITH c, j, totalPapers, count(DISTINCT p) AS dbPapers,
                 toFloat(count(DISTINCT p)) / totalPapers AS ratio
            WHERE ratio >= 0.90

            MERGE (j)-[:RELATED_TO {ratio: round(ratio, 2)}]->(c)
            RETURN j.journalName AS venue, 'journal' AS type,
                   totalPapers, dbPapers, round(ratio * 100, 1) AS pct
        """, msg="C2 — Database-related venues (>=90% DB papers)")

        # ─────────────────────────────────────────────────────────
        # C3 — Top-100 papers of the database community
        #
        # Finds the 100 most cited papers published in any
        # database-related venue (conference, workshop, or journal)
        # identified in C2. Citation count includes citations from
        # any paper in the graph.
        # Asserts by setting the property isTop100DB = true on
        # each of the top-100 Paper nodes.
        # ─────────────────────────────────────────────────────────

        run(session, """
            MATCH (c:Community {communityName: 'Database'})

            MATCH (p:Paper)-[:PUBLISHED_IN]->(venue)
                  -[:IS_EDITION_OF|BELONGS_TO*1..2]->(hub)
                  -[:RELATED_TO]->(c)

            OPTIONAL MATCH (p)<-[:CITED]-(citing:Paper)

            WITH p, count(citing) AS citations
            ORDER BY citations DESC
            LIMIT 100

            SET p.isTop100DB = true
            RETURN p.title AS title, p.year AS year, citations
            ORDER BY citations DESC
        """, msg="C3 — Top-100 DB papers (assert isTop100DB = true)")

        # ─────────────────────────────────────────────────────────
        # C4 — Identify potential reviewers and gurus
        #
        # Any author of at least one top-100 DB paper is a
        # potential reviewer → assert (Author)-[:POTENTIAL_REVIEWER]
        # ->(Community) with their top-100 paper count.
        #
        # Authors of at least two top-100 DB papers are gurus →
        # additionally assert (Author)-[:GURU_OF]->(Community).
        #
        # Both relationships are asserted in a single statement
        # using FOREACH to conditionally create GURU_OF.
        # ─────────────────────────────────────────────────────────

        run(session, """
            MATCH (c:Community {communityName: 'Database'})
            MATCH (a:Author)-[:WROTE]->(p:Paper {isTop100DB: true})

            WITH c, a, count(DISTINCT p) AS top100Papers

            MERGE (a)-[r:POTENTIAL_REVIEWER]->(c)
            SET r.top100PaperCount = top100Papers

            FOREACH (_ IN CASE WHEN top100Papers >= 2 THEN [1] ELSE [] END |
                MERGE (a)-[g:GURU_OF]->(c)
                SET g.top100PaperCount = top100Papers
            )

            RETURN a.authorName AS author,
                   top100Papers,
                   CASE WHEN top100Papers >= 2 THEN true ELSE false END AS isGuru
            ORDER BY top100Papers DESC
        """, msg="C4 — Potential reviewers and gurus")

        # ─────────────────────────────────────────────────────────
        # Summary
        # ─────────────────────────────────────────────────────────
        run(session, """
            MATCH (c:Community {communityName: 'Database'})
            OPTIONAL MATCH (c)-[:HAS_TOPIC]->(k:Keyword)
            WITH c, collect(k.keywordName) AS keywords
            OPTIONAL MATCH (ev:Event)-[:RELATED_TO]->(c)
            WITH c, keywords, count(DISTINCT ev) AS dbConferences
            OPTIONAL MATCH (j:Journal)-[:RELATED_TO]->(c)
            WITH c, keywords, dbConferences, count(DISTINCT j) AS dbJournals
            OPTIONAL MATCH (p:Paper {isTop100DB: true})
            WITH c, keywords, dbConferences, dbJournals, count(DISTINCT p) AS top100Papers
            OPTIONAL MATCH (a)-[:POTENTIAL_REVIEWER]->(c)
            WITH c, keywords, dbConferences, dbJournals, top100Papers,
                 count(DISTINCT a) AS potentialReviewers
            OPTIONAL MATCH (g)-[:GURU_OF]->(c)
            RETURN keywords, dbConferences, dbJournals, top100Papers,
                   potentialReviewers, count(DISTINCT g) AS gurus
        """, msg="C — Summary")
    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()