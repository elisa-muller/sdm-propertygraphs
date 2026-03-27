from neo4j import GraphDatabase

URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "your_password")  # use your correct password

driver = GraphDatabase.driver(URI, auth=AUTH)


def run(session, query, msg=None):
    if msg:
        print(f"\n--- {msg} ---")
    result = session.run(query)
    for r in result:
        print(r)


def main():
    with driver.session(database="neo4j") as session:

        # --------------------------------------------------
        # 1. CREATE CO-AUTHOR RELATIONSHIPS
        # --------------------------------------------------
        run(session, """
        MATCH (a1:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(a2:Author)
        WHERE id(a1) < id(a2)
        MERGE (a1)-[r:CO_AUTHOR]-(a2)
        ON CREATE SET r.weight = 1
        ON MATCH SET r.weight = r.weight + 1
        """, "Creating co-author relationships")

        # --------------------------------------------------
        # 2. DROP OLD GRAPHS (if exist)
        # --------------------------------------------------
        run(session, "CALL gds.graph.drop('paperGraph', false)", "Drop paper graph")
        run(session, "CALL gds.graph.drop('authorGraph', false)", "Drop author graph")

        # --------------------------------------------------
        # 3. PROJECT PAPER GRAPH
        # --------------------------------------------------
        run(session, """
        CALL gds.graph.project(
            'paperGraph',
            'Paper',
            {
                CITED: {
                    orientation: 'NATURAL'
                }
            }
        )
        """, "Projecting paper graph")

        # --------------------------------------------------
        # 4. RUN PAGERANK
        # --------------------------------------------------
        run(session, """
        CALL gds.pageRank.stream('paperGraph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS paper,
               score
        ORDER BY score DESC
        LIMIT 10
        """, "Top influential papers (PageRank)")

        # --------------------------------------------------
        # 5. PROJECT AUTHOR GRAPH
        # --------------------------------------------------
        run(session, """
        CALL gds.graph.project(
            'authorGraph',
            'Author',
            {
                CO_AUTHOR: {
                    orientation: 'UNDIRECTED',
                    properties: 'weight'
                }
            }
        )
        """, "Projecting author collaboration graph")

        # --------------------------------------------------
        # 6. RUN LOUVAIN
        # --------------------------------------------------
        run(session, """
        CALL gds.louvain.stream('authorGraph')
        YIELD nodeId, communityId
        RETURN communityId,
               count(*) AS size,
               collect(gds.util.asNode(nodeId).authorName)[0..5] AS authors
        ORDER BY size DESC
        """, "Detected author communities")

    driver.close()


if __name__ == "__main__":
    main()