from neo4j import GraphDatabase
import random

# ─────────────────────────────────────────────
# CONNECTION — update password if needed
# ─────────────────────────────────────────────
URI  = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "your_password")

driver = GraphDatabase.driver(URI, auth=AUTH)

random.seed(42)

ORGANIZATIONS = [
    {"organizationName": "MIT",                  "type": "university"},
    {"organizationName": "Stanford University",  "type": "university"},
    {"organizationName": "CMU",                  "type": "university"},
    {"organizationName": "ETH Zurich",           "type": "university"},
    {"organizationName": "Oxford University",    "type": "university"},
    {"organizationName": "TU Berlin",            "type": "university"},
    {"organizationName": "UPC Barcelona",        "type": "university"},
    {"organizationName": "Google Research",      "type": "company"},
    {"organizationName": "Microsoft Research",   "type": "company"},
    {"organizationName": "IBM Research",         "type": "company"},
    {"organizationName": "Meta AI",              "type": "company"},
    {"organizationName": "Amazon Research",      "type": "company"},
]

REVIEW_CONTENTS = [
    "The paper presents a solid contribution with well-supported experimental results.",
    "Interesting approach, but the evaluation section needs more detail.",
    "The methodology is sound and the writing is clear.",
    "Some related work is missing, but the core idea is novel.",
    "Strong theoretical foundation with practical implications.",
    "The paper is well-structured and addresses an important problem.",
    "Results are promising but the scalability analysis is incomplete.",
    "Good paper overall, minor revisions recommended.",
]


def run(session, query, params=None, msg=None):
    if msg:
        print(f"  {msg}...")
    session.run(query, params or {})


def main():
    with driver.session(database="neo4j") as session:

        # ─────────────────────────────────────
        # 1. CREATE ORGANIZATION NODES
        # ─────────────────────────────────────
        print("Creating Organization nodes...")
        session.run("""
            UNWIND $orgs AS org
            CREATE (:Organization {
                organizationName: org.organizationName,
                type:             org.type
            })
        """, {"orgs": ORGANIZATIONS})
        print(f"  {len(ORGANIZATIONS)} organizations created.")

        # ─────────────────────────────────────
        # 2. CREATE AFFILIATED_TO RELATIONSHIPS
        # Assign each author to a random organization
        # ─────────────────────────────────────
        print("Creating AFFILIATED_TO relationships...")
        session.run("""
            MATCH (a:Author)
            MATCH (o:Organization)
            WITH a, collect(o) AS orgs
            WITH a, orgs[toInteger(rand() * size(orgs))] AS org
            CREATE (a)-[:AFFILIATED_TO]->(org)
        """)
        print("  Done.")

        # ─────────────────────────────────────
        # 3. CREATE REVIEW NODES
        # Replace (Author)-[:REVIEWED]->(Paper)
        # with (Author)-[:WROTE_REVIEW]->(Review)-[:EVALUATES]->(Paper)
        #
        # Decision logic:
        #   - 70% of papers: all 3 reviewers accept  → decision = 'accept'
        #   - 30% of papers: 2 accept, 1 reject
        #     The last reviewer (by order) gets 'reject'
        # ─────────────────────────────────────
        print("Creating Review nodes and replacing REVIEWED relationships...")

        # Step 3a — get all papers and their reviewers
        result = session.run("""
            MATCH (a:Author)-[:REVIEWED]->(p:Paper)
            WITH p, collect(a) AS reviewers
            RETURN p.paperId AS paperId, reviewers
        """)

        review_data = []
        for record in result:
            paper_id  = record["paperId"]
            reviewers = record["reviewers"]
            num       = len(reviewers)
            unanimous = random.random() < 0.7  # 70% all accept

            for idx, reviewer in enumerate(reviewers):
                if unanimous:
                    decision = "accept"
                else:
                    # last reviewer rejects
                    decision = "reject" if idx == num - 1 else "accept"

                review_data.append({
                    "authorName": reviewer["authorName"],
                    "paperId":    paper_id,
                    "content":    random.choice(REVIEW_CONTENTS),
                    "decision":   decision,
                })

        print(f"  {len(review_data)} reviews to create...")

        # Step 3b — create Review nodes and relationships in batches
        batch_size = 1000
        for i in range(0, len(review_data), batch_size):
            batch = review_data[i:i+batch_size]
            session.run("""
                UNWIND $rows AS row
                MATCH (a:Author {authorName: row.authorName})
                MATCH (p:Paper  {paperId:    row.paperId})
                CREATE (r:Review {
                    content:          row.content,
                    suggestedDecision: row.decision
                })
                CREATE (a)-[:WROTE_REVIEW]->(r)
                CREATE (r)-[:EVALUATES]->(p)
            """, {"rows": batch})

        # Step 3c — delete old REVIEWED relationships
        print("  Deleting old REVIEWED relationships...")
        session.run("MATCH ()-[r:REVIEWED]->() DELETE r")
        print("  Done.")

        # ─────────────────────────────────────
        # 4. SUMMARY
        # ─────────────────────────────────────
        print("\nSummary:")
        result = session.run(
            "MATCH (n) RETURN labels(n) AS label, count(n) AS count ORDER BY count DESC"
        )
        for record in result:
            print(f"  {record['label']}: {record['count']}")

        print("\nRelationships:")
        result = session.run(
            "MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS count ORDER BY count DESC"
        )
        for record in result:
            print(f"  {record['rel']}: {record['count']}")

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()