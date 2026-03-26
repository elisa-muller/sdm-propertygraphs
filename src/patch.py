from neo4j import GraphDatabase

# ─────────────────────────────────────────────
# CONNECTION — update password if needed
# ─────────────────────────────────────────────
URI  = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "your_password")

driver = GraphDatabase.driver(URI, auth=AUTH)

RENAMES = [
    ("data mining",       "data processing"),
    ("database systems",  "data management"),
    ("distributed systems","data storage"),
    ("optimization",      "indexing"),
    ("data integration",  "data modeling"),
    ("query processing",  "data querying"),
]


def main():
    with driver.session(database="neo4j") as session:
        print("Renaming keyword nodes in Neo4j...")

        for old_name, new_name in RENAMES:
            result = session.run("""
                MATCH (k:Keyword {keywordName: $old})
                SET k.keywordName = $new
                RETURN k.keywordName AS renamed
            """, {"old": old_name, "new": new_name})

            records = list(result)
            if records:
                print(f"  '{old_name}' → '{new_name}' ✓")
            else:
                print(f"  '{old_name}' NOT FOUND (already renamed or missing)")

        # Verify
        print("\nCurrent keyword nodes:")
        result = session.run(
            "MATCH (k:Keyword) RETURN k.keywordName AS kw ORDER BY kw"
        )
        for r in result:
            print(f"  {r['kw']}")

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()