import json
import csv
import os

# -------- Paths --------
input_file = "data/raw/test_sample.json"
output_dir = "neo4j/import"

os.makedirs(output_dir, exist_ok=True)

# -------- Read raw JSON --------
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

papers = []
authors_dict = {}
wrote = []

# -------- Extract data --------
for paper in data.get("data", []):
    paper_id = paper.get("paperId")
    title = paper.get("title", "")
    year = paper.get("year")
    abstract = paper.get("abstract", "")
    citation_count = paper.get("citationCount", 0)
    reference_count = paper.get("referenceCount", 0)
    venue = paper.get("venue", "")

    # Add paper row
    papers.append({
        "paperId": paper_id,
        "title": title,
        "year": year,
        "abstract": abstract,
        "citationCount": citation_count,
        "referenceCount": reference_count,
        "venue": venue
    })

    # Add authors and WROTE relationships
    for author in paper.get("authors", []):
        author_id = author.get("authorId")
        author_name = author.get("name", "")

        if author_id not in authors_dict:
            authors_dict[author_id] = {
                "authorId": author_id,
                "name": author_name
            }

        wrote.append({
            "authorId": author_id,
            "paperId": paper_id
        })

# Convert authors dict to list
authors = list(authors_dict.values())

# -------- Write papers.csv --------
with open(os.path.join(output_dir, "papers.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["paperId", "title", "year", "abstract", "citationCount", "referenceCount", "venue"]
    )
    writer.writeheader()
    writer.writerows(papers)

# -------- Write authors.csv --------
with open(os.path.join(output_dir, "authors.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["authorId", "name"]
    )
    writer.writeheader()
    writer.writerows(authors)

# -------- Write wrote.csv --------
with open(os.path.join(output_dir, "wrote.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["authorId", "paperId"]
    )
    writer.writeheader()
    writer.writerows(wrote)

print("CSV files created successfully in neo4j/import/")
print(f"papers.csv: {len(papers)} rows")
print(f"authors.csv: {len(authors)} rows")
print(f"wrote.csv: {len(wrote)} rows")