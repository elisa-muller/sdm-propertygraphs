import pandas as pd

papers = pd.read_csv("neo4j/load_csv/papers_clean.csv", dtype=str, keep_default_na=False)
authors = pd.read_csv("neo4j/load_csv/authors_clean.csv", dtype=str, keep_default_na=False)
wrote = pd.read_csv("neo4j/load_csv/wrote_clean.csv", dtype=str, keep_default_na=False)

main_author = pd.read_csv("neo4j/load_csv/main_author_clean.csv", dtype=str, keep_default_na=False)
reviewed = pd.read_csv("neo4j/load_csv/reviewed_clean.csv", dtype=str, keep_default_na=False)
keywords = pd.read_csv("neo4j/load_csv/keywords_clean.csv", dtype=str, keep_default_na=False)
has_keyword = pd.read_csv("neo4j/load_csv/has_keyword_clean.csv", dtype=str, keep_default_na=False)
cited = pd.read_csv("neo4j/load_csv/cited_clean.csv", dtype=str, keep_default_na=False)

print("papers:", len(papers))
print("authors:", len(authors))
print("main_author:", len(main_author))
print("reviewed:", len(reviewed))
print("keywords:", len(keywords))
print("has_keyword:", len(has_keyword))
print("cited:", len(cited))

# 1. exactly one main author per paper
print("main_author distinct papers:", main_author[":END_ID(Paper)"].nunique())

# 2. no duplicate main_author rows
print("main_author duplicates:", main_author.duplicated().sum())

# 3. no self-review
wrote_pairs = set(zip(wrote[":START_ID(Author)"], wrote[":END_ID(Paper)"]))
reviewed_pairs = set(zip(reviewed[":START_ID(Author)"], reviewed[":END_ID(Paper)"]))
self_reviews = reviewed_pairs.intersection(wrote_pairs)
print("self reviews:", len(self_reviews))

# 4. no self-citations
self_citations = (cited[":START_ID(Paper)"] == cited[":END_ID(Paper)"]).sum()
print("self citations:", self_citations)

# 5. has_keyword valid papers
print("has_keyword distinct papers:", has_keyword[":START_ID(Paper)"].nunique())

# 6. keyword ids valid
print("distinct keywords used:", has_keyword[":END_ID(Keyword)"].nunique())