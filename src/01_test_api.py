'''
import requests

url = "https://api.semanticscholar.org/graph/v1/paper/search"

params = {
    "query": "data management",
    "limit": 1,
    "fields": "paperId,title,year,authors"
}

response = requests.get(url, params=params)

print("Status code:", response.status_code)
print(response.json())
'''

import json

with open("data/raw/test_sample.json", "r") as f:
    data = json.load(f)

for paper in data["data"]:
    print("Title:", paper["title"])
    print("Year:", paper["year"])
    print("Authors:", [a["name"] for a in paper["authors"]])
    print("---")