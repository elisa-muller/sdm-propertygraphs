'''
In this part of the assignment you will have to create a graph following the model designed in the previous section.
You must (partially) instantiate your graph using real data, e.g., load data from Semantic Scholar, DBLP, or any other data source that you may find.
'''

# src/01_download.py
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv


DEFAULT_BASE_URL = "https://api.semanticscholar.org/graph/v1"

# Fields we want for each paper (adapt as needed to match our A.1 model)
DETAIL_FIELDS = [
    "paperId",
    "title",
    "abstract",
    "year",
    "externalIds",          # DOI usually inside externalIds
    "authors",              # includes authorId + name (and sometimes url/homepage depending on API)
    "publicationVenue",     # venue: journal/conference/workshop + id/name/type
    "journal",              # journal metadata (volume/pages/etc.) if applicable
    "url",
]

# Fields for references endpoint (citing -> cited)
REFERENCE_FIELDS = [
    "citedPaper.paperId",
    "citedPaper.title",
    "citedPaper.year",
    "isInfluential",
]


@dataclass
class RetryConfig:
    max_retries: int = 8
    base_sleep_s: float = 1.0
    max_sleep_s: float = 30.0


class S2Client:
    """
    Minimal Semantic Scholar Graph API client with:
    - optional API key
    - exponential backoff for 429 / transient errors
    """

    def __init__(self, base_url: str, api_key: Optional[str], retry: RetryConfig, user_agent: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.retry = retry
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        if api_key:
            # Semantic Scholar typically uses x-api-key
            self.session.headers.update({"x-api-key": api_key})

    def _request(self, method: str, path: str, *, params: Dict[str, Any] | None = None,
                 json_body: Any | None = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(self.retry.max_retries):
            try:
                resp = self.session.request(method, url, params=params, json=json_body, timeout=60)

                # Rate limited
                if resp.status_code == 429:
                    sleep_s = min(self.retry.max_sleep_s, self.retry.base_sleep_s * (2 ** attempt))
                    # If server provides Retry-After, respect it
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            sleep_s = max(sleep_s, float(ra))
                        except ValueError:
                            pass
                    time.sleep(sleep_s)
                    continue

                # Temporary server errors
                if resp.status_code in (500, 502, 503, 504):
                    sleep_s = min(self.retry.max_sleep_s, self.retry.base_sleep_s * (2 ** attempt))
                    time.sleep(sleep_s)
                    continue

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_err = e
                sleep_s = min(self.retry.max_sleep_s, self.retry.base_sleep_s * (2 ** attempt))
                time.sleep(sleep_s)

        raise RuntimeError(f"Request failed after retries: {method} {url}") from last_err

    def search_papers(self, query: str, *, limit: int, fields: List[str],
                      year: Optional[str] = None, min_citations: Optional[int] = None,
                      offset: int = 0, page_size: int = 100) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "query": query,
            "limit": min(page_size, limit),
            "offset": offset,
            "fields": ",".join(fields),
        }
        if year:
            params["year"] = year
        if min_citations is not None:
            # Semantic Scholar search supports minCitationCount
            params["minCitationCount"] = min_citations

        return self._request("GET", "/paper/search", params=params)

    def get_paper(self, paper_id: str, fields: List[str]) -> Dict[str, Any]:
        params = {"fields": ",".join(fields)}
        return self._request("GET", f"/paper/{paper_id}", params=params)

    def get_papers_batch(self, paper_ids: List[str], fields: List[str]) -> List[Dict[str, Any]]:
        # Batch endpoint (more efficient). If it fails for any reason, caller can fallback.
        payload = {"ids": paper_ids}
        params = {"fields": ",".join(fields)}
        data = self._request("POST", "/paper/batch", params=params, json_body=payload)
        # Some APIs return a dict with "data", others a list; handle both.
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return data["data"]
        if isinstance(data, list):
            return data
        raise ValueError("Unexpected response format from /paper/batch")

    def get_references(self, paper_id: str, fields: List[str], limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        params = {
            "fields": ",".join(fields),
            "limit": limit,
            "offset": offset,
        }
        return self._request("GET", f"/paper/{paper_id}/references", params=params)


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    return n


def iter_search_ids(client: S2Client, query: str, *, total_limit: int,
                    year: Optional[str], min_citations: Optional[int], page_size: int) -> List[str]:
    """
    Fetch paperIds using pagination until total_limit.
    """
    ids: List[str] = []
    offset = 0

    # We only need minimal fields during search (paperId)
    search_fields = ["paperId", "title", "year"]

    while len(ids) < total_limit:
        remaining = total_limit - len(ids)
        resp = client.search_papers(
            query,
            limit=remaining,
            fields=search_fields,
            year=year,
            min_citations=min_citations,
            offset=offset,
            page_size=page_size,
        )
        data = resp.get("data", [])
        if not data:
            break

        for item in data:
            pid = item.get("paperId")
            if pid:
                ids.append(pid)
                if len(ids) >= total_limit:
                    break

        # Semantic Scholar returns "next" offset sometimes; if not, we increment by page_size
        next_offset = resp.get("next")
        if next_offset is None:
            offset += page_size
        else:
            offset = int(next_offset)

    # Deduplicate while preserving order
    seen = set()
    unique_ids = []
    for pid in ids:
        if pid not in seen:
            seen.add(pid)
            unique_ids.append(pid)
    return unique_ids


def chunked(xs: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(xs), size):
        yield xs[i : i + size]


def download_details(client: S2Client, paper_ids: List[str], *, fields: List[str],
                     batch_size: int, use_batch: bool) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    if use_batch:
        for batch in chunked(paper_ids, batch_size):
            try:
                items = client.get_papers_batch(batch, fields=fields)
                # Sometimes APIs return None entries for missing items; filter them
                results.extend([x for x in items if isinstance(x, dict) and x.get("paperId")])
            except Exception:
                # Fallback: per-paper
                for pid in batch:
                    try:
                        results.append(client.get_paper(pid, fields=fields))
                    except Exception:
                        # Keep going; record minimal stub so we know it existed
                        results.append({"paperId": pid, "_error": "failed_to_fetch_details"})
    else:
        for pid in paper_ids:
            try:
                results.append(client.get_paper(pid, fields=fields))
            except Exception:
                results.append({"paperId": pid, "_error": "failed_to_fetch_details"})

    return results


def download_references(client: S2Client, paper_ids: List[str], *, fields: List[str],
                        per_paper_limit: int) -> List[Dict[str, Any]]:
    """
    Returns a list of reference records. Each record includes:
      - citingPaperId
      - citedPaper + metadata (depending on requested fields)
    """
    refs_out: List[Dict[str, Any]] = []
    for i, pid in enumerate(paper_ids, start=1):
        offset = 0
        collected = 0
        while collected < per_paper_limit:
            resp = client.get_references(pid, fields=fields, limit=min(1000, per_paper_limit - collected), offset=offset)
            data = resp.get("data", [])
            if not data:
                break
            for r in data:
                # attach citing paper id explicitly (useful later)
                r["citingPaperId"] = pid
                refs_out.append(r)
            collected += len(data)
            next_offset = resp.get("next")
            if next_offset is None:
                offset += len(data)
            else:
                offset = int(next_offset)
    return refs_out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download a subset of papers + references from Semantic Scholar API.")
    p.add_argument("--query", required=True, help="Search query, e.g. 'database systems'")
    p.add_argument("--year", default="2019-2024", help="Year filter, e.g. '2019-2024' or '2022'")
    p.add_argument("--limit", type=int, default=3000, help="Total number of papers to download")
    p.add_argument("--min-citations", type=int, default=1, help="Minimum citation count in search")
    p.add_argument("--out", type=Path, default=Path("data/raw"), help="Output folder for raw JSONL")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL")
    p.add_argument("--page-size", type=int, default=100, help="Search page size (<=100 recommended)")
    p.add_argument("--batch-size", type=int, default=100, help="Batch size for /paper/batch")
    p.add_argument("--no-batch", action="store_true", help="Disable /paper/batch and use per-paper endpoint")
    p.add_argument("--refs-per-paper", type=int, default=200, help="Max references to fetch per paper")
    p.add_argument("--max-retries", type=int, default=8, help="Max retries per request")
    p.add_argument("--user-agent", default="SDM-Lab-Downloader/1.0 (UPC project)", help="Custom User-Agent")
    return p.parse_args()


def main() -> int:
    load_dotenv()  # loads .env if present

    args = parse_args()
    api_key = os.getenv("S2_API_KEY")  # optional; requests can still work unauthenticated but will rate-limit quickly

    retry = RetryConfig(max_retries=args.max_retries)

    client = S2Client(
        base_url=args.base_url,
        api_key=api_key,
        retry=retry,
        user_agent=args.user_agent,
    )

    args.out.mkdir(parents=True, exist_ok=True)

    manifest = {
        "query": args.query,
        "year": args.year,
        "limit": args.limit,
        "min_citations": args.min_citations,
        "page_size": args.page_size,
        "batch_size": args.batch_size,
        "refs_per_paper": args.refs_per_paper,
        "used_api_key": bool(api_key),
        "timestamp_unix": int(time.time()),
        "detail_fields": DETAIL_FIELDS,
        "reference_fields": REFERENCE_FIELDS,
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"[1/3] Searching paperIds (limit={args.limit}) ...")
    paper_ids = iter_search_ids(
        client,
        args.query,
        total_limit=args.limit,
        year=args.year,
        min_citations=args.min_citations,
        page_size=args.page_size,
    )
    print(f"  Found {len(paper_ids)} unique paperIds")

    print("[2/3] Downloading paper details ...")
    details = download_details(
        client,
        paper_ids,
        fields=DETAIL_FIELDS,
        batch_size=args.batch_size,
        use_batch=(not args.no_batch),
    )
    n_details = write_jsonl(args.out / "papers.jsonl", details)
    print(f"  Wrote {n_details} paper records to {args.out / 'papers.jsonl'}")

    print("[3/3] Downloading references (citations) ...")
    refs = download_references(client, paper_ids, fields=REFERENCE_FIELDS, per_paper_limit=args.refs_per_paper)
    n_refs = write_jsonl(args.out / "references.jsonl", refs)
    print(f"  Wrote {n_refs} reference records to {args.out / 'references.jsonl'}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())