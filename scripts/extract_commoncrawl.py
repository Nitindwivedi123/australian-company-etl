#!/usr/bin/env python3
import os
import re
import gzip
import psycopg2
import requests
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ─── CONFIG ───────────────────────────────────────────────────────────────────
WARC_INDEX_URL = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/warc.paths.gz"
MAX_WARCS     = 5         # slice for testing; remove or increase for full run
MAX_WORKERS   = 12         # tuned to your CPU / network
BATCH_SIZE    = 500      # larger batch = fewer commits

DB_CONF = dict(
    dbname   = os.getenv("POSTGRES_DB",      "company_db"),
    user     = os.getenv("POSTGRES_USER",    "etl_user"),
    password = os.getenv("POSTGRES_PASSWORD","securepassword"),
    host     = os.getenv("POSTGRES_HOST",    "postgres"),
)

# ─── HTML EXTRACTION (regex-based) ─────────────────────────────────────────────
_title_re    = re.compile(b"<title>(.*?)</title>", re.IGNORECASE|re.DOTALL)
_meta_ind_re = re.compile(
    b"<meta\\s+name=[\"']industry[\"']\\s+content=[\"'](.*?)[\"']",
    re.IGNORECASE|re.DOTALL
)

def extract_html_fields(raw: bytes):
    """Fast, regex‐based grab of <title> and <meta name='industry'>."""
    title = None
    m = _title_re.search(raw)
    if m:
        try:
            title = m.group(1).strip().decode("utf8", "ignore")
        except:
            title = None

    industry = None
    m2 = _meta_ind_re.search(raw)
    if m2:
        try:
            industry = m2.group(1).strip().decode("utf8", "ignore")
        except:
            industry = None

    return title, industry

# ─── WARC WORKER ────────────────────────────────────────────────────────────────
def fetch_and_parse(path: str):
    url = f"https://data.commoncrawl.org/{path}"
    session = requests.Session()
    try:
        r = session.get(url, stream=True, timeout=60)
        r.raise_for_status()
        gz = gzip.GzipFile(fileobj=r.raw)

        records = []
        for rec in ArchiveIterator(gz):
            if rec.rec_type != "response":
                continue
            page_url = rec.rec_headers.get_header("WARC-Target-URI")
            if not page_url or ".au" not in page_url:
                continue

            raw = rec.content_stream().read()
            title, industry = extract_html_fields(raw)
            records.append((page_url, title, industry))

        return path, records

    except Exception as e:
        print(f"[ERROR] Failed {path}: {e}")
        return path, []

# ─── MAIN ETL ─────────────────────────────────────────────────────────────────
def run():
    # 1. Download & decompress index
    idx = requests.get(WARC_INDEX_URL, timeout=60)
    idx.raise_for_status()
    all_paths = gzip.decompress(idx.content).decode().splitlines()[:MAX_WARCS]

    # 2. Load already‐processed from Postgres
    conn = psycopg2.connect(**DB_CONF)
    cur  = conn.cursor()
    cur.execute("SELECT file_path FROM processed_files")
    done = {r[0] for r in cur}
    cur.close()

    to_do = [p for p in all_paths if p not in done]
    if not to_do:
        print(" No new WARCs to process.")
        conn.close()
        return

    # 3. Shared buffer & lock for batching
    buffer = []
    buf_lock = Lock()
    total   = 0

    def handle_records(path, recs):
        nonlocal total
        if not recs:
            print(f"[SKIP] {path}: 0 records")
            return

        with buf_lock:
            buffer.extend(recs)
            # once buffer is big enough, flush
            if len(buffer) >= BATCH_SIZE:
                batch = buffer[:BATCH_SIZE]
                del buffer[:BATCH_SIZE]

                cur = conn.cursor()
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO staging_commoncrawl (website_url, company_name, industry)
                    VALUES %s ON CONFLICT DO NOTHING
                    """,
                    batch
                )
                # mark these WARCs processed
                cur.execute(
                    "INSERT INTO processed_files (file_path) VALUES %s ON CONFLICT DO NOTHING",
                    [(path,)]
                )
                conn.commit()
                cur.close()
                total += len(batch)
                print(f"[BATCH] committed {len(batch)} rows (total {total})")

    # 4. Download & parse in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = { pool.submit(fetch_and_parse, p): p for p in to_do }
        for fut in as_completed(futures):
            path = futures[fut]
            p, recs = fut.result()
            handle_records(p, recs)

    # 5. Flush any remaining
    if buffer:
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO staging_commoncrawl (website_url, company_name, industry)
            VALUES %s ON CONFLICT DO NOTHING
            """,
            buffer
        )
        # mark all processed
        for path in to_do:
            cur.execute(
                "INSERT INTO processed_files (file_path) VALUES (%s) ON CONFLICT DO NOTHING",
                (path,)
            )
        conn.commit()
        total += len(buffer)
        print(f"[FINAL] committed {len(buffer)} rows (total {total})")
        cur.close()

    conn.close()
    print(f"All done. Inserted {total} rows.")

if __name__ == "__main__":
    run()
