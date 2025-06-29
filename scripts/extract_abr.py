#!/usr/bin/env python3
"""
Production-ready ABR bulk extract loader (single ZIP, single run).

This script will:
 1. Discover ABR bulk-extract ZIP URLs via CKAN API (or ABR_ZIP_URLS override)
 2. Download the first ZIP to disk
 3. Stream-parse each contained XML via iterparse
 4. Extract: ABN (+status, from-date), EntityTypeText, Main/Other name, State & Postcode, pick a “start_date”
 5. Batch-upsert into staging_abr
 6. Record that ZIP in processed_files to skip next time
"""

import os
import sys
import logging
import requests
import zipfile
import xml.etree.ElementTree as ET
from tempfile import NamedTemporaryFile
import psycopg2
from psycopg2.extras import execute_values

# ─── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("extract_abr")

# ─── CKAN lookup ────────────────────────────────────────────────────────────────
def get_abr_zip_urls():
    override = os.getenv("ABR_ZIP_URLS")
    if override:
        urls = [u.strip() for u in override.split(",") if u.strip()]
        logger.info("Using override ZIP URL(s): %s", urls)
        return urls

    api_url = "https://data.gov.au/api/3/action/package_show?id=abn-bulk-extract"
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    resources = resp.json()["result"]["resources"]

    zip_urls = [
        r["url"] for r in resources
        if r.get("format","").upper()=="XML"
           and r.get("mimetype","").lower()=="application/zip"
           and "part" in r.get("name","").lower()
    ]
    if not zip_urls:
        raise RuntimeError("No ABR ZIP resources found via CKAN API")
    logger.info("Discovered %d ZIP URL(s)", len(zip_urls))
    return sorted(zip_urls)

# ─── bulk upsert ────────────────────────────────────────────────────────────────
def _bulk_upsert(conn, records):
    sql = """
    INSERT INTO staging_abr
      (abn, entity_name, entity_type, entity_status, address, postcode, state, start_date)
    VALUES %s
    ON CONFLICT (abn) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()

# ─── namespace-safe lookups ─────────────────────────────────────────────────────
def find_text(elem, path):
    # try exact
    node = elem.find(path)
    if node is not None and node.text:
        return node.text.strip()
    # deep search for final tag
    tag = path.split("/")[-1]
    node = elem.find(f".//{tag}")
    return node.text.strip() if node is not None and node.text else None

def find_elem(elem, tag):
    node = elem.find(tag)
    if node is not None:
        return node
    node = elem.find(f".//{tag}")
    if node is not None:
        return node
    # handle namespaced
    for c in elem.iter():
        if c.tag.endswith(tag):
            return c
    return None

# ─── process one ZIP ────────────────────────────────────────────────────────────
def process_zip(conn, zip_url, batch_size=1000):
    logger.info("Downloading ZIP: %s", zip_url)
    with NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        for chunk in requests.get(zip_url, stream=True, timeout=300).iter_content(8_192):
            tmp.write(chunk)
        tmp_path = tmp.name

    total = 0
    try:
        with zipfile.ZipFile(tmp_path) as zf:
            xmls = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not xmls:
                raise RuntimeError("No XMLs in %s" % zip_url)

            for xml_name in xmls:
                logger.info("Parsing %s", xml_name)
                batch, count = [], 0
                with zf.open(xml_name) as f:
                    for _, elem in ET.iterparse(f, events=("end",)):
                        if elem.tag=="ABR" or elem.tag.endswith("}ABR"):
                            # ABN
                            abn_el = find_elem(elem, "ABN")
                            abn = abn_el.text.strip() if abn_el is not None else None
                            status = abn_el.get("status") if abn_el is not None else None
                            abn_date = abn_el.get("ABNStatusFromDate") if abn_el is not None else None
                            if not abn:
                                elem.clear(); continue

                            # Type
                            etype = find_text(elem, "EntityType/EntityTypeText")
                            if not etype:
                                etype = find_text(elem, "EntityTypeText")

                            # Name
                            name = find_text(elem, "MainEntity/NonIndividualName/NonIndividualNameText")
                            if not name:
                                name = find_text(elem, "NonIndividualNameText")

                            # Address
                            state = find_text(elem, "AddressDetails/State") or find_text(elem, "State")
                            post = find_text(elem, "AddressDetails/Postcode") or find_text(elem, "Postcode")
                            address = ", ".join(filter(None, [state, post])) or None

                            # choose start_date
                            gst_el = find_elem(elem, "GST")
                            gst_date = gst_el.get("GSTStatusFromDate") if gst_el is not None else None
                            start_date = abn_date or gst_date

                            batch.append((abn, name, etype, status, address, post, state, start_date))
                            count += 1
                            if len(batch)>=batch_size:
                                _bulk_upsert(conn, batch)
                                logger.info("  committed %d rows", len(batch))
                                batch.clear()

                            elem.clear()

                if batch:
                    _bulk_upsert(conn, batch)
                    logger.info("  committed final %d rows", len(batch))
                total += count
                logger.info("✔ %s: %d records", xml_name, count)

    finally:
        os.unlink(tmp_path)

    logger.info("✔ Completed ZIP, total %d records", total)
    return total

# ─── main ───────────────────────────────────────────────────────────────────────
def run():
    urls = get_abr_zip_urls()
    zip_url = urls[0]
    logger.info("Processing only this ZIP: %s", zip_url)

    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB","company_db"),
        user=os.getenv("POSTGRES_USER","etl_user"),
        password=os.getenv("POSTGRES_PASSWORD","securepassword"),
        host=os.getenv("POSTGRES_HOST","postgres"),
    )

    # skip if already done
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM processed_files WHERE file_path=%s", (zip_url,))
        if cur.fetchone():
            logger.info("Already processed, exiting.")
            return

    count = process_zip(conn, zip_url, batch_size=int(os.getenv("BATCH_SIZE","1000")))

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO processed_files (file_path, processed_at) VALUES (%s, NOW()) ON CONFLICT DO NOTHING",
            (zip_url,)
        )
    conn.commit()
    conn.close()
    logger.info("All done: %d rows loaded", count)

if __name__=="__main__":
    run()
