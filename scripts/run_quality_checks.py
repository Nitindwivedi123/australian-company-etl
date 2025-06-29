#!/usr/bin/env python3

import psycopg2
import os
import sys
import logging

DB_CONF = dict(
    dbname   = os.getenv("POSTGRES_DB",      "company_db"),
    user     = os.getenv("POSTGRES_USER",    "etl_user"),
    password = os.getenv("POSTGRES_PASSWORD","securepassword"),
    host     = os.getenv("POSTGRES_HOST",    "postgres"),
    port     = os.getenv("POSTGRES_PORT",    "5432"),
)

TABLES = ["staging_commoncrawl", "staging_abr", "unified_companies"]

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def run():
    logger.info("Starting quality checks on Australian company pipeline data...")

    try:
        conn = psycopg2.connect(**DB_CONF)
    except Exception as e:
        logger.error("Failed to connect to PostgreSQL: %s", e)
        sys.exit(1)

    conn.autocommit = True  # Needed for VACUUM
    cur = conn.cursor()

    # VACUUM ANALYZE each table
    for table in TABLES:
        try:
            cur.execute(f"VACUUM ANALYZE {table};")
            logger.info("VACUUM ANALYZE completed on %s", table)
        except Exception as e:
            logger.warning("Could not VACUUM %s: %s", table, e)

    # Print row counts
    for table in TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            logger.info("%s: %d rows", table, count)
        except Exception as e:
            logger.warning("Could not count rows in %s: %s", table, e)

    # NULL checks in unified_companies
    logger.info("Checking NULLs in critical columns of unified_companies...")
    for col in ["abn", "entity_name", "website_url"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM unified_companies WHERE {col} IS NULL;")
            count = cur.fetchone()[0]
            logger.info("%s IS NULL: %d rows", col, count)
        except Exception as e:
            logger.warning("Could not check NULLs for %s: %s", col, e)

    # merged_confidence distribution with streaming cursor for big data
    logger.info("merged_confidence distribution in unified_companies:")
    try:
        dist_cur = conn.cursor(name="conf_cursor", withhold=True)
        dist_cur.execute("""
            SELECT merged_confidence, COUNT(*)
            FROM unified_companies
            GROUP BY merged_confidence
            ORDER BY merged_confidence;
        """)
        for val, cnt in dist_cur:
            logger.info("merged_confidence = %s: %d rows", val, cnt)
        dist_cur.close()
    except Exception as e:
        logger.warning("Could not get merged_confidence distribution: %s", e)

    # Check duplicate ABNs
    logger.info("Checking for duplicate ABNs in unified_companies...")
    try:
        dup_cur = conn.cursor(name="dup_cursor", withhold=True)
        dup_cur.execute("""
            SELECT abn, COUNT(*)
            FROM unified_companies
            GROUP BY abn
            HAVING COUNT(*) > 1;
        """)
        found = False
        for abn, count in dup_cur:
            found = True
            logger.info("ABN %s occurs %d times", abn, count)
        if not found:
            logger.info("No duplicate ABNs found.")
        dup_cur.close()
    except Exception as e:
        logger.warning("Could not check duplicate ABNs: %s", e)

    # Check text indexes (pg_trgm / tsvector)
    logger.info("Verifying pg_trgm / tsvector indexes...")
    try:
        cur.execute("""
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND (indexname LIKE 'idx_%trgm%' OR indexname LIKE 'idx_%tsvector%')
        """)
        indexes = cur.fetchall()
        if indexes:
            for idx, tbl in indexes:
                logger.info("Found index %s on %s", idx, tbl)
        else:
            logger.warning("No pg_trgm or tsvector indexes found. Consider adding for better performance.")
    except Exception as e:
        logger.warning("Could not verify text indexes: %s", e)

    cur.close()
    conn.close()
    logger.info("All quality checks completed.")

if __name__ == "__main__":
    run()
