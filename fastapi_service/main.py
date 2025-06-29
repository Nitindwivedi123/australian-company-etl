from fastapi import FastAPI, Query
import psycopg2
import os

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="postgres"
    )

@app.get("/companies/{abn}")
def get_company_by_abn(abn: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM unified_companies WHERE abn = %s", (abn,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {
            "abn": row[1],
            "entity_name": row[2],
            "entity_type": row[3],
            "entity_status": row[4],
            "address": row[5],
            "postcode": row[6],
            "state": row[7],
            "start_date": row[8],
            "website_url": row[9],
            "company_name": row[10],
            "industry": row[11],
            "merged_confidence": row[12]
        }
    return {"error": "Company not found"}

@app.get("/companies/search")
def search_companies(name: str = Query(..., description="Company name to search for")):
    conn = get_conn()
    cur = conn.cursor()
    query = """SELECT * FROM unified_companies WHERE entity_name ILIKE %s OR company_name ILIKE %s"""
    cur.execute(query, (f"%{name}%", f"%{name}%"))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"abn": r[1], "entity_name": r[2], "company_name": r[10]} for r in rows]

@app.get("/companies/by_state")
def get_companies_by_state(state: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM unified_companies WHERE state = %s", (state,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"abn": r[1], "entity_name": r[2], "company_name": r[10]} for r in rows]
