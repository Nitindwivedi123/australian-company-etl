#!/usr/bin/env python3
# scripts/entity_matching.py

import os
import re
import json
import logging
from typing import List, Dict, Any, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
from rapidfuzz import process, fuzz
from pydantic import BaseModel, Field

# ─── Optional LLM imports (LangChain + Gemini) ──────────────────────────────────
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain.prompts import (
        SystemMessagePromptTemplate,
        HumanMessagePromptTemplate,
        ChatPromptTemplate,
    )
    from langchain.output_parsers import PydanticOutputParser, OutputFixingParser
    from langchain.schema import SystemMessage, HumanMessage
    from google.auth.exceptions import DefaultCredentialsError
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    logger.warning("LangChain not available - LLM fallback disabled")

# ─── Logging ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s")

# ─── Config ─────────────────────────────────────────────────────────────────────
FUZZY_SCORE_CUTOFF = int(os.getenv("FUZZY_SCORE_CUTOFF", "80"))  # Further lowered for testing
ABR_FETCH_LIMIT = int(os.getenv("ABR_FETCH_LIMIT", "10000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
ENABLE_DIAGNOSTICS = os.getenv("ENABLE_DIAGNOSTICS", "true").lower() == "true"

# ─── Enhanced Normalization ─────────────────────────────────────────────────────
ABBREV = {
    r'\bPTY\s*LTD\b': 'PROPRIETARY LIMITED',
    r'\bPROPRIETARY\s*LIMITED\b': 'PROPRIETARY LIMITED',
    r'\bPTY\b': 'PROPRIETARY',
    r'\bLTD\b': 'LIMITED',
    r'\bCO\b': 'COMPANY',
    r'\bCOMP\b': 'COMPANY',
    r'\bCORP\b': 'CORPORATION',
    r'\bINC\b': 'INCORPORATED',
    r'\bLLC\b': 'LIMITED LIABILITY COMPANY',
    r'&AMP;': 'AND',
    r'&': 'AND',
    r'\+': 'AND'
}

# Extended noise patterns for web page titles
NOISE_PATTERNS = [
    r'\b(THE|A|AN)\b',  # Articles
    r'\b(GROUP|HOLDINGS|ENTERPRISES|SERVICES|SOLUTIONS)\b',  # Business terms
    r'\([^)]*\)',  # Content in parentheses
    r'\[[^\]]*\]',  # Content in brackets
    r'\|[^|]*$',  # Everything after last pipe (common in page titles)
    r'\s*-\s*[^-]*$',  # Everything after last dash (page titles)
    r'&#\d+;',  # HTML entities
    r'&[a-zA-Z]+;',  # Named HTML entities
    r'\b(BREEDS?|PHOTOGRAPHER?|SERVICES?|PORTAL|CATALOG|SHOP)\b',  # Web content terms
]

PUNCT_RE = re.compile(r'[^A-Za-z0-9 ]+')
MULTISPACE = re.compile(r'\s+')

def normalize_name(s: str) -> str:
    """Enhanced normalization with web content handling"""
    if not s or not s.strip():
        return ''
    
    # Convert to uppercase and trim
    s = s.upper().strip()
    
    # Handle HTML entities first
    s = s.replace('&AMP;', '&').replace('&QUOT;', '"').replace('&LT;', '<').replace('&GT;', '>')
    
    # Remove noise patterns (web page artifacts)
    for pattern in NOISE_PATTERNS:
        s = re.sub(pattern, '', s, flags=re.IGNORECASE)
    
    # Apply abbreviation standardization
    for pat, repl in ABBREV.items():
        s = re.sub(pat, repl, s, flags=re.IGNORECASE)
    
    # Clean punctuation and normalize spaces
    s = PUNCT_RE.sub(' ', s)
    s = MULTISPACE.sub(' ', s).strip()
    
    return s

def extract_company_name_from_title(title: str) -> str:
    """Try to extract company name from web page title"""
    if not title:
        return ''
    
    # Common patterns for extracting company names from page titles
    patterns = [
        r'^([A-Z][a-zA-Z\s&]+(?:PTY|LTD|LIMITED|COMPANY|CORP|INC))',  # Company name at start
        r'([A-Z][a-zA-Z\s&]+(?:PTY|LTD|LIMITED|COMPANY|CORP|INC))',   # Company name anywhere
        r'^([A-Z][a-zA-Z\s&]{3,20})\s*[-|]',  # Name before separator
        r'^([A-Z][a-zA-Z\s&]{3,30})$',  # Simple name
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            if len(candidate) >= 3 and not re.match(r'^[0-9_\-\s]+$', candidate):
                return candidate
    
    return title  # Return original if no pattern matches

def is_valid_company_name(name: str) -> bool:
    """Check if a company name is valid and meaningful"""
    if not name or len(name.strip()) < 3:
        return False
    
    # Skip names that are obviously not company names
    invalid_patterns = [
        r'^\s*$',  # Empty or whitespace only
        r'^[0-9_\-\s]+$',  # Only numbers, underscores, dashes, spaces
        r'^[^a-zA-Z]*$',  # No letters at all
        r'(PHOTOGRAPHER|WEDDING|COAST|BREEDS|CATALOG|PORTAL|LOGIN|CONTACT)',  # Web content
        r'(RSS|FEED|BLOG|NEWS|ARTICLE)',  # Web content types
        r'^(RE:|LOG\s+IN|CONTACT\s+US)$',  # Common page titles
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, name, re.IGNORECASE):
            return False
    
    return True

def get_company_keywords(name: str) -> set:
    """Extract meaningful keywords from company name"""
    normalized = normalize_name(name)
    # Remove common business terms and keep meaningful words
    words = normalized.split()
    keywords = set()
    
    for word in words:
        if len(word) >= 3 and word not in {'PROPRIETARY', 'LIMITED', 'COMPANY', 'CORPORATION', 'AND', 'THE'}:
            keywords.add(word)
    
    return keywords

def keyword_match_score(abr_name: str, cc_name: str) -> int:
    """Calculate match score based on keyword overlap"""
    abr_keywords = get_company_keywords(abr_name)
    cc_keywords = get_company_keywords(cc_name)
    
    if not abr_keywords or not cc_keywords:
        return 0
    
    intersection = abr_keywords.intersection(cc_keywords)
    union = abr_keywords.union(cc_keywords)
    
    if not union:
        return 0
    
    # Jaccard similarity * 100
    return int((len(intersection) / len(union)) * 100)

# ─── LLM Setup (only if available) ──────────────────────────────────────────────
llm = None
base_parser = None
fixing_parser = None

if LANGCHAIN_AVAILABLE:
    def _build_gemini_llm() -> Optional[ChatGoogleGenerativeAI]:
        try:
            model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
            return ChatGoogleGenerativeAI(model=model, temperature=0.0)
        except DefaultCredentialsError as e:
            logger.warning("No GCP ADC found; skipping LLM fallback: %s", e)
            return None
    
    llm = _build_gemini_llm()

    # ─── Pydantic Models for LLM fallback ───────────────────────────────────────────
    class FallbackRequest(BaseModel):
        ename: str
        options: List[str] = Field(..., description="Candidate company names")

    class FallbackResponseRecord(BaseModel):
        ename: str
        match: str
        confidence: int

    class FallbackResponse(BaseModel):
        records: List[FallbackResponseRecord]

    # ─── LLM prompt and parser ──────────────────────────────────────────────────────
    system_prompt = SystemMessagePromptTemplate.from_template(
        "You are a company-name matching assistant. "
        "Input JSON has `records`: each with `ename` and `options`. "
        "Return JSON with `records`: each has `ename`, a `match` or 'No Match', "
        "and `confidence` (0-100)."
    )
    user_prompt = HumanMessagePromptTemplate.from_template("{payload}")
    prompt = ChatPromptTemplate.from_messages([system_prompt, user_prompt])

    if llm:
        base_parser = PydanticOutputParser(pydantic_object=FallbackResponse)
        fixing_parser = OutputFixingParser.from_llm(parser=base_parser, llm=llm)

# ─── Fallback LLM function ──────────────────────────────────────────────────────
def call_llm_fallback(requests: List[FallbackRequest]) -> FallbackResponse:
    if not llm or not fixing_parser:
        raise RuntimeError("LLM not available for fallback")
    payload = {"records": [r.dict() for r in requests]}
    content = json.dumps(payload, ensure_ascii=False)
    pv = prompt.format_prompt(payload=content)
    messages = [
        SystemMessage(content=pv.to_messages()[0].content),
        HumanMessage(content=pv.to_messages()[1].content),
    ]
    resp = llm.predict_messages(messages)
    return fixing_parser.parse(resp.content)

def fallback_match_and_prepare(to_fallback: List[Dict[str, Any]]) -> List[Tuple]:
    if not LANGCHAIN_AVAILABLE:
        return []
    
    batch = [
        FallbackRequest(ename=rec["ename"], options=[c["name"] for c in rec["candidates"]])
        for rec in to_fallback
    ]
    llm_resp = call_llm_fallback(batch)
    rows: List[Tuple] = []
    for rec in llm_resp.records:
        if rec.match != "No Match":
            src = next(r for r in to_fallback if r["ename"] == rec.ename)
            cand = next(c for c in src["candidates"] if c["name"] == rec.match)
            rows.append((
                src["abn"], src["ename"], src["entity_type"], src["entity_status"],
                src["address"], src["postcode"], src["state"], src["start_date"],
                cand["url"], cand["name"], cand["industry"], rec.confidence
            ))
    return rows

# ─── Diagnostics ────────────────────────────────────────────────────────────────
def run_diagnostics(conn):
    """Run diagnostics to understand data quality"""
    logger.info("=== RUNNING DIAGNOSTICS ===")
    
    with conn.cursor() as cur:
        # Check Common Crawl data quality
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN company_name IS NULL OR TRIM(company_name) = '' THEN 1 END) as empty_names,
                COUNT(CASE WHEN LENGTH(TRIM(company_name)) < 3 THEN 1 END) as short_names
            FROM staging_commoncrawl
        """)
        cc_stats = cur.fetchone()
        logger.info("Common Crawl: %d total, %d empty names, %d short names", 
                   cc_stats[0], cc_stats[1], cc_stats[2])
        
        # Sample company names
        cur.execute("""
            SELECT company_name 
            FROM staging_commoncrawl 
            WHERE company_name IS NOT NULL AND TRIM(company_name) != ''
            LIMIT 10
        """)
        cc_samples = [row[0] for row in cur.fetchall()]
        logger.info("Sample CC names: %s", cc_samples[:5])
        
        # Check ABR data
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN entity_name IS NULL OR TRIM(entity_name) = '' THEN 1 END) as empty_names
            FROM staging_abr
        """)
        abr_stats = cur.fetchone()
        logger.info("ABR: %d total, %d empty names", abr_stats[0], abr_stats[1])
        
        # Sample ABR names
        cur.execute("""
            SELECT entity_name 
            FROM staging_abr 
            WHERE entity_name IS NOT NULL AND TRIM(entity_name) != ''
            LIMIT 10
        """)
        abr_samples = [row[0] for row in cur.fetchall()]
        logger.info("Sample ABR names: %s", abr_samples[:5])
        
        # Show normalization examples
        logger.info("=== NORMALIZATION EXAMPLES ===")
        for i, (cc_name, abr_name) in enumerate(zip(cc_samples[:3], abr_samples[:3])):
            cc_norm = normalize_name(cc_name)
            abr_norm = normalize_name(abr_name)
            cc_extracted = extract_company_name_from_title(cc_name)
            logger.info("Pair %d:", i+1)
            logger.info("  CC: '%s' -> normalized: '%s' -> extracted: '%s'", 
                       cc_name, cc_norm, cc_extracted)
            logger.info("  ABR: '%s' -> normalized: '%s'", abr_name, abr_norm)
            logger.info("  Keyword match score: %d", keyword_match_score(abr_name, cc_extracted))

# ─── Main ETL ───────────────────────────────────────────────────────────────────
def run():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
    )

    if ENABLE_DIAGNOSTICS:
        run_diagnostics(conn)

    # Load CC candidates with enhanced processing
    with conn.cursor() as cur:
        cur.execute("""
            SELECT website_url, company_name, industry
            FROM staging_commoncrawl
            WHERE company_name IS NOT NULL 
            AND TRIM(company_name) != ''
            AND LENGTH(TRIM(company_name)) >= 3
        """)
        cc_raw = cur.fetchall()

    cc_candidates, cc_clean_names = [], []
    valid_count = 0
    
    for url, name, industry in cc_raw:
        # Try to extract company name from title
        extracted_name = extract_company_name_from_title(name)
        
        if is_valid_company_name(extracted_name):
            norm = normalize_name(extracted_name)
            if norm and len(norm) >= 3:
                cc_candidates.append({
                    "url": url, 
                    "name": extracted_name,  # Use extracted name
                    "original_name": name,   # Keep original for reference
                    "industry": industry
                })
                cc_clean_names.append(norm)
                valid_count += 1

    logger.info("Loaded %d valid Common Crawl companies from %d total records", 
                valid_count, len(cc_raw))
    
    if valid_count == 0:
        logger.error("No valid Common Crawl companies found! Check your data quality.")
        logger.error("Consider adjusting is_valid_company_name() function or data preprocessing.")
        return

    # Debug: Show processed examples
    if ENABLE_DIAGNOSTICS:
        logger.info("Sample processed CC names:")
        for i in range(min(5, len(cc_candidates))):
            cand = cc_candidates[i]
            logger.info("  Original: '%s' -> Extracted: '%s' -> Normalized: '%s'", 
                       cand["original_name"], cand["name"], cc_clean_names[i])

    # Stream ABR rows
    abr_cur = conn.cursor(name="abr_cursor", withhold=True)
    abr_cur.itersize = BATCH_SIZE
    abr_cur.execute(f"""
        SELECT abn, entity_name, entity_type, entity_status,
               address, postcode, state, start_date
        FROM staging_abr
        WHERE entity_name IS NOT NULL
        AND TRIM(entity_name) != ''
        LIMIT {ABR_FETCH_LIMIT}
    """)

    total_auto, total_fb, total_processed = 0, 0, 0

    while True:
        batch = abr_cur.fetchmany(BATCH_SIZE)
        if not batch:
            break

        unified, to_fallback = [], []

        for abn, ename, etype, estatus, addr, pcode, st, sdate in batch:
            total_processed += 1
            norm_ename = normalize_name(ename)
            
            if not norm_ename:
                continue
                
            # Debug first few matches
            if total_processed <= 3 and ENABLE_DIAGNOSTICS:
                logger.info("Processing ABR #%d: '%s' -> normalized: '%s'", 
                           total_processed, ename, norm_ename)
            
            # Multi-strategy matching
            best_match = None
            best_score = 0
            
            # Strategy 1: Standard fuzzy matching
            strategies = [
                (fuzz.token_sort_ratio, "token_sort"),
                (fuzz.token_set_ratio, "token_set"),
                (fuzz.ratio, "ratio")
            ]
            
            for scorer, strategy_name in strategies:
                match = process.extractOne(
                    norm_ename,
                    cc_clean_names,
                    scorer=scorer,
                    score_cutoff=FUZZY_SCORE_CUTOFF - 20  # Very low for testing
                )
                
                if match and match[1] > best_score:
                    best_match = match
                    best_score = match[1]
                    if total_processed <= 3 and ENABLE_DIAGNOSTICS:
                        logger.info("  Strategy %s: score %d -> '%s'", 
                                   strategy_name, match[1], cc_candidates[match[2]]["name"])
            
            # Strategy 2: Keyword-based matching
            keyword_matches = []
            for i, cc_name in enumerate([c["name"] for c in cc_candidates]):
                keyword_score = keyword_match_score(ename, cc_name)
                if keyword_score >= 30:  # At least 30% keyword overlap
                    keyword_matches.append((cc_name, keyword_score, i))
            
            # Take best keyword match if better than fuzzy
            if keyword_matches:
                keyword_matches.sort(key=lambda x: x[1], reverse=True)
                best_keyword = keyword_matches[0]
                if best_keyword[1] > best_score:
                    best_match = (best_keyword[0], best_keyword[1], best_keyword[2])
                    best_score = best_keyword[1]
                    if total_processed <= 3 and ENABLE_DIAGNOSTICS:
                        logger.info("  Keyword strategy: score %d -> '%s'", 
                                   best_keyword[1], best_keyword[0])
            
            if best_match and best_score >= FUZZY_SCORE_CUTOFF:
                _, score, idx = best_match
                cand = cc_candidates[idx]
                unified.append((
                    abn, ename, etype, estatus, addr, pcode, st, sdate,
                    cand["url"], cand["name"], cand["industry"], score
                ))
                
                if total_processed <= 3 and ENABLE_DIAGNOSTICS:
                    logger.info("  ✓ MATCH: '%s' -> '%s' (score: %d)", 
                               ename, cand["name"], score)
            else:
                # Prepare for fallback
                top5 = process.extract(
                    norm_ename, 
                    cc_clean_names, 
                    limit=5, 
                    scorer=fuzz.token_sort_ratio
                )
                opts = [cc_candidates[i] for (_, __, i) in top5]
                to_fallback.append({
                    "abn": abn, "ename": ename, "entity_type": etype, "entity_status": estatus,
                    "address": addr, "postcode": pcode, "state": st, "start_date": sdate,
                    "candidates": opts
                })
                
                if total_processed <= 3 and ENABLE_DIAGNOSTICS:
                    logger.info("  ✗ NO MATCH: '%s' -> best score was %d", ename, best_score)

        # Insert auto-matches
        if unified:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO unified_companies
                      (abn, entity_name, entity_type, entity_status,
                       address, postcode, state, start_date,
                       website_url, company_name, industry, merged_confidence)
                    VALUES %s
                    ON CONFLICT (abn) DO NOTHING
                """, unified)
            conn.commit()

        total_auto += len(unified)
        total_fb += len(to_fallback)
        
        if total_processed <= 1000 or total_processed % 5000 == 0:  # Log less frequently
            match_rate = (total_auto / total_processed * 100) if total_processed > 0 else 0
            logger.info("Progress: %d processed, %d matched (%.1f%%), %d fallback", 
                       total_processed, total_auto, match_rate, total_fb)

        # # LLM fallback (if available and enabled)
        # if to_fallback and llm and len(to_fallback) <= 100:  # Limit LLM calls
        #     try:
        #         fb_rows = fallback_match_and_prepare(to_fallback)
        #         if fb_rows:
        #             with conn.cursor() as cur:
        #                 execute_values(cur, """
        #                     INSERT INTO unified_companies
        #                       (abn, entity_name, entity_type, entity_status,
        #                        address, postcode, state, start_date,
        #                        website_url, company_name, industry, merged_confidence)
        #                     VALUES %s
        #                     ON CONFLICT (abn) DO NOTHING
        #                 """, fb_rows)
        #             conn.commit()
        #             logger.info("LLM fallback: %d additional matches", len(fb_rows))
        #     except Exception as e:
        #         logger.warning("LLM fallback failed: %s", e)

    abr_cur.close()
    conn.close()
    
    final_match_rate = (total_auto / total_processed * 100) if total_processed > 0 else 0
    logger.info("=== FINAL RESULTS ===")
    logger.info("Processed: %d ABR records", total_processed)
    logger.info("Auto-matched: %d (%.1f%%)", total_auto, final_match_rate)
    logger.info("Fallback candidates: %d", total_fb)
    
    if final_match_rate < 1:
        logger.warning("Very low match rate! Consider:")
        logger.warning("1. Lowering FUZZY_SCORE_CUTOFF (currently %d)", FUZZY_SCORE_CUTOFF)
        logger.warning("2. Improving data preprocessing")
        logger.warning("3. Using different data sources")

if __name__ == "__main__":
    run()