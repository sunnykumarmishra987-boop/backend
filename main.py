"""
Chardi.ai — FastAPI Backend
Connects to Supabase Postgres and serves tenders API.

Install:
    pip install fastapi uvicorn psycopg2-binary python-dotenv

Run:
    uvicorn main:app --reload --port 8000
"""

import os
import math
import io
import csv
import json
from typing import Optional
from datetime import datetime
import uvicorn
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv

load_dotenv()

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Chardi.ai API",
    description="Government tender intelligence — India",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # tighten to your Vercel domain in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Portals covered ────────────────────────────────────────────────────────────

ALLOWED_PORTALS = {
    "cppp",
    "mahatenders",
    "kppp",             
    "tntenders",
    "up_eprocurement",         # Updated
    "telangana_eprocurement",  # Updated
    "wbtenders",
    "kerala_tenders",          # Updated
    "ap_eprocurement",
    "rajasthan_eprocurement",  # Updated
}

# ── DB connection ──────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# ── Helpers ────────────────────────────────────────────────────────────────────

def row_to_dict(row) -> dict:
    d = dict(row)
    # Convert datetime objects to ISO strings for JSON
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d

def build_where(
    q:               Optional[str],
    status:          Optional[str],
    state:           Optional[str],
    buyer_type:      Optional[list[str]],
    portal:          Optional[list[str]],
    min_value:       Optional[float],
    max_value:       Optional[float],
    deadline_after:  Optional[str],
    deadline_before: Optional[str],
) -> tuple[str, list]:
    """Build WHERE clause and params list from filter args."""
    clauses = ["is_deleted = FALSE"]
    params  = []

    # Restrict to covered portals
    portal_filter = [p for p in (portal or []) if p in ALLOWED_PORTALS]
    if portal_filter:
        placeholders = ",".join(["%s"] * len(portal_filter))
        clauses.append(f"source_portal IN ({placeholders})")
        params.extend(portal_filter)
    else:
        placeholders = ",".join(["%s"] * len(ALLOWED_PORTALS))
        clauses.append(f"source_portal IN ({placeholders})")
        params.extend(list(ALLOWED_PORTALS))

    if q:
        clauses.append(
            "to_tsvector('english', coalesce(title,'') || ' ' || coalesce(description,'') || ' ' || coalesce(buyer_name,''))"
            " @@ plainto_tsquery('english', %s)"
        )
        params.append(q)

    if status and status != "all":
        clauses.append("status = %s")
        params.append(status)

    if state and state != "all":
        if state == "central":
            clauses.append("state IS NULL")
        else:
            clauses.append("state = %s")
            params.append(state)

    if buyer_type:
        placeholders = ",".join(["%s"] * len(buyer_type))
        clauses.append(f"buyer_type IN ({placeholders})")
        params.extend(buyer_type)

    if min_value is not None:
        clauses.append("value >= %s")
        params.append(min_value)

    if max_value is not None:
        clauses.append("value <= %s")
        params.append(max_value)

    if deadline_after:
        clauses.append("deadline_at >= %s")
        params.append(deadline_after)

    if deadline_before:
        clauses.append("deadline_at <= %s")
        params.append(deadline_before)

    where = "WHERE " + " AND ".join(clauses)
    return where, params

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/api/tenders")
def list_tenders(
    q:               Optional[str]   = Query(None,  description="Full-text search"),
    status:          Optional[str]   = Query(None,  description="open|closed|cancelled|all"),
    state:           Optional[str]   = Query(None,  description="2-letter state code or 'central'"),
    buyer_type:      list[str]       = Query([],    description="central_ministry|psu|state_govt|defence"),
    portal:          list[str]       = Query([],    description="Portal filter"),
    min_value:       Optional[float] = Query(None,  description="Min tender value (INR)"),
    max_value:       Optional[float] = Query(None,  description="Max tender value (INR)"),
    deadline_after:  Optional[str]   = Query(None,  description="ISO date string"),
    deadline_before: Optional[str]   = Query(None,  description="ISO date string"),
    sort_by:         Optional[str]   = Query("published_at", description="Column to sort by"),
    sort_dir:        Optional[str]   = Query("desc", description="asc|desc"),
    page:            int             = Query(1,     ge=1),
    limit:           int             = Query(25,    ge=1, le=100),
):
    # Validate sort column to prevent SQL injection
    allowed_sorts = {"published_at", "deadline_at", "value", "title", "buyer_name", "scraped_at"}
    sort_col = sort_by if sort_by in allowed_sorts else "published_at"
    sort_direction = "ASC" if sort_dir == "asc" else "DESC"

    where, params = build_where(
        q, status, state, buyer_type, portal,
        min_value, max_value, deadline_after, deadline_before
    )

    offset = (page - 1) * limit

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Total count
        cur.execute(f"SELECT COUNT(*) as cnt FROM tenders {where}", params)
        total = cur.fetchone()["cnt"]

        # Paginated rows
        cur.execute(f"""
            SELECT
                id, tender_ref_no, source_portal, source_url,
                title, buyer_name, buyer_type, state,
                published_at, deadline_at, status,
                value, currency, category, description,
                document_urls, scraped_at, nit_number
            FROM tenders
            {where}
            ORDER BY {sort_col} {sort_direction} NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        tenders = [row_to_dict(r) for r in cur.fetchall()]

        cur.close()
        conn.close()

        return {
            "tenders":     tenders,
            "total":       total,
            "page":        page,
            "limit":       limit,
            "total_pages": max(1, math.ceil(total / limit)),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tenders/export")
def export_tenders(
    format:          str             = Query("csv", description="csv|json"),
    q:               Optional[str]   = Query(None),
    status:          Optional[str]   = Query(None),
    state:           Optional[str]   = Query(None),
    buyer_type:      list[str]       = Query([]),
    portal:          list[str]       = Query([]),
    min_value:       Optional[float] = Query(None),
    max_value:       Optional[float] = Query(None),
    deadline_after:  Optional[str]   = Query(None),
    deadline_before: Optional[str]   = Query(None),
):
    where, params = build_where(
        q, status, state, buyer_type, portal,
        min_value, max_value, deadline_after, deadline_before
    )

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute(f"""
            SELECT
                id, tender_ref_no, source_portal, source_url,
                title, buyer_name, buyer_type, state,
                published_at, deadline_at, status,
                value, currency, category, scraped_at
            FROM tenders
            {where}
            ORDER BY published_at DESC NULLS LAST
            LIMIT 5000
        """, params)

        rows = [row_to_dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()

        date_str = datetime.now().strftime("%Y-%m-%d")

        if format == "json":
            content = json.dumps(rows, indent=2, default=str)
            return StreamingResponse(
                io.StringIO(content),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename=chardi-tenders-{date_str}.json"}
            )

        # CSV
        headers = ["id","tender_ref_no","source_portal","title","buyer_name",
                   "buyer_type","state","status","value","currency","category",
                   "published_at","deadline_at","source_url"]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=chardi-tenders-{date_str}.csv"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tenders/{tender_id}")
def get_tender(tender_id: str):
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute(
            "SELECT * FROM tenders WHERE id = %s AND is_deleted = FALSE",
            (tender_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="TENDER_NOT_FOUND")

        return row_to_dict(row)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
def get_stats():
    try:
        conn = get_conn()
        cur  = conn.cursor()

        portal_placeholders = ",".join(["%s"] * len(ALLOWED_PORTALS))
        portal_params = list(ALLOWED_PORTALS)

        cur.execute(f"""
            SELECT
                COUNT(*)                                    AS total,
                COUNT(*) FILTER (WHERE status = 'open')    AS open_count,
                SUM(value)                                  AS total_value,
                COUNT(DISTINCT source_portal)               AS portal_count,
                MAX(scraped_at)                             AS last_scraped_at
            FROM tenders
            WHERE is_deleted = FALSE
              AND source_portal IN ({portal_placeholders})
        """, portal_params)

        row = row_to_dict(cur.fetchone())
        cur.close()
        conn.close()

        return {
            "total":          int(row["total"] or 0),
            "open_count":     int(row["open_count"] or 0),
            "total_value":    float(row["total_value"] or 0),
            "portal_count":   int(row["portal_count"] or 0),
            "last_scraped_at": row["last_scraped_at"],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/chart/state")
def chart_by_state(
    portal:          list[str]   = Query([]),
    status:          Optional[str] = Query(None),
    buyer_type:      list[str]   = Query([]),
):
    """Returns tender counts grouped by state for the bar chart."""
    where, params = build_where(
        q=None, status=status, state=None,
        buyer_type=buyer_type, portal=portal,
        min_value=None, max_value=None,
        deadline_after=None, deadline_before=None
    )

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute(f"""
            SELECT
                COALESCE(state, 'Central') AS state_label,
                COUNT(*) AS count
            FROM tenders
            {where}
            GROUP BY state_label
            ORDER BY count DESC
            LIMIT 20
        """, params)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [{"state": r["state_label"], "count": int(r["count"])} for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    

    # Railway assigns a dynamic port. Default to 8000 for local dev.
    port = int(os.getenv("PORT", 8000))

    # Use 0.0.0.0 to allow external access, disable reload in production
    uvicorn.run("main:app", host="0.0.0.0", port=port)