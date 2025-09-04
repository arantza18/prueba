
import os
import json
import re
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from datetime import datetime
from decimal import Decimal
import psycopg

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _sanitize_ident(name: str) -> str:
    if not name:
        raise ValueError("Empty identifier")
    if not _IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

def _json_default(obj):
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)

def _connect():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL env var is not set")
    return psycopg.connect(dsn, autocommit=True)

def _list_tables(cur):
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY 1,2
    """)
    return [{"schema": s, "table": t} for (s, t) in cur.fetchall()]

def _get_columns(cur, schema, table):
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, table))
    return [{"name": r[0], "data_type": r[1]} for r in cur.fetchall()]

def _column_null_count(cur, schema, table, col):
    sql = f"SELECT COUNT(*) FILTER (WHERE \"{col}\" IS NULL) FROM \"{schema}\".\"{table}\""
    cur.execute(sql)
    return cur.fetchone()[0]

def _column_unique_count(cur, schema, table, col):
    sql = f"SELECT COUNT(DISTINCT \"{col}\") FROM \"{schema}\".\"{table}\""
    cur.execute(sql)
    return cur.fetchone()[0]

def _numeric_stats(cur, schema, table, col):
    sql = f"""
        SELECT
            COUNT(\"{col}\")::bigint AS n,
            MIN(\"{col}\") AS min,
            MAX(\"{col}\") AS max,
            AVG(\"{col}\") AS mean,
            STDDEV_SAMP(\"{col}\") AS stddev,
            PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY \"{col}\") AS q1,
            PERCENTILE_DISC(0.5)  WITHIN GROUP (ORDER BY \"{col}\") AS median,
            PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY \"{col}\") AS q3
        FROM \"{schema}\".\"{table}\"
        WHERE \"{col}\" IS NOT NULL
    """
    cur.execute(sql)
    row = cur.fetchone()
    return dict(n=row[0], min=row[1], max=row[2], mean=row[3], stddev=row[4], q1=row[5], median=row[6], q3=row[7])

def _table_row_counts_and_dupes(cur, schema, table, columns):
    col_list = ", ".join([f'\"{c["name"]}\"' for c in columns])
    sql = f"""
        SELECT
            COUNT(*)::bigint AS total_rows,
            COUNT(*)::bigint - COUNT(DISTINCT ({col_list}))::bigint AS duplicate_rows
        FROM \"{schema}\".\"{table}\"
    """
    cur.execute(sql)
    total, dupes = cur.fetchone()
    pct = float(dupes) / float(total) if total else 0.0
    return dict(total_rows=int(total), duplicate_rows=int(dupes), duplicate_pct=pct)

def _histogram(cur, schema, table, col, buckets=20):
    sql = f"""
        WITH stats AS (
            SELECT MIN(\"{col}\") AS mn, MAX(\"{col}\") AS mx
            FROM \"{schema}\".\"{table}\" WHERE \"{col}\" IS NOT NULL
        ), hist AS (
            SELECT width_bucket(\"{col}\", stats.mn, stats.mx, %s) AS bkt, COUNT(*)::bigint AS ct,
                   stats.mn, stats.mx
            FROM \"{schema}\".\"{table}\", stats
            WHERE \"{col}\" IS NOT NULL
            GROUP BY bkt, stats.mn, stats.mx
            ORDER BY bkt
        )
        SELECT bkt, ct, mn, mx FROM hist
    """
    cur.execute(sql, (buckets,))
    data = [{
        "bucket": int(r[0]),
        "count": int(r[1]),
        "min": float(r[2]) if r[2] is not None else None,
        "max": float(r[3]) if r[3] is not None else None
    } for r in cur.fetchall()]
    return data

class handler(BaseHTTPRequestHandler):
    def _set_headers(self, status=200, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def _write_json(self, payload, status=200):
        self._set_headers(status=status, content_type="application/json; charset=utf-8")
        self.wfile.write(json.dumps(payload, default=_json_default).encode("utf-8"))

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path or "/"
            qs = parse_qs(parsed.query or "")

            if path.startswith("/api/health"):
                return self._write_json({"ok": True, "ts": datetime.utcnow().isoformat()})

            if path.startswith("/api/tables"):
                with _connect() as conn, conn.cursor() as cur:
                    tables = _list_tables(cur)
                return self._write_json({"tables": tables})

            if path.startswith("/api/summary"):
                tbl = (qs.get("table") or [""])[0]
                if "." in tbl:
                    sch, tab = tbl.split(".", 1)
                else:
                    sch, tab = "public", tbl or ""
                sch = _sanitize_ident(sch)
                tab = _sanitize_ident(tab)
                with _connect() as conn, conn.cursor() as cur:
                    cols = _get_columns(cur, sch, tab)
                    if not cols:
                        return self._write_json({"error": f"Tabla {sch}.{tab} no encontrada"}, status=404)
                    counts = _table_row_counts_and_dupes(cur, sch, tab, cols)
                    col_metrics = []
                    for c in cols:
                        name = c["name"]; dtype = c["data_type"]
                        nulls = _column_null_count(cur, sch, tab, name)
                        uniq = _column_unique_count(cur, sch, tab, name)
                        entry = {"name": name, "data_type": dtype, "nulls": int(nulls), "unique": int(uniq)}
                        if any(x in (dtype or "").lower() for x in ["integer","numeric","double","real","bigint","smallint","decimal"]):
                            try:
                                entry["numeric_stats"] = _numeric_stats(cur, sch, tab, name)
                            except Exception as e:
                                entry["numeric_stats_error"] = str(e)
                        col_metrics.append(entry)
                return self._write_json({"table": f"{sch}.{tab}", "row_stats": counts, "columns": col_metrics})

            if path.startswith("/api/histogram"):
                tbl = (qs.get("table") or [""])[0]
                col = (qs.get("column") or [""])[0]
                buckets = int((qs.get("buckets") or ["20"])[0])
                if "." in tbl:
                    sch, tab = tbl.split(".", 1)
                else:
                    sch, tab = "public", tbl or ""
                sch = _sanitize_ident(sch); tab = _sanitize_ident(tab); col = _sanitize_ident(col)
                with _connect() as conn, conn.cursor() as cur:
                    data = _histogram(cur, sch, tab, col, buckets=buckets)
                return self._write_json({"table": f"{sch}.{tab}", "column": col, "buckets": buckets, "histogram": data})

            if path.startswith("/api/preview"):
                tbl = (qs.get("table") or [""])[0]
                limit = int((qs.get("limit") or ["10"])[0])
                if "." in tbl:
                    sch, tab = tbl.split(".", 1)
                else:
                    sch, tab = "public", tbl or ""
                sch = _sanitize_ident(sch); tab = _sanitize_ident(tab)
                with _connect() as conn, conn.cursor() as cur:
                    cur.execute(f'SELECT * FROM "{sch}"."{tab}" LIMIT %s', (limit,))
                    rows = cur.fetchall()
                    colnames = [d[0] for d in cur.description]
                return self._write_json({"columns": colnames, "rows": rows})

            return self._write_json({"error": "Not found"}, status=404)

        except Exception as e:
            return self._write_json({"error": str(e)}, status=500)
