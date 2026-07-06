"""
cockroach.py — Loader e metriche per il cluster CockroachDB a 3 nodi
(db/cockroachdb_schema.sql, docker/docker-compose.distributed.yml).

CockroachDB e' wire-compatible con PostgreSQL: psycopg2 funziona senza
modifiche e gran parte delle query di pipeline/metrics.py::PostgresMetrics
sono riusabili quasi identiche. Quello che cambia e' SOTTO al livello SQL:

  - una tabella e' divisa in RANGE (~512MiB) replicati 3x via Raft;
  - un client si connette a UN SOLO nodo, che fa da "gateway/coordinatore"
    per la query, instradando internamente verso i nodi che possiedono i
    range coinvolti (il client non lo vede, ma lo paga in latenza);
  - EXPLAIN riporta un campo 'distribution: local|full' che dice se il
    planner ha eseguito la query su un solo nodo o l'ha distribuita.
"""
from __future__ import annotations
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data import iter_nodes, iter_edges  # noqa: E402
from metrics import _newman_from_mixing  # noqa: E402


# ============================================================================
# CONNESSIONE
# ============================================================================
NODE_PORTS = {1: 26257, 2: 26258, 3: 26259}


def connect_cockroach(node: int = 1):
    import psycopg2
    crdb_host = os.environ.get("CRDB_HOST", "")
    if crdb_host:
        # Multi-VM: nodo 1 su IP reale, porta standard
        host, port = crdb_host, 26257
    else:
        # Single-host: nodi su localhost con porte diverse
        host, port = "localhost", NODE_PORTS[node]
    return psycopg2.connect(host=host, port=port,
                             dbname="defaultdb", user="root",
                             sslmode="disable")


# ============================================================================
# LOADER
# ============================================================================
class CockroachLoader:
    """Carica users/follows. Lo schema (db/cockroachdb_schema.sql) viene
    eseguito statement-per-statement: l'indice hash-sharded
    'follows_src_hash' e' un artefatto didattico (vedi commenti nello
    schema) e su alcune versioni/edizioni potrebbe richiedere flag
    aggiuntivi — se fallisce, lo saltiamo senza interrompere il load."""

    def __init__(self, conn, schema_path="db/cockroachdb_schema.sql"):
        self.conn = conn
        self.schema_path = schema_path

    def load(self, ds, keep=None, page_size=5000) -> dict:
        from psycopg2.extras import execute_values
        cur = self.conn.cursor()

        # --- schema, statement per statement (best-effort) ------------------
        with open(self.schema_path) as f:
            ddl = f.read()
        for stmt in (s.strip() for s in ddl.split(";")):
            if not stmt:
                continue
            try:
                cur.execute(stmt)
                self.conn.commit()
            except Exception as ex:
                self.conn.rollback()
                print(f"[cockroach] DDL saltata ({ex}): {stmt.splitlines()[0][:60]}...")

        # --- LOAD nodi (batch INSERT, execute_values) ------------------------
        t0 = time.perf_counter()
        rows = [(n["id"], n["views"], n["mature"], n["life_time"],
                 n["dead_account"], n["language"], n["affiliate"])
                for n in iter_nodes(ds, keep)]
        execute_values(
            cur,
            "INSERT INTO users (id,views,mature,life_time,dead_account,language,affiliate) "
            "VALUES %s ON CONFLICT (id) DO NOTHING",
            rows, page_size=page_size)
        self.conn.commit()

        # --- LOAD archi DUPLICATI (a,b)+(b,a) — commit ogni page_size righe ---
        # NON accumulare tutto in una lista e fare un'unica transazione: su
        # dataset grandi la transazione rimane aperta per minuti/ore e
        # CockroachDB la abortisce con "batch timestamp before GC threshold"
        # (il timer GC del cluster avanza oltre il timestamp di inizio tx).
        # La soluzione e' fare TANTE transazioni brevi (1 per pagina).
        rows_edges = 0
        buf = []
        for a, b in iter_edges(ds, keep):
            buf.append((a, b))
            buf.append((b, a))
            if len(buf) >= page_size:
                execute_values(
                    cur,
                    "INSERT INTO follows (source_id, target_id) VALUES %s "
                    "ON CONFLICT (source_id, target_id) DO NOTHING",
                    buf, page_size=page_size)
                self.conn.commit()
                rows_edges += len(buf)
                buf = []
        if buf:
            execute_values(
                cur,
                "INSERT INTO follows (source_id, target_id) VALUES %s "
                "ON CONFLICT (source_id, target_id) DO NOTHING",
                buf, page_size=page_size)
            self.conn.commit()
            rows_edges += len(buf)
        load_sec = time.perf_counter() - t0

        return {"load_sec": load_sec, "index_sec": 0.0,
                "rows_nodes": len(rows), "rows_edges": rows_edges}


# ============================================================================
# METRICHE
# ============================================================================
class CockroachMetrics:
    """SQL quasi identico a PostgresMetrics. Le differenze sono commentate
    dove rilevanti (deviazione standard portabile, EXPLAIN distribution)."""

    def __init__(self, conn):
        self.conn = conn

    def _q(self, sql, params=None, one=False):
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone() if one else cur.fetchall()

    # --- 1. GRADO (globale) --------------------------------------------------
    def degree(self):
        """
        COMPLESSITA': O(E). GROUP BY source_id su tutta la tabella follows.
        ARCHITETTURA: a differenza di Postgres, questa scan puo' coinvolgere
          range su nodi diversi: il nodo coordinatore raccoglie i risultati
          parziali del GROUP BY da ciascun range e li combina (DistSQL).
        """
        row = self._q("""
            WITH deg AS (
                SELECT source_id AS id, count(*) AS d FROM follows GROUP BY source_id
            )
            SELECT avg(d)::float8, avg((d*d)::float8), max(d)::float8 FROM deg
        """, one=True)
        avg_d, avg_d2, max_d = row
        std_d = (avg_d2 - avg_d ** 2) ** 0.5 if avg_d2 is not None else None
        return {"avg_deg": avg_d, "std_deg": std_d, "max_deg": max_d}

    # --- 1a. GRADO locale (1 nodo, query a chiave nota) -----------------------
    def degree_local(self, node_id):
        """Query LOCALE: 'quanti segue X' = count(*) WHERE source_id=X.
        COMPLESSITA': O(grado(X)). In CockroachDB questa query in genere
        tocca UN SOLO range (quindi 1-3 nodi per la replica Raft, ma il
        client parla con 1 solo coordinatore): EXPLAIN la marca come
        'distribution: local'."""
        t0 = time.perf_counter()
        row = self._q("SELECT count(*) FROM follows WHERE source_id=%s", (node_id,), one=True)
        dt = time.perf_counter() - t0
        return {"node_id": node_id, "degree": row[0], "time_sec": dt}

    # --- 1b. GRADO globale, con timing --------------------------------------
    def degree_global(self):
        """Stesso calcolo di degree(), con tempo di esecuzione. Su una
        tabella che attraversa piu' range/nodi, EXPLAIN la marca come
        'distribution: full'."""
        t0 = time.perf_counter()
        out = self.degree()
        out["time_sec"] = time.perf_counter() - t0
        return out

    # --- helper: distribuzione del piano (local | full) ----------------------
    def explain_distribution(self, sql, params=None):
        """Esegue EXPLAIN e ritorna la riga con 'distribution: local|full'.
        E' l'evidenza concreta — da incollare in relazione — di QUANDO
        CockroachDB esegue una query su un solo nodo e quando la distribuisce
        su piu' nodi/range."""
        with self.conn.cursor() as cur:
            cur.execute(f"EXPLAIN {sql}", params or ())
            rows = cur.fetchall()
        for r in rows:
            line = str(r[0])
            if "distribution:" in line:
                return line.strip()
        return None

    # --- 2. CLUSTERING (transitivita') ---------------------------------------
    def clustering_global(self):
        """
        COMPLESSITA': O(sum_v deg(v)^2), come in Postgres (self-join a 3 via
        su follows). ATTENZIONE: su un campione grande questa query e' la
        piu' pesante e in un cluster a 3 nodi soffre del coordinamento
        cross-range — eseguila solo su campioni piccoli (poche migliaia di
        nodi), coerentemente con run_distributed_benchmark.py.
        """
        row = self._q("""
            WITH und AS (
                SELECT source_id AS a, target_id AS b FROM follows WHERE source_id < target_id
            ),
            tri AS (
                SELECT count(*) AS t
                FROM und e1
                JOIN und e2 ON e1.b = e2.a
                JOIN und e3 ON e3.a = e1.a AND e3.b = e2.b
            ),
            triplets AS (
                SELECT sum(d*(d-1)/2) AS p
                FROM (SELECT source_id, count(*) d FROM follows GROUP BY source_id) g
            )
            SELECT (3.0 * tri.t) / NULLIF(triplets.p, 0), tri.t
            FROM tri, triplets
        """, one=True)
        return {"transitivity": row[0], "triangles": row[1]}

    # --- 4. PAGERANK (iterativo, come in Postgres) ----------------------------
    def pagerank(self, iterations=10, damping=0.85):
        """
        COMPLESSITA': O(iter * E). Stessa logica di PostgresMetrics.pagerank:
          ogni iterazione e' un hash-join + GROUP BY materializzato in una
          tabella temporanea. In CockroachDB ogni iterazione e' una
          transazione distribuita: il costo per iterazione include il
          coordinamento Raft per i range coinvolti, non solo l'I/O locale.
        """
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pr")
            cur.execute("""
                CREATE TABLE pr AS
                SELECT id, 1.0 / (SELECT count(*) FROM users)::float8 AS rank
                FROM users
            """)
            cur.execute("DROP TABLE IF EXISTS outdeg")
            cur.execute("""
                CREATE TABLE outdeg AS
                SELECT source_id AS id, count(*) AS d FROM follows GROUP BY source_id
            """)
            self.conn.commit()
            n = self._q("SELECT count(*) FROM users", one=True)[0]
            for _ in range(iterations):
                cur.execute("DROP TABLE IF EXISTS pr_new")
                cur.execute("""
                    CREATE TABLE pr_new AS
                    SELECT u.id,
                           (1 - %s) / %s + %s * COALESCE(sum(pr.rank / od.d), 0) AS rank
                    FROM users u
                    LEFT JOIN follows f ON f.target_id = u.id
                    LEFT JOIN pr ON pr.id = f.source_id
                    LEFT JOIN outdeg od ON od.id = f.source_id
                    GROUP BY u.id
                """, (damping, n, damping))
                cur.execute("DROP TABLE pr")
                cur.execute("ALTER TABLE pr_new RENAME TO pr")
                self.conn.commit()
            result = self._q("SELECT id, rank FROM pr ORDER BY rank DESC LIMIT 10")
            cur.execute("DROP TABLE IF EXISTS pr")
            cur.execute("DROP TABLE IF EXISTS outdeg")
            self.conn.commit()
            return result

    # --- 5. ASSORTATIVITA' -----------------------------------------------------
    def assortativity(self):
        """Identica a PostgresMetrics.assortativity: join follows<->users x2
        + GROUP BY lingua, coefficiente di Newman calcolato in Python."""
        rows = self._q("""
            SELECT ua.language, ub.language, count(*)
            FROM follows f
            JOIN users ua ON ua.id = f.source_id
            JOIN users ub ON ub.id = f.target_id
            GROUP BY ua.language, ub.language
        """)
        return _newman_from_mixing([(r[0], r[1], r[2]) for r in rows])
