"""
loaders.py — Caricamento del dataset in ciascun DB, con misurazione separata
di (a) tempo di import dati e (b) tempo di costruzione indici (metrica 6).

Ogni loader espone load() -> dict con 'load_sec' e 'index_sec'.
DB supportati: PostgreSQL, Neo4j.
"""
from __future__ import annotations
import time
from data import Dataset, iter_nodes, iter_edges


# ============================================================================
#  POSTGRESQL — COPY (la via piu' veloce per il bulk load)
# ============================================================================
class PostgresLoader:
    def __init__(self, conn, schema_path="db/postgres_schema.sql"):
        self.conn = conn
        self.schema_path = schema_path

    def load(self, ds: Dataset, keep=None) -> dict:
        cur = self.conn.cursor()
        # Schema SENZA indici sugli archi: li creiamo dopo per misurare a parte
        with open(self.schema_path) as f:
            ddl = f.read()
        # Eseguiamo il DDL ma rimandiamo gli indici edges (li togliamo qui e
        # li ricreiamo nella fase index per cronometrarli isolati).
        ddl_no_edge_idx = "\n".join(
            l for l in ddl.splitlines()
            if not l.strip().startswith("CREATE INDEX idx_edges"))
        cur.execute(ddl_no_edge_idx)
        self.conn.commit()

        # ---- LOAD nodi via COPY (stream) ----
        import io
        t0 = time.perf_counter()
        buf = io.StringIO()
        for n in iter_nodes(ds, keep):
            buf.write(f"{n['id']}\t{n['views']}\t{n['mature']}\t{n['life_time']}"
                      f"\t{n['dead_account']}\t{n['language']}\t{n['affiliate']}\n")
        buf.seek(0)
        cur.copy_expert(
            "COPY nodes (id,views,mature,life_time,dead_account,language,affiliate) "
            "FROM STDIN WITH (FORMAT text)", buf)

        # ---- LOAD archi DUPLICATI via COPY ----
        buf = io.StringIO()
        for a, b in iter_edges(ds, keep):
            buf.write(f"{a}\t{b}\n{b}\t{a}\n")   # duplicazione bidirezionale
        buf.seek(0)
        cur.copy_expert("COPY edges (src,dst) FROM STDIN WITH (FORMAT text)", buf)
        self.conn.commit()
        load_sec = time.perf_counter() - t0

        # ---- INDICI + ANALYZE (metrica 6, fase indicizzazione) ----
        t0 = time.perf_counter()
        cur.execute("CREATE INDEX idx_edges_src ON edges (src);")
        cur.execute("CREATE INDEX idx_edges_dst ON edges (dst);")
        cur.execute("CREATE INDEX idx_edges_src_dst ON edges (src, dst);")
        cur.execute("ANALYZE nodes;")
        cur.execute("ANALYZE edges;")
        self.conn.commit()
        index_sec = time.perf_counter() - t0
        return {"load_sec": load_sec, "index_sec": index_sec}


# ============================================================================
#  NEO4J — driver a batch (UNWIND)
# ============================================================================
class Neo4jLoader:
    def __init__(self, driver, schema_path="db/neo4j_schema.cypher"):
        self.driver = driver
        self.schema_path = schema_path

    def load(self, ds: Dataset, keep=None, batch=20000) -> dict:
        with self.driver.session() as s:
            s.run(
                "MATCH (n) "
                "CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"
            )
            # constraint+indici PRIMA del load: l'indice su id serve a far
            # combaciare gli estremi degli archi in tempo O(log V) durante il
            # MERGE. Li cronometriamo come 'index_sec' anche se creati prima,
            # perche' il costo concettuale e' la disponibilita' dell'indice.
            t0 = time.perf_counter()
            s.run("CREATE CONSTRAINT channel_id_unique IF NOT EXISTS "
                  "FOR (c:Channel) REQUIRE c.id IS UNIQUE")
            s.run("CREATE INDEX channel_language IF NOT EXISTS "
                  "FOR (c:Channel) ON (c.language)")
            index_sec = time.perf_counter() - t0

            t0 = time.perf_counter()
            # nodi
            chunk = []
            for n in iter_nodes(ds, keep):
                chunk.append(n)
                if len(chunk) >= batch:
                    s.run("UNWIND $rows AS r CREATE (c:Channel) SET c = r",
                          rows=chunk); chunk = []
            if chunk:
                s.run("UNWIND $rows AS r CREATE (c:Channel) SET c = r", rows=chunk)
            # archi (una relazione per coppia non diretta)
            chunk = []
            for a, b in iter_edges(ds, keep):
                chunk.append({"a": a, "b": b})
                if len(chunk) >= batch:
                    s.run("""UNWIND $rows AS r
                             MATCH (x:Channel {id:r.a}) MATCH (y:Channel {id:r.b})
                             CREATE (x)-[:FOLLOWS]->(y)""", rows=chunk); chunk = []
            if chunk:
                s.run("""UNWIND $rows AS r
                         MATCH (x:Channel {id:r.a}) MATCH (y:Channel {id:r.b})
                         CREATE (x)-[:FOLLOWS]->(y)""", rows=chunk)
            load_sec = time.perf_counter() - t0
        return {"load_sec": load_sec, "index_sec": index_sec}
