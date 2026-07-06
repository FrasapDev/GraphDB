"""
cassandra_cluster.py — Loader e metriche per il cluster Cassandra a 3 nodi
(RF=3, keyspace twitch_dist, vedi db/cassandra_cluster_schema.cql e
docker/docker-compose.distributed.yml).

Cosa cambia rispetto al benchmark centralizzato (pipeline/metrics.py,
1 nodo RF=1):
  - la CONNESSIONE deve attraversare il bridge Docker (vedi
    DockerEndPointFactory sotto: spiega come il driver scopre i nodi via
    gossip e perche' serve una traduzione IP->porta per arrivare dall'host).
  - il LIVELLO DI CONSISTENZA (CL) e' configurabile per ogni query: con
    RF=3, CL=ONE/QUORUM/ALL richiedono l'ACK di 1, 2 o 3 repliche.
  - distinguiamo esplicitamente query LOCALE (1 partizione: "i follow di X")
    da query GLOBALE (full token-range scan: "grado medio").

GERARCHIA: CassandraClusterMetrics estende CassandraMetrics (stesso modello
dati, stesse query per pagerank/clustering/assortativity — qui aggiungiamo
solo le due varianti locale/globale del grado con CL parametrico).
"""
from __future__ import annotations
import math
import os
import random
import sys
import time
from collections import defaultdict

# pipeline/ contiene data.py: lo rendiamo importabile anche quando questo
# script e' lanciato da pipeline/distributed/.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data import iter_nodes, iter_edges  # noqa: E402


# ============================================================================
# CASSANDRA METRICS — spostato da pipeline/metrics.py (rimosso dal benchmark
# centralizzato) per continuare a servire il benchmark distribuito.
# ============================================================================
class CassandraMetrics:
    """Full-scan + aggregazione Python lato client. Usata come base da
    CassandraClusterMetrics per pagerank/assortativity (operazioni globali
    che non dipendono dal CL e funzionano identiche su 1 nodo o su 3)."""

    def __init__(self, session):
        self.session = session

    def _all_adjacency(self):
        adj = defaultdict(list)
        rows = self.session.execute(
            "SELECT source_id, target_id FROM follows_by_source")
        for r in rows:
            adj[r.source_id].append(r.target_id)
        return adj

    def degree(self):
        adj = self._all_adjacency()
        degs = [len(v) for v in adj.values()]
        mean = sum(degs) / len(degs)
        var = sum((d - mean) ** 2 for d in degs) / len(degs)
        return {"avg_deg": mean, "std_deg": math.sqrt(var), "max_deg": max(degs)}

    def clustering_global(self, adj=None):
        adj = adj or self._all_adjacency()
        nbr = {k: set(v) for k, v in adj.items()}
        triangles = 0
        triplets = 0
        for _, ns in nbr.items():
            ns = list(ns)
            k = len(ns)
            triplets += k * (k - 1) // 2
            for i in range(k):
                for j in range(i + 1, k):
                    if ns[j] in nbr.get(ns[i], ()):
                        triangles += 1
        triangles //= 3
        transitivity = (3 * triangles / triplets) if triplets else 0.0
        return {"transitivity": transitivity, "triangles": triangles}

    def betweenness(self, sample=500, adj=None):
        adj = adj or self._all_adjacency()
        nbr = {k: list(set(v)) for k, v in adj.items()}
        nodes = list(nbr.keys())
        random.seed(42)
        sources = random.sample(nodes, min(sample, len(nodes)))
        bc = dict.fromkeys(nbr, 0.0)
        from collections import deque
        for s in sources:
            S, P, sigma, d = [], defaultdict(list), defaultdict(float), {}
            sigma[s] = 1.0; d[s] = 0
            Q = deque([s])
            while Q:
                v = Q.popleft(); S.append(v)
                for w in nbr.get(v, ()):
                    if w not in d:
                        d[w] = d[v] + 1; Q.append(w)
                    if d[w] == d[v] + 1:
                        sigma[w] += sigma[v]; P[w].append(v)
            delta = defaultdict(float)
            while S:
                w = S.pop()
                for v in P[w]:
                    delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
                if w != s:
                    bc[w] += delta[w]
        top = sorted(bc.items(), key=lambda x: -x[1])[:10]
        return [{"id": n, "score": v} for n, v in top]

    def pagerank(self, iterations=10, damping=0.85, adj=None):
        adj = adj or self._all_adjacency()
        nodes = list(adj.keys())
        N = len(nodes)
        rank = {n: 1.0 / N for n in nodes}
        outdeg = {n: len(adj[n]) for n in nodes}
        for _ in range(iterations):
            new = {n: (1 - damping) / N for n in nodes}
            for src, tgts in adj.items():
                if not tgts:
                    continue
                share = damping * rank[src] / outdeg[src]
                for t in tgts:
                    if t in new:
                        new[t] += share
            rank = new
        top = sorted(rank.items(), key=lambda x: -x[1])[:10]
        return [{"id": n, "score": v} for n, v in top]

    def assortativity(self, adj=None):
        lang = {}
        for r in self.session.execute("SELECT id, language FROM channels_by_id"):
            lang[r.id] = r.language
        adj = adj or self._all_adjacency()
        mixing = defaultdict(int)
        for s, tgts in adj.items():
            ls = lang.get(s)
            for t in tgts:
                mixing[(ls, lang.get(t))] += 1
        from metrics import _newman_from_mixing
        triples = [(a, b, c) for (a, b), c in mixing.items()]
        return _newman_from_mixing(triples)


# ============================================================================
# CONNESSIONE — gossip + traduzione IP per l'accesso dall'host
# ============================================================================
try:
    from cassandra.connection import DefaultEndPoint, EndPointFactory
except ImportError:  # pragma: no cover - import facoltativo se cassandra-driver assente
    DefaultEndPoint = EndPointFactory = object


class DockerEndPointFactory(EndPointFactory):
    """Traduce gli IP statici del bridge 'distnet' nelle porte pubblicate
    sull'host (vedi docker-compose.distributed.yml)."""

    IP_TO_HOST_PORT = {
        "172.28.1.11": 9042,  # cass-1
        "172.28.1.12": 9043,  # cass-2
        "172.28.1.13": 9044,  # cass-3
    }

    def configure(self, cluster):
        self._cluster = cluster
        return self

    def create(self, row):
        addr = str(row.get("rpc_address") or row.get("native_transport_address")
                    or row.get("peer") or row.get("broadcast_address") or "")
        port = self.IP_TO_HOST_PORT.get(addr, 9042)
        return DefaultEndPoint("127.0.0.1", port)


def connect_cluster(contact_point="127.0.0.1", port=9042):
    """Connessione al cluster Cassandra.

    Single-host (docker-compose.distributed.yml su un solo host):
      CASS_CONTACT_POINTS non impostato → usa 127.0.0.1 + DockerEndPointFactory
      che traduce gli IP del bridge Docker (172.28.1.x) in porte host.

    Multi-VM (ogni nodo su una VM separata, IP reali raggiungibili):
      export CASS_CONTACT_POINTS=10.0.1.6,10.0.1.5,10.0.1.8
      → connessione diretta agli IP reali, nessuna traduzione necessaria.
    """
    from cassandra.cluster import Cluster
    env_seeds = os.environ.get("CASS_CONTACT_POINTS", "")
    if env_seeds:
        # Multi-VM: gli IP reali del VNet sono direttamente raggiungibili.
        contact_points = [h.strip() for h in env_seeds.split(",") if h.strip()]
        cluster = Cluster(contact_points, port=port,
                          connect_timeout=30)
        session = cluster.connect()
        session.default_timeout = 120
    else:
        # Single-host: traduzione IP bridge Docker → porte host
        cluster = Cluster([contact_point], port=port,
                           endpoint_factory=DockerEndPointFactory())
        session = cluster.connect()
    return session


# ============================================================================
# LOADER
# ============================================================================
class CassandraClusterLoader:
    """Carica il grafo nel cluster a 3 nodi. Il livello di consistenza di
    SCRITTURA e' parametrico: con RF=3,
      - CL=ONE   -> il coordinatore aspetta l'ACK di 1 replica (le altre 2
                     vengono scritte in background / hinted handoff se down)
      - CL=QUORUM-> aspetta 2/3 repliche (compromesso AP/CP tipico)
      - CL=ALL   -> aspetta tutte le 3 repliche (massima sicurezza, minima
                     disponibilita': basta 1 nodo down per bloccare le scritture)
    Il tempo di load misura quindi anche il COSTO DEL CL scelto, non solo
    il volume di dati.
    """

    def __init__(self, session, schema_path="db/cassandra_cluster_schema.cql"):
        self.session = session
        self.schema_path = schema_path

    def load(self, ds, keep=None, write_cl=None) -> dict:
        from cassandra import ConsistencyLevel
        from cassandra.concurrent import execute_concurrent_with_args
        s = self.session
        if write_cl is None:
            # CASS_LOAD_CL=ONE conviene per il dataset completo (168k/13.6M):
            # CL=ONE non aspetta l'ACK delle 2 repliche remote, dimezza i
            # tempi di load. Le metriche (consistency_demo) usano QUORUM/ALL
            # indipendentemente da questa variabile.
            env_cl = os.environ.get("CASS_LOAD_CL", "").upper()
            write_cl = {"ONE": ConsistencyLevel.ONE,
                        "ALL": ConsistencyLevel.ALL}.get(env_cl, ConsistencyLevel.QUORUM)
            
        with open(self.schema_path) as f:
            raw = f.read()
        cleaned_lines = []
        for line in raw.splitlines():
            idx = line.find("//")
            cleaned_lines.append(line[:idx] if idx != -1 else line)
        cleaned = "\n".join(cleaned_lines)
        for stmt in cleaned.split(";"):
            stmt = stmt.strip()
            if stmt:
                s.execute(stmt)
        s.set_keyspace("twitch_dist")

        ins_ch = s.prepare(
            "INSERT INTO channels_by_id "
            "(id,views,mature,life_time,dead_account,language,affiliate) "
            "VALUES (?,?,?,?,?,?,?)")
        ins_lang = s.prepare("INSERT INTO channels_by_language (language,id) VALUES (?,?)")
        ins_fol = s.prepare("INSERT INTO follows_by_source (source_id,target_id) VALUES (?,?)")
        for stmt in (ins_ch, ins_lang, ins_fol):
            stmt.consistency_level = write_cl

        t0 = time.perf_counter()
        rows = [(n["id"], n["views"], n["mature"], n["life_time"],
                 n["dead_account"], n["language"], n["affiliate"])
                for n in iter_nodes(ds, keep)]
        execute_concurrent_with_args(s, ins_ch, rows, concurrency=10)
        execute_concurrent_with_args(s, ins_lang, [(n[5], n[0]) for n in rows], concurrency=10)

        # Batch da 10k: evita di tenere in RAM tutti i 13.6M archi in una volta
        # (OOM killer su VM con 16GB). Stesso pattern di CockroachLoader.load().
        rows_edges = 0
        buf = []
        for a, b in iter_edges(ds, keep):
            buf.append((a, b))
            buf.append((b, a))
            if len(buf) >= 10000:
                execute_concurrent_with_args(s, ins_fol, buf, concurrency=20)
                rows_edges += len(buf)
                buf = []
        if buf:
            execute_concurrent_with_args(s, ins_fol, buf, concurrency=20)
            rows_edges += len(buf)
        load_sec = time.perf_counter() - t0

        return {
            "load_sec": load_sec,
            "index_sec": 0.0,
            "write_consistency": ConsistencyLevel.value_to_name[write_cl],
            "rows_nodes": len(rows),
            "rows_edges": rows_edges,
        }


# ============================================================================
# METRICHE — degree locale vs globale, a CL parametrico
# ============================================================================
class CassandraClusterMetrics(CassandraMetrics):
    """Estende CassandraMetrics (stesse query di pagerank/clustering/
    assortativity, che operano sul full-scan e quindi sono GIA' 'globali')
    con le due varianti del grado che servono a mostrare la differenza
    locale/globale e l'effetto del CL."""

    # --- query LOCALE: 1 partizione -----------------------------------------
    def degree_local(self, node_id, consistency_level):
        """'Quanti segue il canale node_id' = lettura di UNA partizione.
        COMPLESSITA': O(1) partizioni. Con RF=3 il coordinatore contatta CL
        repliche di QUELLA partizione (1, 2 o 3) — il costo NON dipende dalla
        dimensione del cluster, solo dal CL scelto e dalla latenza di rete
        verso quelle repliche."""
        from cassandra.query import SimpleStatement
        from cassandra import ConsistencyLevel
        stmt = SimpleStatement(
            "SELECT target_id FROM follows_by_source WHERE source_id=%s",
            consistency_level=consistency_level)
        t0 = time.perf_counter()
        rows = list(self.session.execute(stmt, (node_id,)))
        dt = time.perf_counter() - t0
        return {
            "node_id": node_id,
            "degree": len(rows),
            "time_sec": dt,
            "consistency_level": ConsistencyLevel.value_to_name[consistency_level],
        }

    # --- query GLOBALE: full token-range scan -------------------------------
    def degree_global(self, consistency_level):
        """Grado medio = full scan di TUTTE le partizioni (vedi
        CassandraMetrics.degree / _all_adjacency). Qui il CL si applica a
        OGNI pagina della scan: con CL=ALL, ogni pagina aspetta l'ACK di
        tutte e 3 le repliche -> il costo del CL si MOLTIPLICA per il
        numero di pagine, a differenza della query locale."""
        from cassandra import ConsistencyLevel
        old_cl = self.session.default_consistency_level
        self.session.default_consistency_level = consistency_level
        try:
            t0 = time.perf_counter()
            out = self.degree()
            dt = time.perf_counter() - t0
        finally:
            self.session.default_consistency_level = old_cl
        out["time_sec"] = dt
        out["consistency_level"] = ConsistencyLevel.value_to_name[consistency_level]
        return out
