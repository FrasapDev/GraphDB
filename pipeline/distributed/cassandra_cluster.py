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
import os
import sys
import time

# pipeline/ contiene data.py e metrics.py: li rendiamo importabili anche
# quando questo script e' lanciato da pipeline/distributed/.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data import iter_nodes, iter_edges  # noqa: E402
from metrics import CassandraMetrics  # noqa: E402


# ============================================================================
# CONNESSIONE — gossip + traduzione IP per l'accesso dall'host
# ============================================================================
# COME FUNZIONA LA SCOPERTA DEI NODI (gossip):
# il driver si connette al "contact point" (cass-1, porta host 9042), legge
# system.local/system.peers e scopre gli IP INTERNI degli altri nodi
# (172.28.1.12, .13 sul bridge "distnet"). Da QUESTI IP il driver apre poi
# connessioni dirette per il connection pooling e il routing delle query
# verso il nodo "owner" della partizione (token-aware routing).
#
# PROBLEMA: dall'host (fuori dal bridge Docker) 172.28.1.12:9042 non e'
# raggiungibile — solo 127.0.0.1:9043 lo e' (porta pubblicata di cass-2).
# SOLUZIONE: un EndPointFactory custom intercetta la creazione degli
# endpoint per i peer scoperti e li traduce IP-fisso -> 127.0.0.1:porta-host,
# usando la mappa statica definita in docker-compose.distributed.yml.
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
    """Connessione al cluster a 3 nodi dall'host. Il contact_point iniziale
    e' sempre cass-1 (porta 9042); l'EndPointFactory si occupa di cass-2/3."""
    from cassandra.cluster import Cluster
    cluster = Cluster([contact_point], port=port,
                       endpoint_factory=DockerEndPointFactory())
    return cluster.connect()


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
        write_cl = write_cl if write_cl is not None else ConsistencyLevel.QUORUM

        # --- schema: stesso parsing "rimuovi // fino a fine riga" usato dal
        # loader centralizzato (pipeline/loaders.py), necessario perche' i
        # commenti // restano attaccati agli statement dopo lo split su ';'.
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
        execute_concurrent_with_args(s, ins_ch, rows, concurrency=50)
        execute_concurrent_with_args(s, ins_lang, [(n[5], n[0]) for n in rows], concurrency=50)

        fol = []
        for a, b in iter_edges(ds, keep):
            fol.append((a, b))
            fol.append((b, a))
        execute_concurrent_with_args(s, ins_fol, fol, concurrency=100)
        load_sec = time.perf_counter() - t0

        return {
            "load_sec": load_sec,
            "index_sec": 0.0,
            "write_consistency": ConsistencyLevel.value_to_name[write_cl],
            "rows_nodes": len(rows),
            "rows_edges": len(fol),
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
