#!/usr/bin/env python3
"""
run_benchmark.py — Pipeline unica del benchmark Twitch Gamers Network.

Esegue:
  1. download + decompressione dataset (SNAP)
  2. avvio container (lo fai tu con docker compose up; qui solo le connessioni)
  3. load nei tre DB con misura tempi (metrica 6)
  4. esecuzione delle 6 metriche: 1 warmup + 2 misure cronometrate
  5. salvataggio results.json
  6. generazione results.md con tabelle e istogramma testuale

USO:
  # avvia prima i container:
  docker compose -f docker/docker-compose.yml up -d

  # poi:
  python pipeline/run_benchmark.py --data ./data
  python pipeline/run_benchmark.py --data ./data --sample 20000   # sottocampione
  python pipeline/run_benchmark.py --data ./data --only postgres,neo4j
  python pipeline/run_benchmark.py --data ./data --reference-only  # solo NetworkX

Dipendenze:
  pip install psycopg2-binary neo4j cassandra-driver networkx requests
"""
from __future__ import annotations
import argparse
import json
import statistics
import sys
import time
from data import Dataset, download_and_extract, select_sample, iter_edges, iter_nodes


def timed(fn, *a, **k):
    t0 = time.perf_counter()
    out = fn(*a, **k)
    return out, time.perf_counter() - t0


def measure(fn, *a, warmup=1, runs=2, **k):
    """1 warmup (scartato) + N misure. Ritorna (risultato, [tempi])."""
    for _ in range(warmup):
        fn(*a, **k)
    times, res = [], None
    for _ in range(runs):
        res, dt = timed(fn, *a, **k)
        times.append(dt)
    return res, times


# ---------------------------------------------------------------------------
#  Connessioni (best-effort: ogni DB e' opzionale)
# ---------------------------------------------------------------------------
def connect_postgres():
    import psycopg2
    return psycopg2.connect(host="localhost", port=5432, dbname="twitch",
                            user="bench", password="bench")


def connect_neo4j():
    from neo4j import GraphDatabase
    return GraphDatabase.driver("bolt://localhost:7687",
                                auth=("neo4j", "benchpass"))


def connect_cassandra():
    from cassandra.cluster import Cluster
    cluster = Cluster(["127.0.0.1"], port=9042)
    return cluster.connect()


# ---------------------------------------------------------------------------
#  Riferimento NetworkX (verita' di terreno, nessun DB richiesto)
# ---------------------------------------------------------------------------
def run_reference(ds: Dataset, keep, sample_btw=1000):
    import networkx as nx
    print("[ref] costruisco il grafo NetworkX ...")
    G = nx.Graph()
    G.add_nodes_from(n["id"] for n in iter_nodes(ds, keep))
    G.add_edges_from(iter_edges(ds, keep))
    lang = {n["id"]: n["language"] for n in iter_nodes(ds, keep)}
    nx.set_node_attributes(G, lang, "language")
    out = {}
    degs = [d for _, d in G.degree()]
    out["degree"], dt = {"avg_deg": statistics.mean(degs),
                         "std_deg": statistics.pstdev(degs),
                         "max_deg": max(degs)}, 0
    _, t = measure(lambda: [d for _, d in G.degree()])
    out["_t_degree"] = t
    cc, t = measure(nx.transitivity, G)
    out["clustering"] = {"transitivity": cc}; out["_t_clustering"] = t
    pr, t = measure(nx.pagerank, G, alpha=0.85, max_iter=10)
    out["pagerank_top"] = sorted(pr.items(), key=lambda x: -x[1])[:5]
    out["_t_pagerank"] = t
    asr, t = measure(nx.attribute_assortativity_coefficient, G, "language")
    out["assortativity"] = asr; out["_t_assortativity"] = t
    return out


# ---------------------------------------------------------------------------
#  Esecuzione metriche su un DB
# ---------------------------------------------------------------------------
def run_db(name, metrics_obj, prep=None):
    """Esegue le metriche disponibili su un oggetto *Metrics e ritorna i tempi."""
    res = {}
    if prep:
        prep()  # es. Neo4j.project()

    def safe(label, fn, *a, **k):
        try:
            out, times = measure(fn, *a, **k)
            res[label] = {"result": _jsonable(out),
                          "times_sec": times,
                          "median_sec": statistics.median(times)}
            print(f"  [{name}] {label}: {statistics.median(times):.3f}s (mediana)")
        except Exception as ex:
            res[label] = {"error": str(ex)}
            print(f"  [{name}] {label}: ERRORE {ex}")

    safe("degree", metrics_obj.degree)
    if hasattr(metrics_obj, "clustering"):
        safe("clustering", metrics_obj.clustering)
    elif hasattr(metrics_obj, "clustering_global"):
        safe("clustering", metrics_obj.clustering_global)
    if hasattr(metrics_obj, "betweenness"):
        safe("betweenness", metrics_obj.betweenness)
    safe("pagerank", metrics_obj.pagerank)
    safe("assortativity", metrics_obj.assortativity)
    return res


def _jsonable(o):
    if isinstance(o, dict):
        return {str(k): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(x) for x in o]
    if isinstance(o, float):
        return round(o, 6)
    return o


# ---------------------------------------------------------------------------
#  Report
# ---------------------------------------------------------------------------
def write_report(results, path="results.md"):
    from report import build_report
    build_report(results, path)
    print(f"[report] scritto {path}")


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data")
    ap.add_argument("--sample", type=int, default=None,
                    help="sottocampiona ai primi N nodi (consigliato su 8-16GB)")
    ap.add_argument("--only", default="postgres,neo4j")
    ap.add_argument("--reference-only", action="store_true")
    ap.add_argument("--skip-load", action="store_true",
                    help="i DB sono gia' caricati, esegui solo le metriche")
    args = ap.parse_args()

    download_and_extract(args.data)
    ds = Dataset(args.data, sample_nodes=args.sample)
    keep = select_sample(ds)
    targets = set(args.only.split(","))

    results = {"meta": {"sample": args.sample, "ts": time.time()}, "load": {}, "metrics": {}}

    # Riferimento NetworkX
    try:
        results["metrics"]["reference"] = run_reference(ds, keep)
    except Exception as ex:
        print(f"[ref] saltato: {ex}")

    if args.reference_only:
        _dump(results); return

    from loaders import PostgresLoader, Neo4jLoader, CassandraLoader
    from metrics import PostgresMetrics, Neo4jMetrics, CassandraMetrics

    # PostgreSQL
    if "postgres" in targets:
        try:
            conn = connect_postgres()
            if not args.skip_load:
                print("[postgres] load ...")
                results["load"]["postgres"] = PostgresLoader(conn).load(ds, keep)
            results["metrics"]["postgres"] = run_db("postgres", PostgresMetrics(conn))
        except Exception as ex:
            print(f"[postgres] non disponibile: {ex}")

    # Neo4j
    if "neo4j" in targets:
        try:
            drv = connect_neo4j()
            if not args.skip_load:
                print("[neo4j] load ...")
                results["load"]["neo4j"] = Neo4jLoader(drv).load(ds, keep)
            nm = Neo4jMetrics(drv)
            results["metrics"]["neo4j"] = run_db("neo4j", nm, prep=nm.project)
        except Exception as ex:
            print(f"[neo4j] non disponibile: {ex}")

    # Cassandra
    if "cassandra" in targets:
        try:
            sess = connect_cassandra()
            if not args.skip_load:
                print("[cassandra] load ...")
                results["load"]["cassandra"] = CassandraLoader(sess).load(ds, keep)
            sess.set_keyspace("twitch")
            results["metrics"]["cassandra"] = run_db("cassandra", CassandraMetrics(sess))
        except Exception as ex:
            print(f"[cassandra] non disponibile: {ex}")

    _dump(results)


def _dump(results):
    with open("results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("[ok] results.json scritto")
    try:
        write_report(results)
    except Exception as ex:
        print(f"[report] errore: {ex}")


if __name__ == "__main__":
    main()
