#!/usr/bin/env python3
"""
run_distributed_benchmark.py — Orchestratore del confronto "ambito
distribuito": Cassandra cluster (3 nodi, RF=3, AP) vs CockroachDB
(3 nodi, RF=3, CP/Raft).

USO:
  # 1. ferma il benchmark centralizzato (condivide la RAM con questo)
  docker compose -f docker/docker-compose.yml down

  # 2. avvia il cluster distribuito
  docker compose -f docker/docker-compose.distributed.yml up -d

  # 3. esegui (campione piccolo: 3+3 container piccoli, non e' il dataset
  #    intero del benchmark centralizzato)
  python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000

  # 4. risultati
  cat results_distributed.md
  cat results_distributed.json

Il confronto a 4 colonne nel report unisce questi risultati con
results.json (postgres/neo4j del benchmark centralizzato) — campioni
diversi, quindi il confronto e' SOLO sui TEMPI RELATIVI e sui MECCANISMI,
non sui valori assoluti (vedi docs/07_distribuito.md).
"""
from __future__ import annotations
import argparse
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data import Dataset, download_and_extract, select_sample  # noqa: E402
from run_benchmark import measure, _jsonable  # noqa: E402
from cassandra_cluster import connect_cluster, CassandraClusterLoader, CassandraClusterMetrics  # noqa: E402
from cockroach import connect_cockroach, CockroachLoader, CockroachMetrics  # noqa: E402


# ============================================================================
# CASSANDRA CLUSTER
# ============================================================================
def run_cassandra(ds, keep, args, results):
    from cassandra import ConsistencyLevel

    sess = connect_cluster()
    if not args.skip_load:
        print("[cassandra-cluster] load (write CL=QUORUM) ...")
        results["load"]["cassandra_cluster"] = CassandraClusterLoader(sess).load(
            ds, keep, write_cl=ConsistencyLevel.QUORUM)
    sess.set_keyspace("twitch_dist")
    cm = CassandraClusterMetrics(sess)

    m = {}
    levels = {"ONE": ConsistencyLevel.ONE, "QUORUM": ConsistencyLevel.QUORUM, "ALL": ConsistencyLevel.ALL}

    # --- grado GLOBALE (full scan), un giro per CL --------------------------
    m["degree_global"] = {}
    for name, cl in levels.items():
        r = cm.degree_global(cl)
        m["degree_global"][name] = r
        print(f"  [cassandra-cluster] degree_global CL={name}: {r['time_sec']:.3f}s "
              f"(avg_deg={r['avg_deg']:.2f})")

    # --- grado LOCALE (1 partizione), un giro per CL -------------------------
    sample_node = min(keep) if keep else 1
    m["degree_local"] = {}
    for name, cl in levels.items():
        r = cm.degree_local(sample_node, cl)
        m["degree_local"][name] = r
        print(f"  [cassandra-cluster] degree_local(id={sample_node}) CL={name}: "
              f"{r['time_sec']*1000:.2f}ms (degree={r['degree']})")

    # --- pagerank/assortativity: riusano CassandraMetrics (full scan) -------
    try:
        out, times = measure(cm.pagerank)
        m["pagerank"] = {"result": _jsonable(out), "median_sec": statistics.median(times)}
    except Exception as ex:
        m["pagerank"] = {"error": str(ex)}

    try:
        out, times = measure(cm.assortativity)
        m["assortativity"] = {"result": _jsonable(out), "median_sec": statistics.median(times)}
    except Exception as ex:
        m["assortativity"] = {"error": str(ex)}

    results["metrics"]["cassandra_cluster"] = _jsonable(m)


# ============================================================================
# COCKROACHDB
# ============================================================================
def run_cockroach(ds, keep, args, results):
    conn = connect_cockroach(node=1)
    if not args.skip_load:
        print("[cockroach] load ...")
        results["load"]["cockroach"] = CockroachLoader(conn).load(ds, keep)
    cm = CockroachMetrics(conn)
    m = {}

    out, times = measure(cm.degree_global, warmup=1, runs=2)
    m["degree_global"] = {"result": _jsonable(out), "median_sec": statistics.median(times)}
    print(f"  [cockroach] degree_global: {m['degree_global']['median_sec']:.3f}s "
          f"(avg_deg={out['avg_deg']:.2f})")

    sample_node = min(keep) if keep else 1
    r = cm.degree_local(sample_node)
    m["degree_local"] = _jsonable(r)
    print(f"  [cockroach] degree_local(id={sample_node}): {r['time_sec']*1000:.2f}ms "
          f"(degree={r['degree']})")

    # --- EXPLAIN: distribuzione locale vs globale del piano -------------------
    m["explain_local"] = cm.explain_distribution(
        "SELECT count(*) FROM follows WHERE source_id=%s", (sample_node,))
    m["explain_global"] = cm.explain_distribution(
        "SELECT source_id, count(*) FROM follows GROUP BY source_id")
    print(f"  [cockroach] EXPLAIN locale : {m['explain_local']}")
    print(f"  [cockroach] EXPLAIN globale: {m['explain_global']}")

    for label, fn in (("pagerank", cm.pagerank), ("assortativity", cm.assortativity)):
        try:
            out, times = measure(fn)
            m[label] = {"result": _jsonable(out), "median_sec": statistics.median(times)}
        except Exception as ex:
            m[label] = {"error": str(ex)}

    if args.with_clustering:
        try:
            out, times = measure(cm.clustering_global)
            m["clustering"] = {"result": _jsonable(out), "median_sec": statistics.median(times)}
        except Exception as ex:
            m["clustering"] = {"error": str(ex)}
    else:
        print("  [cockroach] clustering saltato (--with-clustering per abilitarlo: "
              "self-join pesante, sconsigliato sopra qualche migliaio di nodi)")

    results["metrics"]["cockroach"] = m


# ============================================================================
# MAIN
# ============================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data")
    ap.add_argument("--sample", type=int, default=5000,
                    help="campione di nodi (default 5000: cluster a 6 container "
                         "piccoli, NON e' pensato per il dataset intero)")
    ap.add_argument("--only", default="cassandra,cockroach")
    ap.add_argument("--skip-load", action="store_true",
                    help="i DB sono gia' caricati, esegui solo le metriche")
    ap.add_argument("--with-clustering", action="store_true",
                    help="esegue anche il clustering coefficient su CockroachDB")
    args = ap.parse_args()

    download_and_extract(args.data)
    ds = Dataset(args.data, sample_nodes=args.sample)
    keep = select_sample(ds)
    targets = set(args.only.split(","))

    # Se results_distributed.json esiste gia' (es. una run precedente con
    # --only cassandra su una macchina con poca RAM), ne riusiamo load/metrics
    # e aggiorniamo solo le sezioni dei sistemi richiesti in questa run: cosi'
    # si possono eseguire i due cluster IN SEQUENZA (uno spento mentre l'altro
    # gira) e ottenere comunque un unico results_distributed.json con
    # entrambi. IMPORTANTE: usare lo stesso --sample in tutte le run, altrimenti
    # 'keep' (il campione di nodi) cambia e i risultati non sono confrontabili.
    out_path = "results_distributed.json"
    if os.path.exists(out_path):
        with open(out_path) as f:
            results = json.load(f)
        results.setdefault("load", {})
        results.setdefault("metrics", {})
        print(f"[merge] {out_path} esistente, aggiorno solo: {sorted(targets)}")
    else:
        results = {"load": {}, "metrics": {}}
    results["meta"] = {"sample": args.sample, "ts": time.time()}

    if "cassandra" in targets:
        try:
            run_cassandra(ds, keep, args, results)
        except Exception as ex:
            print(f"[cassandra-cluster] non disponibile: {ex}")

    if "cockroach" in targets:
        try:
            run_cockroach(ds, keep, args, results)
        except Exception as ex:
            print(f"[cockroach] non disponibile: {ex}")

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"[ok] {out_path} scritto")

    try:
        from report_distributed import build_report
        build_report(results)
        print("[report] scritto results_distributed.md")
    except Exception as ex:
        print(f"[report] errore: {ex}")


if __name__ == "__main__":
    main()
