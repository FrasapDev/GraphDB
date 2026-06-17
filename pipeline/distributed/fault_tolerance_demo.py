#!/usr/bin/env python3
"""
fault_tolerance_demo.py — "Kill di un nodo": cosa succede a Cassandra (AP)
e a CockroachDB (CP) quando un nodo del cluster a 3 va giu', a parita' di
replication factor 3.

CASSANDRA (RF=3 su 3 nodi -> ogni nodo ha TUTTI i dati):
  - 3/3 nodi su: CL=ONE/QUORUM/ALL funzionano tutti.
  - 2/3 nodi su (1 spento): CL=ONE e QUORUM (2/3) funzionano ancora,
    CL=ALL (3/3) FALLISCE — e' il trade-off "scegli tu quanta disponibilita'
    sacrificare per quanta consistenza".
  - 1/3 nodi su: solo CL=ONE funziona (l'unico nodo vivo ha comunque una
    replica, perche' RF=3=numero di nodi).

COCKROACHDB (RF=3 di default sulle tabelle utente, Raft):
  - 3/3 nodi: tutto ok.
  - 2/3 nodi: i range con leader sul nodo spento ri-eleggono un nuovo
    leader Raft tra i 2 superstiti (hanno comunque il quorum 2/3) -> le
    query continuano a funzionare, con un picco di latenza per la
    ri-elezione.
  - 1/3 nodi: NESSUN range ha piu' quorum (serve 2/3) -> il cluster e'
    DI FATTO INDISPONIBILE per letture/scritture forti. Le query vanno in
    timeout: e' il prezzo della consistenza forte (CP) sotto partizione.

ATTENZIONE: questo script esegue `docker stop` / `docker start` sui
container del cluster DISTRIBUITO (docker-compose.distributed.yml). Non
toccare il benchmark centralizzato (docker-compose.yml) — i nomi dei
container sono diversi (tbd-* vs tb-*).
"""
from __future__ import annotations
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cassandra_cluster import connect_cluster  # noqa: E402
from cockroach import connect_cockroach  # noqa: E402


def docker_stop(name):
    print(f"  $ docker stop {name}")
    subprocess.run(["docker", "stop", name], check=True, capture_output=True)


def docker_start(name):
    print(f"  $ docker start {name}")
    subprocess.run(["docker", "start", name], check=True, capture_output=True)


# ============================================================================
# CASSANDRA
# ============================================================================
def cassandra_fault_demo(settle_sec=8):
    from cassandra.query import SimpleStatement
    from cassandra import ConsistencyLevel

    out = {}
    sess = connect_cluster()
    sess.execute("""
        CREATE KEYSPACE IF NOT EXISTS twitch_dist
        WITH replication = {'class':'SimpleStrategy','replication_factor':3}
    """)
    sess.set_keyspace("twitch_dist")
    sess.execute("""
        CREATE TABLE IF NOT EXISTS consistency_demo (
            id int PRIMARY KEY, value text, written_at timestamp)
    """)

    def try_write(cl_name, cl):
        stmt = SimpleStatement(
            "INSERT INTO consistency_demo (id, value, written_at) VALUES (1, %s, toTimestamp(now()))",
            consistency_level=cl)
        try:
            sess.execute(stmt, (f"fault-test-{cl_name}",), timeout=5)
            return "OK"
        except Exception as ex:
            return f"FAIL ({type(ex).__name__}: {ex})"

    levels = {"ONE": ConsistencyLevel.ONE, "QUORUM": ConsistencyLevel.QUORUM,
              "ALL": ConsistencyLevel.ALL}

    print("\n[cassandra] baseline, 3/3 nodi su:")
    out["3_of_3"] = {n: try_write(n, cl) for n, cl in levels.items()}
    for n, r in out["3_of_3"].items():
        print(f"  CL={n:6s} -> {r}")

    print("\n[cassandra] spengo cass-2 (2/3 nodi su)...")
    docker_stop("tbd-cass-2")
    time.sleep(settle_sec)
    out["2_of_3"] = {n: try_write(n, cl) for n, cl in levels.items()}
    for n, r in out["2_of_3"].items():
        print(f"  CL={n:6s} -> {r}")

    print("\n[cassandra] spengo anche cass-3 (1/3 nodi su)...")
    docker_stop("tbd-cass-3")
    time.sleep(settle_sec)
    out["1_of_3"] = {n: try_write(n, cl) for n, cl in levels.items()}
    for n, r in out["1_of_3"].items():
        print(f"  CL={n:6s} -> {r}")

    print("\n[cassandra] ripristino cass-2 e cass-3 ...")
    docker_start("tbd-cass-2")
    docker_start("tbd-cass-3")
    return out


# ============================================================================
# COCKROACHDB
# ============================================================================
def cockroach_fault_demo(settle_sec=10):
    out = {}

    def try_query(label):
        conn = connect_cockroach(node=1)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SET statement_timeout = '5s'")
            t0 = time.perf_counter()
            cur.execute("SELECT count(*) FROM users")
            n = cur.fetchone()[0]
            dt = time.perf_counter() - t0
            return f"OK ({n} righe, {dt:.2f}s)"
        except Exception as ex:
            return f"FAIL ({type(ex).__name__}: {ex})"
        finally:
            conn.close()

    print("\n[cockroach] baseline, 3/3 nodi su:")
    out["3_of_3"] = try_query("baseline")
    print(f"  -> {out['3_of_3']}")

    print("\n[cockroach] spengo crdb-3 (2/3 nodi su, quorum Raft ancora ok)...")
    docker_stop("tbd-crdb-3")
    time.sleep(settle_sec)
    out["2_of_3"] = try_query("2of3")
    print(f"  -> {out['2_of_3']}")

    print("\n[cockroach] spengo anche crdb-2 (1/3 nodi su, NESSUN range ha quorum)...")
    docker_stop("tbd-crdb-2")
    time.sleep(settle_sec)
    out["1_of_3"] = try_query("1of3")
    print(f"  -> {out['1_of_3']}")

    print("\n[cockroach] ripristino crdb-2 e crdb-3 (occorrono ~20-30s per il rejoin Raft)...")
    docker_start("tbd-crdb-2")
    docker_start("tbd-crdb-3")
    return out


def main():
    import json
    results = {}
    try:
        results["cassandra"] = cassandra_fault_demo()
    except Exception as ex:
        print(f"[cassandra] errore demo: {ex}")

    try:
        results["cockroach"] = cockroach_fault_demo()
    except Exception as ex:
        print(f"[cockroach] errore demo: {ex}")

    with open("results_fault_tolerance.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\n[ok] results_fault_tolerance.json scritto")


if __name__ == "__main__":
    main()
