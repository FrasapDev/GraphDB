#!/usr/bin/env python3
"""
consistency_demo.py — Esperimenti sulla CONSISTENZA in due sistemi con
logiche opposte:

  (A) Cassandra (AP, consistenza TUNABILE) — latenza di scrittura/lettura
      al variare del Consistency Level (ONE / QUORUM / ALL) su un cluster
      RF=3. Nessun errore possibile: il CL sceglie solo QUANTE repliche
      aspettare, la query va sempre a buon fine (finche' ci sono repliche
      disponibili — vedi fault_tolerance_demo.py per i guasti).

  (B) CockroachDB vs PostgreSQL — isolamento delle transazioni.
      ATTENZIONE concettuale (vedi db/cockroachdb_schema.sql): CockroachDB
      NON ha un livello SNAPSHOT separato da SERIALIZABLE come Postgres —
      dalla v20.1 ogni `SET TRANSACTION ISOLATION LEVEL ...` viene mappato
      su SERIALIZABLE. Il confronto interessante non e' quindi "SNAPSHOT vs
      SERIALIZABLE su CockroachDB", ma:
        - PostgreSQL READ COMMITTED (default): un read-modify-write
          concorrente produce un LOST UPDATE silenzioso (nessun errore, ma
          il risultato finale e' sbagliato).
        - PostgreSQL SERIALIZABLE: la stessa race viene rilevata e una delle
          due transazioni fallisce con SQLSTATE 40001 ("serialization
          failure"), il client deve ritentarla.
        - CockroachDB: e' SEMPRE nello stato "SERIALIZABLE" — lo stesso
          codice 40001 puo' presentarsi SEMPRE, anche se non lo richiedi
          esplicitamente. La serializzabilita' e' il default non
          negoziabile, ottenuta via concorrenza ottimistica (Raft + retry).

  Esperimento (B): due transazioni concorrenti fanno read-modify-write
  sullo stesso conto (accounts.id=1) con delta opposti (+10/-10), N volte
  ciascuna. Se il sistema e' corretto, il saldo finale torna a 1000 (il
  valore iniziale): un saldo finale != 1000 e' la "prova" di un lost update.
"""
from __future__ import annotations
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cassandra_cluster import connect_cluster  # noqa: E402
from cockroach import connect_cockroach  # noqa: E402


# ============================================================================
# (A) Cassandra — latenza per Consistency Level
# ============================================================================
def cassandra_cl_latency(session, n=200):
    """Scrive e legge n righe della tabella consistency_demo (RF=3) a
    CL=ONE/QUORUM/ALL e misura la latenza mediana. Atteso: ONE < QUORUM < ALL
    sia in scrittura che in lettura, perche' il coordinatore deve aspettare
    l'ACK di un numero crescente di repliche prima di rispondere al client."""
    from cassandra.query import SimpleStatement
    from cassandra import ConsistencyLevel

    levels = {"ONE": ConsistencyLevel.ONE,
              "QUORUM": ConsistencyLevel.QUORUM,
              "ALL": ConsistencyLevel.ALL}
    out = {}
    for name, cl in levels.items():
        write_t = []
        for i in range(n):
            stmt = SimpleStatement(
                "INSERT INTO consistency_demo (id, value, written_at) "
                "VALUES (%s, %s, toTimestamp(now()))",
                consistency_level=cl)
            t0 = time.perf_counter()
            session.execute(stmt, (i, f"{name}-{i}"))
            write_t.append(time.perf_counter() - t0)

        read_t = []
        for i in range(n):
            stmt = SimpleStatement(
                "SELECT value FROM consistency_demo WHERE id=%s",
                consistency_level=cl)
            t0 = time.perf_counter()
            session.execute(stmt, (i,))
            read_t.append(time.perf_counter() - t0)

        out[name] = {
            "write_median_ms": statistics.median(write_t) * 1000,
            "read_median_ms": statistics.median(read_t) * 1000,
        }
    return out


# ============================================================================
# (B) CockroachDB vs PostgreSQL — conflitti e isolamento
# ============================================================================
ACCOUNTS_DDL = """
CREATE TABLE IF NOT EXISTS accounts (
    id      INT8 PRIMARY KEY,
    balance INT8 NOT NULL
)
"""


def _reset_accounts(conn, balance=1000):
    cur = conn.cursor()
    cur.execute(ACCOUNTS_DDL)
    cur.execute("DELETE FROM accounts")
    cur.execute("INSERT INTO accounts (id, balance) VALUES (1, %s)", (balance,))
    conn.commit()


def conflict_demo(conn_factory, isolation=None, n_ops=20, conflict_window=0.05,
                   initial_balance=1000):
    setup = conn_factory()
    _reset_accounts(setup, initial_balance)
    setup.close()

    stats = {"retries": 0, "errors": []}
    lock = threading.Lock()

    def worker(delta):
        conn = conn_factory()
        conn.autocommit = False
        try:
            for _ in range(n_ops):
                while True:
                    try:
                        cur = conn.cursor()
                        if isolation:
                            cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")
                        cur.execute("SELECT balance FROM accounts WHERE id=1")
                        bal = cur.fetchone()[0]
                        time.sleep(conflict_window)
                        cur.execute("UPDATE accounts SET balance=%s WHERE id=1", (bal + delta,))
                        conn.commit()
                        break
                    except Exception as ex:
                        conn.rollback()
                        code = getattr(ex, "pgcode", None)
                        if code == "40001":
                            with lock:
                                stats["retries"] += 1
                            continue  # ritenta la transazione (pattern CockroachDB)
                        with lock:
                            stats["errors"].append(str(ex))
                        break
        finally:
            conn.close()

    t0 = time.perf_counter()
    t1 = threading.Thread(target=worker, args=(+10,))
    t2 = threading.Thread(target=worker, args=(-10,))
    t1.start(); t2.start()
    t1.join(); t2.join()
    elapsed = time.perf_counter() - t0

    check = conn_factory()
    cur = check.cursor()
    cur.execute("SELECT balance FROM accounts WHERE id=1")
    final_balance = cur.fetchone()[0]
    check.close()

    return {
        "isolation": isolation or "default",
        "elapsed_sec": elapsed,
        "retries": stats["retries"],
        "errors": stats["errors"],
        "final_balance": final_balance,
        "expected_balance": initial_balance,  # +10*n_ops -10*n_ops = 0 di netto
        "consistent": final_balance == initial_balance,
    }


# ============================================================================
# Connettori
# ============================================================================
def connect_postgres():
    """Connessione al PostgreSQL del benchmark centralizzato.
    In modalità multi-VM imposta POSTGRES_HOST con l'IP della VM che ospita
    Postgres (es. Worker1 10.0.1.4). Se non impostato, usa localhost."""
    import psycopg2
    host = os.environ.get("POSTGRES_HOST", "localhost")
    return psycopg2.connect(host=host, port=5432, dbname="twitch",
                             user="bench", password="bench")


# ============================================================================
# Main
# ============================================================================
def main():
    import json
    results = {}

    # --- (A) Cassandra: latenza per CL ---------------------------------------
    try:
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
        print("[cassandra] misuro latenza per Consistency Level (ONE/QUORUM/ALL) ...")
        results["cassandra_cl_latency"] = cassandra_cl_latency(sess)
        for cl, r in results["cassandra_cl_latency"].items():
            print(f"  CL={cl:6s} write={r['write_median_ms']:.2f}ms  read={r['read_median_ms']:.2f}ms")
    except Exception as ex:
        print(f"[cassandra] non disponibile: {ex}")

    # --- (B) CockroachDB: sempre SERIALIZABLE --------------------------------
    try:
        print("\n[cockroach] demo conflitti (sempre SERIALIZABLE) ...")
        r = conflict_demo(lambda: connect_cockroach(node=1), isolation=None)
        results["cockroach_conflict"] = r
        print(f"  retry={r['retries']}  saldo finale={r['final_balance']} "
              f"(atteso {r['expected_balance']})  consistente={r['consistent']}")
    except Exception as ex:
        print(f"[cockroach] non disponibile: {ex}")

    # --- (B) PostgreSQL: READ COMMITTED vs SERIALIZABLE ----------------------
    try:
        print("\n[postgres] demo conflitti, READ COMMITTED (default) ...")
        r = conflict_demo(connect_postgres, isolation="READ COMMITTED")
        results["postgres_read_committed"] = r
        print(f"  retry={r['retries']}  saldo finale={r['final_balance']} "
              f"(atteso {r['expected_balance']})  consistente={r['consistent']}")

        print("\n[postgres] demo conflitti, SERIALIZABLE ...")
        r = conflict_demo(connect_postgres, isolation="SERIALIZABLE")
        results["postgres_serializable"] = r
        print(f"  retry={r['retries']}  saldo finale={r['final_balance']} "
              f"(atteso {r['expected_balance']})  consistente={r['consistent']}")
    except Exception as ex:
        print(f"[postgres] non disponibile: {ex}")

    with open("results_consistency.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\n[ok] results_consistency.json scritto")


if __name__ == "__main__":
    main()
