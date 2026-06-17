"""
report_distributed.py — Genera results_distributed.md confrontando
Cassandra cluster (AP, RF=3, consistenza tunabile) vs CockroachDB
(CP, RF=3, Raft/SERIALIZABLE).
"""
from __future__ import annotations
import json
import os


def _load_json(path):
    if path and os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def _fmt(v, suffix="s", digits=3):
    if v is None:
        return "n/d"
    if isinstance(v, (int, float)):
        return f"{v:.{digits}f}{suffix}"
    return "n/d"


def _fmt_ms(v, digits=2):
    if v is None:
        return "n/d"
    return f"{v * 1000:.{digits}f}ms"


def _median_sec(entry):
    if isinstance(entry, dict):
        return entry.get("median_sec")
    return None


def _deg(entry):
    """Estrae avg_deg/std_deg/max_deg da un blocco degree (formato cockroach o cassandra)."""
    if not isinstance(entry, dict):
        return {}
    return entry.get("result") or entry


def build_report(results, path="results_distributed.md", **_kwargs):
    dist_metrics = results.get("metrics", {})
    dist_load = results.get("load", {})
    sample = results.get("meta", {}).get("sample")

    cass = dist_metrics.get("cassandra_cluster", {}) or {}
    crdb = dist_metrics.get("cockroach", {}) or {}

    scope = f"{sample} nodi" if sample else "dataset completo"

    lines = []
    lines.append("# Benchmark Distribuito: Cassandra (AP) vs CockroachDB (CP)\n")
    lines.append(f"**Dataset:** Twitch Gamers Network — {scope}, 3 nodi RF=3 per sistema.\n")
    lines.append("| | Cassandra cluster | CockroachDB |")
    lines.append("|---|---|---|")
    lines.append("| Categoria CAP | AP (disponibilita' prioritaria) | CP (consistenza forte) |")
    lines.append("| Consenso | Gossip + hinted handoff | Raft (leader per range) |")
    lines.append("| Consistenza | Tunabile per query (ONE/QUORUM/ALL) | Sempre SERIALIZABLE |")
    lines.append("| Modello dati | Wide-column, denormalizzato | Relazionale (SQL) |")

    # =========================================================================
    # 1. Caricamento
    # =========================================================================
    lines.append("\n## 1. Caricamento\n")
    lines.append("| | Cassandra cluster | CockroachDB |")
    lines.append("|---|---|---|")

    cl = dist_load.get("cassandra_cluster", {})
    cr = dist_load.get("cockroach", {})
    cl_note = f"CL={cl['write_consistency']}" if cl.get("write_consistency") else ""
    lines.append(f"| Tempo load (s) | {_fmt(cl.get('load_sec'), 's', 2)} {cl_note} | "
                  f"{_fmt(cr.get('load_sec'), 's', 2)} |")
    lines.append(f"| Righe nodi | {cl.get('rows_nodes', 'n/d')} | {cr.get('rows_nodes', 'n/d')} |")
    lines.append(f"| Righe archi | {cl.get('rows_edges', 'n/d')} | {cr.get('rows_edges', 'n/d')} |")

    # =========================================================================
    # 2. Grado medio
    # =========================================================================
    lines.append("\n## 2. Grado medio\n")
    lines.append("Cassandra: misurato a CL=QUORUM. CockroachDB: query distribuita "
                  "(`distribution: full`) con consistenza forte.\n")
    lines.append("| | Cassandra (CL=QUORUM) | CockroachDB |")
    lines.append("|---|---|---|")

    cass_q = (cass.get("degree_global") or {}).get("QUORUM", {})
    crdb_d = _deg(crdb.get("degree_global", {}))

    lines.append(f"| Grado medio | {_fmt(cass_q.get('avg_deg'), '', 3)} | "
                  f"{_fmt(crdb_d.get('avg_deg'), '', 3)} |")
    lines.append(f"| Dev. std | {_fmt(cass_q.get('std_deg'), '', 3)} | "
                  f"{_fmt(crdb_d.get('std_deg'), '', 3)} |")
    lines.append(f"| Grado max | {_fmt(cass_q.get('max_deg'), '', 0)} | "
                  f"{_fmt(crdb_d.get('max_deg'), '', 0)} |")

    # =========================================================================
    # 3. Tempi per metrica
    # =========================================================================
    lines.append("\n## 3. Tempi di esecuzione\n")
    lines.append("| Metrica | Cassandra cluster | CockroachDB |")
    lines.append("|---|---|---|")

    cass_deg_t = cass_q.get("time_sec")
    crdb_deg_t = _median_sec(crdb.get("degree_global"))
    lines.append(f"| Grado medio (global) | {_fmt(cass_deg_t)} | {_fmt(crdb_deg_t)} |")

    cass_pr = _median_sec(cass.get("pagerank"))
    crdb_pr = _median_sec(crdb.get("pagerank"))
    lines.append(f"| PageRank (10 it.) | {_fmt(cass_pr)} | {_fmt(crdb_pr)} |")

    cass_as = _median_sec(cass.get("assortativity"))
    crdb_as = _median_sec(crdb.get("assortativity"))
    lines.append(f"| Assortativita' | {_fmt(cass_as)} | {_fmt(crdb_as)} |")

    if "clustering" in crdb:
        lines.append(f"| Clustering | n/d | {_fmt(_median_sec(crdb.get('clustering')))} |")

    # =========================================================================
    # 4. Cassandra — query locale vs globale per Consistency Level
    # =========================================================================
    lines.append("\n## 4. Cassandra — locale vs globale per Consistency Level\n")
    lines.append("**Locale** = lettura di 1 partizione (`source_id = X`), "
                  "**globale** = full token-range scan su tutti i nodi.\n")
    lines.append("| CL | locale (ms) | globale (s) | note |")
    lines.append("|---|---|---|---|")

    notes = {
        "ONE":    "1 replica risponde, le altre 2 in background",
        "QUORUM": "2/3 repliche rispondono — compromesso tipico",
        "ALL":    "tutte e 3 le repliche rispondono — max latenza",
    }
    deg_local  = cass.get("degree_local",  {}) or {}
    deg_global = cass.get("degree_global", {}) or {}
    for cl in ("ONE", "QUORUM", "ALL"):
        loc = deg_local.get(cl, {})
        glb = deg_global.get(cl, {})
        lines.append(f"| {cl} | {_fmt_ms(loc.get('time_sec'))} | "
                      f"{_fmt(glb.get('time_sec'))} | {notes[cl]} |")

    # =========================================================================
    # 5. CockroachDB — EXPLAIN distribution
    # =========================================================================
    lines.append("\n## 5. CockroachDB — distribuzione del piano (EXPLAIN)\n")
    explain_local  = crdb.get('explain_local',  'n/d')
    explain_global = crdb.get('explain_global', 'n/d')
    lines.append("| Query | EXPLAIN distribution |")
    lines.append("|---|---|")
    lines.append(f"| `WHERE source_id = X` (punto singolo) | `{explain_local}` |")
    lines.append(f"| `GROUP BY source_id` (scan globale) | `{explain_global}` |")
    if explain_local == explain_global == "distribution: full":
        lines.append("\n> **Nota:** su un campione piccolo l'intera tabella `follows` "
                      "sta in un solo Raft range, quindi CockroachDB usa DistSQL "
                      "(`full`) per entrambe le query. La distinzione `local` vs `full` "
                      "diventa visibile su dataset piu' grandi, dove la tabella e' "
                      "distribuita su piu' range/nodi reali.")

    # =========================================================================
    # 6. Demo consistenza
    # =========================================================================
    consistency = _load_json("results_consistency.json")
    if consistency:
        lines.append("\n## 6. Demo consistenza\n")

        cl_lat = consistency.get("cassandra_cl_latency")
        if cl_lat:
            lines.append("### Cassandra — latenza scrittura/lettura per CL\n")
            lines.append("| CL | scrittura mediana (ms) | lettura mediana (ms) |")
            lines.append("|---|---|---|")
            for cl in ("ONE", "QUORUM", "ALL"):
                r = cl_lat.get(cl, {})
                lines.append(f"| {cl} | {_fmt(r.get('write_median_ms'), 'ms', 2)} | "
                              f"{_fmt(r.get('read_median_ms'), 'ms', 2)} |")

        lines.append("\n### CockroachDB — conflitti concorrenti (read-modify-write)\n")
        lines.append("Due thread con delta +10/-10 su `accounts.id=1`. "
                      "Saldo iniziale = saldo atteso = 1000.\n")
        lines.append("| Sistema | isolamento | retry (40001) | saldo finale | consistente |")
        lines.append("|---|---|---|---|---|")
        for key, label in (("cockroach_conflict",       "CockroachDB (sempre SERIALIZABLE)"),
                            ("postgres_serializable",    "PostgreSQL SERIALIZABLE"),
                            ("postgres_read_committed",  "PostgreSQL READ COMMITTED")):
            r = consistency.get(key)
            if r:
                lines.append(f"| {label} | {r.get('isolation')} | {r.get('retries')} | "
                              f"{r.get('final_balance')} | {r.get('consistent')} |")

    # =========================================================================
    # 7. Demo fault tolerance
    # =========================================================================
    fault = _load_json("results_fault_tolerance.json")
    if fault:
        lines.append("\n## 7. Fault tolerance — nodo down (RF=3)\n")

        cass_fault = fault.get("cassandra")
        if cass_fault:
            lines.append("### Cassandra — effetto del CL con nodi mancanti\n")
            lines.append("| Nodi attivi | CL=ONE | CL=QUORUM | CL=ALL |")
            lines.append("|---|---|---|---|")
            for key, label in (("3_of_3", "3/3"), ("2_of_3", "2/3 (1 spento)"),
                                ("1_of_3", "1/3 (2 spenti)")):
                r = cass_fault.get(key, {})
                lines.append(f"| {label} | {r.get('ONE', 'n/d')} | "
                              f"{r.get('QUORUM', 'n/d')} | {r.get('ALL', 'n/d')} |")

        crdb_fault = fault.get("cockroach")
        if crdb_fault:
            lines.append("\n### CockroachDB — Raft sotto quorum\n")
            lines.append("| Nodi attivi | SELECT count(*) FROM users |")
            lines.append("|---|---|")
            for key, label in (("3_of_3", "3/3"), ("2_of_3", "2/3 — quorum Raft ok, ri-elezione"),
                                ("1_of_3", "1/3 — NESSUN range ha quorum")):
                lines.append(f"| {label} | {crdb_fault.get(key, 'n/d')} |")

    lines.append("\n---\n*Generato da `report_distributed.py` — "
                  "vedi `docs/07_distribuito.md` per l'interpretazione.*\n")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path
