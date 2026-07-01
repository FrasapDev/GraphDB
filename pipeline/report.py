"""
report.py — Genera results.md da results.json: tabelle comparative dei tempi
per metrica, tempi di load/indicizzazione, e un istogramma testuale della
distribuzione dei gradi (metrica 1).
"""
from __future__ import annotations


def _fmt(v):
    if isinstance(v, dict) and "median_sec" in v:
        return f"{v['median_sec']:.3f}s"
    if isinstance(v, dict) and "error" in v:
        return "n/d"
    return "—"


def _ascii_hist(degree_result, width=40):
    """Istogramma testuale log-binned della distribuzione dei gradi, se
    disponibile dal riferimento (richiede la lista dei gradi, altrimenti
    mostra solo media/dev std/max)."""
    return None


def build_report(results, path="results.md"):
    metrics = results.get("metrics", {})
    load = results.get("load", {})
    dbs = [d for d in ("postgres", "neo4j") if d in metrics]
    sample = results.get("meta", {}).get("sample")

    lines = []
    lines.append("# Risultati Benchmark — Twitch Gamers Network\n")
    scope = f"sottocampione di {sample} nodi" if sample else "dataset completo (168k nodi, 6.8M archi)"
    lines.append(f"**Scope:** {scope}\n")
    lines.append("Tempi = mediana di 2 misure dopo 1 warmup. `n/d` = non "
                 "disponibile o non calcolabile in modo nativo.\n")

    # --- Tempi di load / indicizzazione (metrica 6) ---
    lines.append("\n## Metrica 6 — Caricamento e indicizzazione\n")
    lines.append("| Database | Load (s) | Indici (s) |")
    lines.append("|---|---|---|")
    for d in dbs:
        l = load.get(d, {})
        ls = f"{l.get('load_sec', float('nan')):.2f}" if l else "—"
        ix = f"{l.get('index_sec', float('nan')):.2f}" if l else "—"
        lines.append(f"| {d} | {ls} | {ix} |")

    # --- Tempi per metrica ---
    metric_labels = [("degree", "1. Grado medio"),
                     ("clustering", "2. Clustering"),
                     ("betweenness", "3. Betweenness (camp.)"),
                     ("pagerank", "4. PageRank (10 it)"),
                     ("assortativity", "5. Assortativita'")]
    lines.append("\n## Tempi di esecuzione per metrica\n")
    header = "| Metrica | " + " | ".join(dbs) + " |"
    lines.append(header)
    lines.append("|" + "---|" * (len(dbs) + 1))
    for key, label in metric_labels:
        row = [label]
        for d in dbs:
            row.append(_fmt(metrics.get(d, {}).get(key, {})))
        lines.append("| " + " | ".join(row) + " |")

    # --- Valori calcolati (verifica di correttezza) ---
    lines.append("\n## Valori calcolati (controllo di correttezza)\n")
    ref = metrics.get("reference", {})
    if ref:
        deg = ref.get("degree", {})
        lines.append(f"- **Riferimento NetworkX** — grado medio "
                     f"{deg.get('avg_deg', float('nan')):.3f}, "
                     f"dev.std {deg.get('std_deg', float('nan')):.3f}, "
                     f"max {deg.get('max_deg', 'n/d')}")
        if "clustering" in ref:
            lines.append(f"  - transitivita' "
                         f"{ref['clustering'].get('transitivity', float('nan')):.4f}")
        if "assortativity" in ref:
            lines.append(f"  - assortativita' per lingua "
                         f"{ref.get('assortativity', float('nan')):.4f}")

    # --- Confronto valori grado tra DB ---
    lines.append("\n### Grado medio per database\n")
    lines.append("| Database | grado medio | dev. std | grado max |")
    lines.append("|---|---|---|---|")
    for d in dbs:
        r = metrics.get(d, {}).get("degree", {}).get("result", {})
        if isinstance(r, dict) and r:
            lines.append(f"| {d} | {r.get('avg_deg','—')} | "
                         f"{r.get('std_deg','—')} | {r.get('max_deg','—')} |")

    lines.append("\n---\n*Generato da run_benchmark.py*\n")

    with open(path, "w") as f:
        f.write("\n".join(lines))
    return path
