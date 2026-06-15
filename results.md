# Risultati Benchmark — Twitch Gamers Network

**Scope:** dataset completo (168k nodi, 6.8M archi)

Tempi = mediana di 2 misure dopo 1 warmup. `n/d` = non disponibile o non calcolabile in modo nativo.


## Metrica 6 — Caricamento e indicizzazione

| Database | Load (s) | Indici (s) |
|---|---|---|
| postgres | 11.94 | 14.08 |
| neo4j | 179.60 | 3.70 |

## Tempi di esecuzione per metrica

| Metrica | postgres | neo4j |
|---|---|---|
| 1. Grado medio | 0.812s | 0.046s |
| 2. Clustering | 2423.600s | 23.672s |
| 3. Betweenness (camp.) | — | 219.251s |
| 4. PageRank (10 it) | 68.980s | 1.083s |
| 5. Assortativita' | 1.659s | 9.108s |

## Valori calcolati (controllo di correttezza)

- **Riferimento NetworkX** — grado medio 80.868, dev.std 314.162, max 35279
  - transitivita' 0.0184
  - assortativita' per lingua 0.6254

### Grado medio per database

| Database | grado medio | dev. std | grado max |
|---|---|---|---|
| postgres | 80.868423 | 314.161796 | 35279 |
| neo4j | 80.868423 | 314.162731 | 35279.0 |

---
*Generato da run_benchmark.py*
