# 2. Metriche e complessita' computazionale

V = numero nodi (168.114), E = numero archi non diretti (6.797.557),
d = grado medio (~81). Archi duplicati (bidirezionali): ~13.6M righe.

Ogni metrica viene eseguita con **1 warmup** (scartato) **+ 2 misurazioni**;
si riporta la **mediana**. Il riferimento di correttezza e' NetworkX sullo
stesso campione.

---

## 2.1 Grado medio (metrica 1)

**Definizione**: per ogni nodo, il numero di archi incidenti. Si calcolano
media, deviazione standard e massimo sull'intero grafo.

**Complessita'**: O(E) — una scansione completa degli archi.

| DB | Implementazione | Note |
|---|---|---|
| **PostgreSQL** | `WITH deg AS (SELECT src, count(*) FROM edges GROUP BY src) SELECT avg(d), stddev_pop(d), max(d)` | Un aggregato SQL servito da `idx_edges_src`; lineare e molto adatto al modello relazionale |
| **Neo4j** | `gds.degree.stream()` sulla proiezione CSR | Il grado e' la differenza tra due offset nell'array CSR → O(1)/nodo, parallelizzabile |

**Valori di riferimento** (dataset completo): grado medio = 80.868,
dev. std = 314.162, max = 35.279.

---

## 2.2 Clustering coefficient (metrica 2)

**Definizione**: transitivity globale = `3 x triangoli / triplette aperte`.
Misura quanto i vicini di un nodo tendono a essere connessi tra loro.

**Complessita'**: O(sum_v deg(v)^2) — per ogni nodo, controlla le coppie di
vicini. E' la metrica piu' costosa del benchmark.

| DB | Implementazione | Note |
|---|---|---|
| **PostgreSQL** | Triple self-join: `und e1 JOIN und e2 ON e1.b=e2.a JOIN und e3 ON e3.a=e1.a AND e3.b=e2.b` | **2423s** sul dataset intero — il join a stella esplode in memoria. Indice `(src,dst)` decisivo per l'index-only lookup "esiste X→Y?" |
| **Neo4j** | `gds.triangleCount.stats()` + `gds.localClusteringCoefficient.stream()` | ~24s — algoritmo nativo ottimizzato su CSR in RAM, parallelizzato |

**Valore di riferimento**: transitivity = 0.0184.

---

## 2.3 Betweenness centrality campionata (metrica 3)

**Definizione**: per ogni nodo, la frazione di cammini minimi tra tutte le
coppie che passano per esso. Indica quanto un nodo e' "ponte". Campionata su
`s` sorgenti per contenere i tempi.

**Complessita'**: Brandes esatto = O(V x E). Con `s` sorgenti = O(s x E).

| DB | Implementazione | Note |
|---|---|---|
| **PostgreSQL** | CTE ricorsiva (`WITH RECURSIVE`) per BFS — restituisce distanze medie come **proxy di costo**, non la betweenness piena | SQL non ha stato di visita nativo: ri-deduplica i nodi visitati ad ogni livello. La Brandes completa in SQL puro e' impraticabile — e' un risultato architetturale, non un limite dell'implementazione |
| **Neo4j** | `gds.betweenness.stream()` con sampling nativo (Brandes ottimizzato) | E' il workload dove l'index-free adjacency da' il vantaggio massimo: ogni hop e' un pointer-chase in RAM |

---

## 2.4 PageRank (metrica 4)

**Definizione**: algoritmo iterativo che assegna un "punteggio di importanza"
ad ogni nodo. Ad ogni iterazione, ogni nodo distribuisce il proprio rank ai
vicini in proporzione al proprio out-degree. Parametri: 10 iterazioni,
damping factor = 0.85.

**Complessita'**: O(iter x E) — ogni iterazione attraversa tutti gli archi.

| DB | Implementazione | Note |
|---|---|---|
| **PostgreSQL** | 10 iterazioni SQL, ognuna = `CREATE TEMP TABLE pr_new AS SELECT ... JOIN edges JOIN pr GROUP BY` + drop/rename | Funziona ma paga la materializzazione ripetuta della tabella rank su disco/temp (~69s) |
| **Neo4j** | `gds.pageRank.stream()` su CSR in RAM | Vettori densi in RAM, iterazione parallela (~1s) — workload OLAP iterativo dove la localita' di memoria della CSR e' decisiva |

---

## 2.5 Assortativita' per lingua (metrica 5)

**Definizione**: coefficiente di Newman `r` in [-1, 1] che misura la tendenza
dei nodi a connettersi con nodi della **stessa lingua**. Valore alto = forte
omofilia linguistica.

**Formula**: `r = (Tr(e) - ||e^2||) / (1 - ||e^2||)` sulla matrice di
mescolamento normalizzata `e_ij` (Newman 2003, assortativita' nominale).

**Complessita'**: O(E) — una scansione degli archi con lookup attributo lingua.

| DB | Implementazione | Note |
|---|---|---|
| **PostgreSQL** | `JOIN edges → nodes` (x2) + `GROUP BY (lingua_a, lingua_b)` | E' IL caso dove SQL e' a casa sua: join relazionale classico servito dalla PK di nodes |
| **Neo4j** | `MATCH (a)-[:FOLLOWS]-(b) RETURN a.language, b.language, count(*)` | Property lookup via pointer dal record relazione al record nodo: O(1)/estremo |

**Valore di riferimento**: r = 0.6254 (forte omofilia linguistica — i canali
Twitch tendono a seguire canali nella stessa lingua).

---

## 2.6 Caricamento e indicizzazione (metrica 6)

**Definizione**: tempo di import del dataset (nodi + archi) e tempo di
creazione degli indici, misurati separatamente.

| DB | Strategia di load | Indici |
|---|---|---|
| **PostgreSQL** | `COPY FROM STDIN` (stream) — il bulk loader piu' veloce di Postgres, bypassa l'ORM e il parsing riga-per-riga | 3 indici B-tree su edges + `ANALYZE` (aggiorna le statistiche del planner) — misurati a parte |
| **Neo4j** | `UNWIND $rows` a batch (20k per transazione) + `MATCH` + `CREATE` | Constraint `UNIQUE` + indice su `language` creati prima del load (servono durante il load per il MATCH) |

---

## 2.7 Tabella riepilogativa delle complessita'

| Metrica | Complessita' | PostgreSQL | Neo4j |
|---|---|---|---|
| 1. Grado | O(E) | GROUP BY | GDS degree |
| 2. Clustering | O(sum deg^2) | Triple self-join | GDS triangleCount |
| 3. Betweenness | O(s x E) | CTE ricorsiva (proxy) | GDS Brandes |
| 4. PageRank | O(iter x E) | 10 iterazioni SQL | GDS pageRank |
| 5. Assortativita' | O(E) | 2x JOIN | Cypher pattern |
| 6. Load/indici | O(V + E) | COPY + CREATE INDEX | UNWIND batch |
