# 1. Modellazione del dato — Twitch Gamers Network

## 1.1 Il dataset

Il dataset proviene da [Stanford SNAP](https://snap.stanford.edu/data/twitch_gamers.html):
- **168.114 nodi** — canali Twitch, ciascuno con attributi: `views`, `mature`,
  `life_time`, `dead_account`, `language`, `affiliate`
- **6.797.557 archi** — relazioni di "follow" mutuo (grafo **non diretto**,
  1 sola componente connessa, nessun attributo mancante)

Il problema di modellazione e': **come rappresentare un grafo non diretto con
attributi sui nodi in tre sistemi con filosofie diverse?** Le scelte fatte qui
sotto sono deliberate e giustificate — non sono l'unico modo, ma sono il modo
che rende piu' trasparente il confronto architetturale.

---

## 1.2 PostgreSQL — modello relazionale (`db/postgres_schema.sql`)

### Schema

```sql
CREATE TABLE nodes (
    id           INTEGER PRIMARY KEY,
    views        BIGINT,
    mature       BOOLEAN,
    life_time    INTEGER,
    dead_account BOOLEAN,
    language     TEXT,
    affiliate    BOOLEAN
);

CREATE TABLE edges (
    src INTEGER NOT NULL,
    dst INTEGER NOT NULL
);
CREATE INDEX idx_edges_src     ON edges (src);
CREATE INDEX idx_edges_dst     ON edges (dst);
CREATE INDEX idx_edges_src_dst ON edges (src, dst);
CREATE INDEX idx_nodes_language ON nodes (language);
```

### Scelta chiave: archi duplicati

Il grafo originale e' non diretto. In SQL, "i vicini di X" potrebbe essere
`WHERE src = X OR dst = X` — ma un `OR` su due colonne diverse **spezza l'uso
degli indici B-tree**: il planner deve fare due range-scan + merge, oppure un
seq scan. E' un anti-pattern noto.

La soluzione standard: inserire **ogni arco due volte** — `(a, b)` e `(b, a)`.
Cosi' "i vicini di X" = `WHERE src = X`, una **singola range-scan contigua**
sull'indice `idx_edges_src`. Costo: 2x lo storage (13.6M righe invece di
6.8M), ma le query sono drasticamente piu' veloci.

> Questo trade-off — pagare storage e scrittura per rendere il vicinato una
> range-scan — e' esattamente cio' che Neo4j ottiene "gratis" con
> l'index-free adjacency.

### Indici

| Indice | Scopo |
|---|---|
| `idx_edges_src` | "vicini in uscita di X" — usato in degree, PageRank, BFS, clustering |
| `idx_edges_dst` | "chi punta a X" — utile per il join nel PageRank |
| `idx_edges_src_dst` | "esiste l'arco X→Y?" — index-only scan per il clustering coefficient (milioni di lookup) |
| `idx_nodes_language` | GROUP BY lingua per l'assortativita' |

La PRIMARY KEY su `nodes(id)` crea automaticamente un indice B-tree univoco,
decisivo per i JOIN `edges.src → nodes.id` nella metrica di assortativita'.

---

## 1.3 Neo4j — modello a grafo (`db/neo4j_schema.cypher`)

### Schema

```cypher
CREATE CONSTRAINT channel_id_unique IF NOT EXISTS
FOR (c:Channel) REQUIRE c.id IS UNIQUE;

CREATE INDEX channel_language IF NOT EXISTS
FOR (c:Channel) ON (c.language);
```

I nodi sono etichettati `:Channel` con le stesse property del CSV. Le
relazioni sono di tipo `:FOLLOWS`, create **una sola volta** per coppia (non
duplicate) perche' in Neo4j ogni relazione e' attraversabile in **entrambe le
direzioni** a costo identico.

### Index-free adjacency (concetto chiave)

In Neo4j ogni nodo memorizza fisicamente il **puntatore** alla testa della
propria lista di relazioni; ogni relazione punta ai due nodi e alle relazioni
precedente/successiva. "I vicini di X" si ottengono seguendo puntatori in
memoria — **nessun indice, nessuna ricerca**, costo `O(grado(X))`.

Gli indici creati nello schema servono solo a **trovare il nodo di partenza**
(lookup per `id` o `language`), non per attraversare il grafo. Questo e' il
vantaggio strutturale di Neo4j per operazioni di traversal (BFS, PageRank,
betweenness).

### GDS (Graph Data Science)

Le metriche complesse (degree, clustering, PageRank, betweenness) usano la
libreria GDS, che **proietta** il grafo dallo storage transazionale in una
struttura **Compressed Sparse Row (CSR) in RAM**, ottimizzata per scansioni
sequenziali e parallelismo. La proiezione e' il corrispettivo Neo4j del
"caricamento in RAM" che fa NetworkX, ma parallelizzato e specializzato.

---

## 1.4 Confronto delle scelte

| Aspetto | PostgreSQL | Neo4j |
|---|---|---|
| Archi duplicati? | Si' (2x) | No (1x, bidirezionale nativo) |
| "Vicini di X" | Range scan su indice B-tree | Pointer-chasing (O(grado)) |
| Join | Nativo (hash/merge join) | Nativo (pattern matching) |
| Aggregazione globale | SQL (`GROUP BY`, `avg`, ...) | Cypher / GDS |
| Indici espliciti | 4 su edges + 1 su nodes | 1 constraint + 1 index |
| Storage archi (168k nodi) | ~13.6M righe | ~6.8M relazioni |
