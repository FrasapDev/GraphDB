# 5. Esplicabilita' dei risultati

Questa sezione spiega **perche'** ciascun database e' piu' veloce o piu'
lento su ciascuna metrica. Non e' sufficiente dire "Neo4j batte PostgreSQL
sul PageRank": bisogna spiegare **quale meccanismo architetturale** produce
quel risultato, cosi' che la conclusione sia trasferibile ad altri dataset e
altri workload.

---

## 5.1 Risultati osservati (dataset completo, 168k nodi, 6.8M archi)

| Metrica | PostgreSQL | Neo4j | Perche' |
|---|---|---|---|
| Load | 12s + 14s indici | 180s + 4s indici | Vedi 5.2 |
| Grado medio | 0.81s | 0.05s | Vedi 5.3 |
| Clustering | 2424s | 24s | Vedi 5.4 |
| Betweenness | n/d (proxy) | 219s | Vedi 5.5 |
| PageRank | 69s | 1.1s | Vedi 5.6 |
| Assortativita' | 1.7s | 9.1s | Vedi 5.7 |

---

## 5.2 Caricamento: PostgreSQL vince (12s vs 180s)

**PostgreSQL** usa `COPY FROM STDIN`: un comando bulk che bypassa il parser
SQL riga-per-riga e scrive direttamente nell'heap table. E' il meccanismo piu'
veloce disponibile, equivalente a un `memcpy` strutturato. Gli indici vengono
creati **dopo** il load (tutti gli archi gia' in tabella), quindi il B-tree
si costruisce con un sort sequenziale — molto piu' efficiente che mantenere
l'indice durante ogni singolo INSERT.

**Neo4j** usa `UNWIND` + `CREATE` a batch (20k per transazione). Ogni
relazione richiede un **doppio MATCH** sui nodi sorgente e destinazione
(serviti dall'indice di unicita'), piu' la creazione del record di relazione
con i suoi puntatori bidirezionali. Il costo per arco e' intrinsecamente piu'
alto: Neo4j sta costruendo la struttura di index-free adjacency (puntatori
nodo↔relazione) **durante** l'inserimento.

> **Lezione architetturale**: il vantaggio di Neo4j nel traversal si paga nel
> caricamento. Il pointer-chasing e' veloce da percorrere ma costoso da
> costruire.

---

## 5.3 Grado medio: Neo4j vince (0.05s vs 0.81s, 16x)

**Neo4j (GDS)**: nella proiezione CSR, il grado di un nodo e' la
**differenza tra due offset** nell'array (O(1)). La scansione di 168k nodi e'
un'operazione sequenziale su array densi in RAM, facilmente parallelizzabile.

**PostgreSQL**: il `GROUP BY src` deve scorrere l'indice `idx_edges_src`
(13.6M entries), contare per ciascun `src`, poi aggregare. E' lineare in E
ma con costante molto piu' alta: ogni riga passa per il buffer manager,
l'aggregator hash, la materializzazione del risultato intermedio.

---

## 5.4 Clustering: Neo4j vince (24s vs 2424s, 100x)

Questa e' la metrica dove la differenza architetturale e' piu' drammatica.

**Il problema**: contare i triangoli richiede, per ogni nodo X, verificare per
ogni coppia di vicini (Y, Z) se esiste l'arco Y→Z. Con grado medio ~81,
ogni nodo genera ~81x80/2 ≈ 3240 lookup "esiste quest'arco?".

**Neo4j**: i lookup sono **pointer-chase** nella CSR in RAM. Ogni verifica
"Y e' connesso a Z" e' un'intersezione di liste ordinate (merge-intersect) su
array contigui, parallelizzabile e cache-friendly.

**PostgreSQL**: ogni lookup "esiste l'arco (Y, Z)?" e' un **index lookup** su
`idx_edges_src_dst`. Anche se e' un index-only scan (non tocca la heap), il
B-tree ha una profondita' di 3-4 livelli: ~4 page read per lookup × milioni
di lookup = miliardi di page access, con contesa sul buffer pool. Il **join a
stella** (3 copie della tabella edges) amplifica il problema.

> **Lezione architetturale**: il clustering coefficient e' il benchmark
> canonico dove l'index-free adjacency batte l'indice B-tree di due ordini di
> grandezza. Non e' una questione di ottimizzazione SQL: e' il modello di
> accesso che non si presta al B-tree.

---

## 5.5 Betweenness: solo Neo4j ha un risultato vero

**Neo4j**: Brandes con sampling nativo via GDS. Ogni BFS dal nodo sorgente
attraversa la CSR per puntatori — O(1) per hop, parallelo.

**PostgreSQL**: la CTE ricorsiva (`WITH RECURSIVE`) simula la BFS espandendo
il fronte via JOIN con `edges`. SQL non ha uno "stato di visita" nativo: deve
ri-deduplicare i nodi visitati ad ogni livello (`DISTINCT` / anti-join). Il
costo per livello BFS = un hash-join + deduplicazione + materializzazione.
La betweenness esatta di Brandes in SQL puro **non e' praticabile** sul grafo
intero: e' un risultato architetturale, non un limite dell'implementazione.
Si restituiscono le distanze medie come **proxy di costo**.

---

## 5.6 PageRank: Neo4j vince (1.1s vs 69s, 63x)

**Neo4j (GDS)**: l'iterazione del PageRank aggiorna vettori densi in RAM
(array float) iterando sulla CSR. Nessuna materializzazione su disco, nessun
parsing di righe, nessun buffer manager. Workload OLAP iterativo: la
localita' di memoria della proiezione e' decisiva.

**PostgreSQL**: ogni iterazione richiede un `CREATE TEMP TABLE ... AS
SELECT ... JOIN edges JOIN pr GROUP BY`. Cio' significa:
1. Leggere la tabella `pr` corrente (~168k righe)
2. Hash-join con `edges` (13.6M righe) → match sulle chiavi
3. GROUP BY nodo + aggregazione SUM
4. Materializzare il risultato come nuova tabella temporanea
5. DROP della vecchia tabella

Ogni iterazione e' un ciclo completo di I/O. 10 iterazioni = 10 cicli. Il
collo di bottiglia non e' la complessita' asintotica (O(iter x E) per
entrambi) ma la **costante moltiplicativa** della materializzazione ripetuta.

---

## 5.7 Assortativita': PostgreSQL vince (1.7s vs 9.1s, 0.2x)

E' l'**unica** metrica dove PostgreSQL batte Neo4j.

**PostgreSQL**: `JOIN edges → nodes` (x2 per i due estremi dell'arco) +
`GROUP BY (lingua_a, lingua_b)`. E' un hash-join classico, il workload per
cui il modello relazionale e' ottimizzato da 50 anni. La PK di `nodes`
rende il lookup O(log V) per arco, e il planner puo' scegliere tra hash-join
e merge-join in base alle statistiche.

**Neo4j**: `MATCH (a)-[:FOLLOWS]-(b) RETURN a.language, b.language, count(*)`.
Per ogni relazione, il driver deve saltare dal record relazione al record nodo
(x2), leggere la property `language`, e aggregare. L'index-free adjacency non
aiuta qui: il costo e' nel **property lookup**, non nel traversal. Il pattern
matching di Cypher introduce overhead di interpretazione per la
serializzazione del risultato aggregato.

> **Lezione architetturale**: l'index-free adjacency e' un vantaggio per il
> traversal (percorsi, BFS, propagazione), ma per le aggregazioni
> attributo-per-attributo sugli estremi degli archi il JOIN relazionale rimane
> piu' efficiente. Neo4j eccelle quando la struttura topologica conta; SQL
> eccelle quando gli attributi contano.

