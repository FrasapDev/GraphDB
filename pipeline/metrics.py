"""
metrics.py — Le 6 metriche, implementate per ciascun database con l'approccio
nativo piu' onesto:

  Neo4j      -> Graph Data Science (GDS): algoritmi nativi su grafo proiettato
  PostgreSQL -> SQL puro, incluse CTE ricorsive per i percorsi
  Cassandra  -> CQL per estrarre le partizioni + aggregazione Python lato client
                (Cassandra non ha join ne' algoritmi di grafo: il calcolo
                 globale e' SEMPRE client-side)

Ogni funzione e' raccolta dentro una classe per DB. Le classi espongono la
stessa interfaccia, cosi' il runner le chiama in modo uniforme e cronometra.

Ogni metodo ha in docstring:
  - COMPLESSITA': Big-O in termini di accessi a disco / partizioni / hop
  - ARCHITETTURA: quale meccanismo interno la rende veloce o lenta

V = numero nodi (168k), E = numero archi (6.8M), d = grado medio (~81).
"""
from __future__ import annotations
import math
import random
from collections import defaultdict


# ============================================================================
#  NEO4J  — Graph Data Science
# ============================================================================
class Neo4jMetrics:
    """Usa GDS. Prima si proietta il grafo in memoria UNA volta (graph
    catalog), poi gli algoritmi girano su quella proiezione compatta
    (CSR in RAM), non sullo store transazionale. La proiezione e' il
    corrispettivo Neo4j del 'caricamento in RAM' che fa NetworkX, ma
    parallelizzato e specializzato."""

    GRAPH = "twitch_g"

    def __init__(self, driver):
        self.driver = driver

    def _run(self, q, **p):
        with self.driver.session() as s:
            return list(s.run(q, **p))

    def project(self):
        """Proietta (:Channel)-[:FOLLOWS]- come grafo NON diretto in RAM.
        UNDIRECTED: ogni relazione diventa bidirezionale nella proiezione,
        coerente col dataset (follower mutui)."""
        self._run(f"CALL gds.graph.drop('{self.GRAPH}', false) "
                  "YIELD graphName RETURN graphName")
        self._run(f"""
            CALL gds.graph.project(
              '{self.GRAPH}',
              'Channel',
              {{ FOLLOWS: {{ orientation: 'UNDIRECTED' }} }}
            )
        """)

    # --- 1. GRADO -----------------------------------------------------------
    def degree(self):
        """
        COMPLESSITA': O(V + E). gds.degree scorre la struttura CSR contando
          i vicini; lineare e parallelizzabile.
        ARCHITETTURA: la proiezione e' una Compressed Sparse Row in RAM; il
          grado e' la differenza tra due offset dell'array -> O(1) per nodo.
          Sullo store nativo sarebbe comunque O(grado) per index-free adjacency.
        """
        rows = self._run(f"""
            CALL gds.degree.stream('{self.GRAPH}')
            YIELD nodeId, score
            RETURN avg(score) AS avg_deg,
                   stdev(score) AS std_deg,
                   max(score) AS max_deg
        """)
        return dict(rows[0])

    # --- 2. CLUSTERING ------------------------------------------------------
    def clustering(self):
        """
        COMPLESSITA': O(V * d^2) nel caso peggiore (per ogni nodo, conta archi
          tra i suoi d vicini). GDS usa triangle counting ottimizzato.
        ARCHITETTURA: l'index-free adjacency rende l'accesso ai vicini-dei-
          vicini una catena di puntatori in RAM; nessun join, nessun indice.
        """
        rows = self._run(f"""
            CALL gds.localClusteringCoefficient.stream('{self.GRAPH}')
            YIELD nodeId, localClusteringCoefficient AS c
            RETURN avg(c) AS avg_local_cc
        """)
        glob = self._run(f"""
            CALL gds.triangleCount.stats('{self.GRAPH}')
            YIELD globalTriangleCount, nodeCount
            RETURN globalTriangleCount AS tri
        """)
        return {"avg_local_cc": rows[0]["avg_local_cc"],
                "triangles": glob[0]["tri"]}

    # --- 3. BETWEENNESS (campione) -----------------------------------------
    def betweenness(self, sample=1000):
        """
        COMPLESSITA': Brandes esatto = O(V*E). Con campionamento di s sorgenti
          = O(s*E). GDS implementa Brandes con sampling nativo.
        ARCHITETTURA: ogni BFS/Dijkstra dal nodo sorgente attraversa per
          puntatori (O(1)/hop). E' il workload dove Neo4j ha il vantaggio
          strutturale piu' netto: il percorso e' una passeggiata in RAM.
        """
        rows = self._run(f"""
            CALL gds.betweenness.stream('{self.GRAPH}',
                 {{ samplingSize: $s, samplingSeed: 42 }})
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).id AS id, score
            ORDER BY score DESC LIMIT 10
        """, s=sample)
        return [dict(r) for r in rows]

    # --- 4. PAGERANK --------------------------------------------------------
    def pagerank(self, iterations=10, damping=0.85):
        """
        COMPLESSITA': O(iter * E). Ogni iterazione propaga lungo tutti gli archi.
        ARCHITETTURA: GDS tiene i vettori di rank in array densi in RAM e
          itera sulla CSR. Workload OLAP iterativo: la localita' di memoria
          della proiezione e' decisiva (no random I/O su disco).
        """
        rows = self._run(f"""
            CALL gds.pageRank.stream('{self.GRAPH}',
                 {{ maxIterations: $it, dampingFactor: $d }})
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).id AS id, score
            ORDER BY score DESC LIMIT 10
        """, it=iterations, d=damping)
        return [dict(r) for r in rows]

    # --- 5. ASSORTATIVITA' per lingua --------------------------------------
    def assortativity(self):
        """
        COMPLESSITA': O(E). Una scansione degli archi confrontando l'attributo
          dei due estremi.
        ARCHITETTURA: l'attributo language e' una property sul record nodo,
          letta seguendo il puntatore dall'arco al nodo: O(1)/estremo.
          Calcoliamo il coefficiente di Newman lato Cypher aggregando la
          matrice di mescolamento e_ij per lingua.
        """
        rows = self._run("""
            MATCH (a:Channel)-[:FOLLOWS]-(b:Channel)
            WITH a.language AS la, b.language AS lb
            RETURN la, lb, count(*) AS cnt
        """)
        return _newman_from_mixing([(r["la"], r["lb"], r["cnt"]) for r in rows])


# ============================================================================
#  POSTGRESQL  — SQL puro
# ============================================================================
class PostgresMetrics:
    """SQL relazionale. Gli archi sono memorizzati DUPLICATI (a,b)+(b,a) cosi'
    che 'i vicini di X' = WHERE src=X (range scan su idx_edges_src). Tutte le
    metriche locali diventano aggregazioni; i percorsi diventano CTE ricorsive
    (BFS), che e' dove SQL soffre."""

    def __init__(self, conn):
        self.conn = conn

    def _q(self, sql, params=None, one=False):
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone() if one else cur.fetchall()

    # --- 1. GRADO -----------------------------------------------------------
    def degree(self):
        """
        COMPLESSITA': O(E) per il GROUP BY (un index scan + aggregazione).
        ARCHITETTURA: con archi duplicati, grado(X)=count(*) WHERE src=X.
          L'aggregato grado-per-nodo e' un GROUP BY src servito da
          idx_edges_src; poi avg/stddev su 168k righe. Sequenziale,
          molto adatto al modello relazionale.
        """
        return dict(zip(
            ["avg_deg", "std_deg", "max_deg"],
            self._q("""
                WITH deg AS (
                    SELECT src AS id, count(*) AS d FROM edges GROUP BY src
                )
                SELECT avg(d)::float, stddev_pop(d)::float, max(d) FROM deg
            """, one=True)))

    # --- 2. CLUSTERING (locale + globale) ----------------------------------
    def clustering_global(self):
        """
        COMPLESSITA': O(sum_v deg(v)^2) = costoso. Per ogni nodo conta le
          coppie di vicini connesse: e' un self-join tripla di edges.
        ARCHITETTURA: il conteggio dei triangoli e' tre join su edges. Il
          planner usa idx_edges_src_dst per l'index-only lookup 'X->Y esiste?'.
          E' il punto dolente di SQL: i triangoli sono un join a stella che
          esplode in memoria. Limitiamo con join su id ordinati per contare
          ogni triangolo una volta sola.
        """
        # transitivity = 3*triangoli / triplette-aperte. Calcolo su grafo
        # non diretto usando la rappresentazione duplicata ma filtrando a<b<c.
        row = self._q("""
            WITH und AS (
                SELECT src AS a, dst AS b FROM edges WHERE src < dst
            ),
            tri AS (
                SELECT count(*) AS t
                FROM und e1
                JOIN und e2 ON e1.b = e2.a
                JOIN und e3 ON e3.a = e1.a AND e3.b = e2.b
            ),
            triplets AS (
                SELECT sum(d*(d-1)/2) AS p
                FROM (SELECT src, count(*) d FROM edges GROUP BY src) g
            )
            SELECT (3.0 * tri.t) / NULLIF(triplets.p, 0) AS transitivity,
                   tri.t AS triangles
            FROM tri, triplets
        """, one=True)
        return {"transitivity": row[0], "triangles": row[1]}

    # --- 3. BETWEENNESS (campione, CTE ricorsiva = BFS) --------------------
    def betweenness_sample(self, sample_ids):
        """
        COMPLESSITA': O(s * (V+E)) per s sorgenti, MA ogni BFS in SQL ricorsivo
          materializza il fronte come righe -> costante moltiplicativa enorme,
          molta scrittura su work_mem/temp.
        ARCHITETTURA: la CTE WITH RECURSIVE simula la BFS espandendo il fronte
          via join con edges. SQL non ha uno stato di visita nativo: deve
          ri-deduplicare i nodi visitati ad ogni livello (DISTINCT/anti-join).
          Questo e' il motivo per cui i percorsi sono lenti in SQL — non c'e'
          il pointer-chasing, c'e' il join ripetuto. Qui calcoliamo solo le
          DISTANZE da ogni sorgente (base di betweenness/closeness) come prova
          di costo; la betweenness esatta di Brandes in SQL e' impraticabile
          sul grafo intero ed e' onesto dichiararlo.
        """
        # Restituiamo, per ciascuna sorgente del campione, la distanza media
        # come proxy di costo. (La betweenness completa richiederebbe il
        # conteggio dei cammini minimi, non fattibile efficientemente in SQL.)
        results = {}
        for s in sample_ids:
            row = self._q("""
                WITH RECURSIVE bfs(node, dist) AS (
                    SELECT %s::int, 0
                  UNION
                    SELECT e.dst, b.dist + 1
                    FROM bfs b JOIN edges e ON e.src = b.node
                    WHERE b.dist < 6
                )
                SELECT count(*) , avg(dist)::float FROM (
                    SELECT node, min(dist) AS dist FROM bfs GROUP BY node
                ) t
            """, (s,), one=True)
            results[s] = {"reached": row[0], "avg_dist": row[1]}
        return results

    # --- 4. PAGERANK (iterativo, una query per iterazione) -----------------
    def pagerank(self, iterations=10, damping=0.85):
        """
        COMPLESSITA': O(iter * E). Ogni iterazione = un join edges + GROUP BY
          per ridistribuire il rank. Materializza il vettore in una tabella
          temporanea ad ogni passo.
        ARCHITETTURA: SQL non ha stato iterativo in-memory; ogni iterazione
          riscrive una tabella di rank e fa hash-join con edges. Funziona ma
          paga la materializzazione ripetuta (write amplification su temp).
        """
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pr;")
            cur.execute("""
                CREATE TEMP TABLE pr AS
                SELECT id, 1.0/ (SELECT count(*) FROM nodes)::float AS rank
                FROM nodes;
            """)
            cur.execute("CREATE TABLE IF NOT EXISTS outdeg AS "
                        "SELECT src AS id, count(*) AS d FROM edges GROUP BY src;")
            n = self._q("SELECT count(*) FROM nodes", one=True)[0]
            for _ in range(iterations):
                cur.execute("""
                    CREATE TEMP TABLE pr_new AS
                    SELECT n.id,
                       (1-%s)/%s + %s * COALESCE(sum(pr.rank / od.d), 0) AS rank
                    FROM nodes n
                    LEFT JOIN edges e ON e.dst = n.id
                    LEFT JOIN pr ON pr.id = e.src
                    LEFT JOIN outdeg od ON od.id = e.src
                    GROUP BY n.id;
                """, (damping, n, damping))
                cur.execute("DROP TABLE pr; ALTER TABLE pr_new RENAME TO pr;")
            self.conn.commit()
            return self._q("SELECT id, rank FROM pr ORDER BY rank DESC LIMIT 10")

    # --- 5. ASSORTATIVITA' --------------------------------------------------
    def assortativity(self):
        """
        COMPLESSITA': O(E). Un join edges->nodes (x2 per i due estremi) +
          GROUP BY lingua. Servito dalla PK di nodes (lookup O(log V)/arco).
        ARCHITETTURA: e' IL caso in cui SQL e' a casa sua: join tra archi e
          attributi dei nodi. La matrice di mescolamento e_ij e' un
          GROUP BY (lingua_a, lingua_b). Il coefficiente di Newman si chiude
          in Python su una matrice piccola.
        """
        rows = self._q("""
            SELECT na.language AS la, nb.language AS lb, count(*) AS cnt
            FROM edges e
            JOIN nodes na ON na.id = e.src
            JOIN nodes nb ON nb.id = e.dst
            GROUP BY na.language, nb.language
        """)
        return _newman_from_mixing([(r[0], r[1], r[2]) for r in rows])


# ============================================================================
#  CASSANDRA  — CQL + aggregazione Python lato client
# ============================================================================
class CassandraMetrics:
    """Cassandra non aggrega lato server oltre la singola partizione e non
    fa join. Ogni metrica GLOBALE = (1) leggi le partizioni necessarie con
    CQL, (2) aggrega in Python. Questo rende esplicito il costo di rete e di
    trasferimento: e' il prezzo di un sistema che ottimizza l'accesso a
    chiave nota a scapito dell'analisi globale."""

    def __init__(self, session):
        self.session = session

    def _all_adjacency(self):
        """Estrae l'intera lista di adiacenza scorrendo follows_by_source.
        E' una FULL TABLE SCAN (token range scan su tutte le partizioni):
        costosa, ma e' l'unico modo per avere il grafo intero lato client.
        Ritorna dict {source: [targets]}."""
        adj = defaultdict(list)
        rows = self.session.execute(
            "SELECT source_id, target_id FROM follows_by_source")
        for r in rows:
            adj[r.source_id].append(r.target_id)
        return adj

    # --- 1. GRADO -----------------------------------------------------------
    def degree(self):
        """
        COMPLESSITA': O(P) partizioni lette (= V) + O(E) trasferiti al client.
        ARCHITETTURA: il grado di un singolo utente noto sarebbe O(1) (una
          partizione). Ma il grado MEDIO e' globale: serve toccare TUTTE le
          partizioni (full scan) e contare lato client. Cassandra non sa fare
          'avg(count per partition)' server-side.
        """
        adj = self._all_adjacency()
        degs = [len(v) for v in adj.values()]
        mean = sum(degs) / len(degs)
        var = sum((d - mean) ** 2 for d in degs) / len(degs)
        return {"avg_deg": mean, "std_deg": math.sqrt(var), "max_deg": max(degs)}

    # --- 2. CLUSTERING ------------------------------------------------------
    def clustering_global(self, adj=None):
        """
        COMPLESSITA': full scan O(E) + O(sum deg^2) lato client per i triangoli.
        ARCHITETTURA: niente join => il test 'i due vicini di X sono connessi?'
          si fa con set lookup in Python sulle liste di adiacenza gia' caricate
          in RAM. Cassandra qui e' solo un fornitore di righe; il grafo lo
          ricostruisce il client. E' lento e usa molta RAM client.
        """
        adj = adj or self._all_adjacency()
        nbr = {k: set(v) for k, v in adj.items()}
        triangles = 0
        triplets = 0
        for v, ns in nbr.items():
            ns = list(ns)
            k = len(ns)
            triplets += k * (k - 1) // 2
            for i in range(k):
                for j in range(i + 1, k):
                    if ns[j] in nbr.get(ns[i], ()):
                        triangles += 1
        # ogni triangolo contato 3 volte (una per vertice)
        triangles //= 3
        transitivity = (3 * triangles / triplets) if triplets else 0.0
        return {"transitivity": transitivity, "triangles": triangles}

    # --- 3. BETWEENNESS -----------------------------------------------------
    def betweenness(self, sample=500, adj=None):
        """
        COMPLESSITA': full scan O(E) per costruire il grafo + Brandes O(s*E)
          lato client.
        ARCHITETTURA: Cassandra NON puo' fare BFS server-side. Si scarica
          tutto e si esegue Brandes in Python. Il DB e' un magazzino, non un
          motore di calcolo su grafo. Il campione e' ridotto a 500 (come da
          vincolo) perche' tutto pesa sulla RAM del client.
        """
        adj = adj or self._all_adjacency()
        nbr = {k: list(set(v)) for k, v in adj.items()}
        nodes = list(nbr.keys())
        random.seed(42)
        sources = random.sample(nodes, min(sample, len(nodes)))
        bc = dict.fromkeys(nbr, 0.0)
        for s in sources:                       # Brandes (non pesato)
            S, P, sigma, d = [], defaultdict(list), defaultdict(float), {}
            sigma[s] = 1.0; d[s] = 0
            from collections import deque
            Q = deque([s])
            while Q:
                v = Q.popleft(); S.append(v)
                for w in nbr.get(v, ()):
                    if w not in d:
                        d[w] = d[v] + 1; Q.append(w)
                    if d[w] == d[v] + 1:
                        sigma[w] += sigma[v]; P[w].append(v)
            delta = defaultdict(float)
            while S:
                w = S.pop()
                for v in P[w]:
                    delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
                if w != s:
                    bc[w] += delta[w]
        top = sorted(bc.items(), key=lambda x: -x[1])[:10]
        return [{"id": n, "score": v} for n, v in top]

    # --- 4. PAGERANK --------------------------------------------------------
    def pagerank(self, iterations=10, damping=0.85, adj=None):
        """
        COMPLESSITA': full scan O(E) + O(iter*E) lato client.
        ARCHITETTURA: identico discorso: l'iterazione di propagazione gira in
          Python su dizionari. Cassandra non ha alcun concetto di iterazione
          su grafo. Tutto il valore aggiunto e' nel client.
        """
        adj = adj or self._all_adjacency()
        nodes = list(adj.keys())
        N = len(nodes)
        rank = {n: 1.0 / N for n in nodes}
        outdeg = {n: len(adj[n]) for n in nodes}
        for _ in range(iterations):
            new = {n: (1 - damping) / N for n in nodes}
            for src, tgts in adj.items():
                if not tgts:
                    continue
                share = damping * rank[src] / outdeg[src]
                for t in tgts:
                    if t in new:
                        new[t] += share
            rank = new
        top = sorted(rank.items(), key=lambda x: -x[1])[:10]
        return [{"id": n, "score": v} for n, v in top]

    # --- 5. ASSORTATIVITA' --------------------------------------------------
    def assortativity(self, adj=None):
        """
        COMPLESSITA': full scan archi O(E) + full scan canali O(V) + join
          lato client.
        ARCHITETTURA: il 'join' arco-lingua NON esiste in Cassandra. Carichiamo
          la mappa id->lingua da channels_by_id (full scan) in un dict, poi
          per ogni arco guardiamo le due lingue in RAM. Il join lo fa il
          client; il DB non sa correlare due tabelle.
        """
        lang = {}
        for r in self.session.execute("SELECT id, language FROM channels_by_id"):
            lang[r.id] = r.language
        adj = adj or self._all_adjacency()
        mixing = defaultdict(int)
        for s, tgts in adj.items():
            ls = lang.get(s)
            for t in tgts:
                mixing[(ls, lang.get(t))] += 1
        triples = [(a, b, c) for (a, b), c in mixing.items()]
        return _newman_from_mixing(triples)


# ============================================================================
#  Helper condiviso: coefficiente di assortativita' di Newman
# ============================================================================
def _newman_from_mixing(triples):
    """triples: lista di (lingua_a, lingua_b, conteggio_archi).
    Calcola r = (Tr(e) - ||e^2||) / (1 - ||e^2||) sulla matrice di
    mescolamento normalizzata e (Newman 2003, assortativita' nominale)."""
    langs = sorted({a for a, _, _ in triples} | {b for _, b, _ in triples})
    idx = {l: i for i, l in enumerate(langs)}
    n = len(langs)
    e = [[0.0] * n for _ in range(n)]
    total = sum(c for _, _, c in triples)
    if total == 0:
        return {"assortativity": float("nan")}
    for a, b, c in triples:
        e[idx[a]][idx[b]] += c / total
    trace = sum(e[i][i] for i in range(n))
    ai = [sum(e[i]) for i in range(n)]
    bi = [sum(e[i][j] for i in range(n)) for j in range(n)]
    sum_ab = sum(ai[i] * bi[i] for i in range(n))
    r = (trace - sum_ab) / (1 - sum_ab) if (1 - sum_ab) != 0 else float("nan")
    return {"assortativity": r}
