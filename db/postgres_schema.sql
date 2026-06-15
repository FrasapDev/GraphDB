-- ============================================================================
-- PostgreSQL — Schema per Twitch Gamers Network
-- ============================================================================
-- Modello relazionale puro. Il grafo non diretto viene rappresentato con archi
-- DIREZIONALI duplicati (a,b) e (b,a). Questa e' una scelta deliberata:
--
--   * Senza duplicazione, ogni query di vicinato dovrebbe fare
--     WHERE src = x OR dst = x, che spezza l'uso degli indici B-tree
--     (un B-tree e' ordinato su una colonna; un OR su due colonne diverse
--     forza o due scansioni di indice + merge, o un seq scan).
--   * Con la duplicazione, "i vicini di x" = WHERE src = x, una sola
--     range-scan contigua sull'indice (src). E' il trucco standard per
--     simulare l'adjacency list in SQL. Costo: 2x lo storage degli archi
--     (13.6M righe invece di 6.8M).
--
-- Questo trade-off — pagare storage e scrittura per rendere il vicinato una
-- range-scan — e' esattamente cio' che Neo4j ottiene "gratis" con
-- l'index-free adjacency. Tenetelo a mente per l'esame.
-- ============================================================================

DROP TABLE IF EXISTS edges CASCADE;
DROP TABLE IF EXISTS nodes CASCADE;

-- ----------------------------------------------------------------------------
-- NODI
-- ----------------------------------------------------------------------------
CREATE TABLE nodes (
    id           INTEGER PRIMARY KEY,   -- numeric_id del dataset
    views        BIGINT,                -- numero di visualizzazioni canale
    mature       BOOLEAN,               -- contenuto per adulti
    life_time    INTEGER,               -- giorni tra primo e ultimo stream
    dead_account BOOLEAN,               -- account inattivo
    language     TEXT,                  -- lingua di broadcast (EN, DE, FR, ...)
    affiliate    BOOLEAN                -- stato affiliate
);
-- La PRIMARY KEY su id crea automaticamente un indice B-tree univoco:
-- e' la struttura che rende O(log N) il lookup di un singolo canale e
-- soprattutto il JOIN edges.dst -> nodes.id nella metrica di assortativita'.

-- Indice su language: la metrica 5 (assortativita') raggruppa e confronta
-- per lingua. Senza indice, ogni accesso "dammi la lingua del nodo k"
-- durante il join sarebbe gia' coperto dalla PK; l'indice su language serve
-- invece quando filtriamo/raggruppiamo PER lingua (GROUP BY language,
-- conteggi per categoria). Su 168k righe e' piccolo e velocizza i GROUP BY.
CREATE INDEX idx_nodes_language ON nodes (language);

-- ----------------------------------------------------------------------------
-- ARCHI (direzionali duplicati — vedi nota in testa)
-- ----------------------------------------------------------------------------
CREATE TABLE edges (
    src INTEGER NOT NULL,
    dst INTEGER NOT NULL
    -- NIENTE foreign key durante il bulk load: il vincolo FK verrebbe
    -- verificato riga per riga, rallentando l'import di 13.6M archi di un
    -- ordine di grandezza. Lo aggiungiamo (opzionalmente) DOPO il load,
    -- quando il check puo' essere fatto in batch. Vedi fondo file.
);

-- ----------------------------------------------------------------------------
-- INDICI sugli archi — il cuore delle performance di attraversamento
-- ----------------------------------------------------------------------------
-- (src): "dammi i vicini in uscita di X" = range scan contigua.
--        Usato in PageRank, BFS/betweenness, clustering, grado in uscita.
CREATE INDEX idx_edges_src ON edges (src);

-- (dst): "chi punta a X". Con archi duplicati e' ridondante per il grado,
--        ma resta utile al planner per alcune direzioni di join e per il
--        check FK batch. In un grafo diretto puro sarebbe indispensabile.
CREATE INDEX idx_edges_dst ON edges (dst);

-- Indice composito (src, dst): copre la verifica "esiste l'arco X->Y?"
-- senza toccare la heap table (index-only scan). E' decisivo nel
-- coefficiente di clustering, dove per ogni coppia di vicini di X dobbiamo
-- chiedere "sono connessi tra loro?" milioni di volte.
CREATE INDEX idx_edges_src_dst ON edges (src, dst);

-- ----------------------------------------------------------------------------
-- Post-load (eseguiti dalla pipeline DOPO l'import, non durante):
-- ----------------------------------------------------------------------------
-- ANALYZE aggiorna le statistiche del planner: senza, il query planner
-- stima male le cardinalita' e sceglie piani pessimi (es. nested loop
-- dove servirebbe un hash join). Su un benchmark e' obbligatorio.
--   ANALYZE nodes;
--   ANALYZE edges;
--
-- FK opzionale (commentata: aggiunge tempo all'import, la lasciamo come
-- documentazione del modello):
--   ALTER TABLE edges ADD CONSTRAINT fk_src FOREIGN KEY (src) REFERENCES nodes(id);
--   ALTER TABLE edges ADD CONSTRAINT fk_dst FOREIGN KEY (dst) REFERENCES nodes(id);
