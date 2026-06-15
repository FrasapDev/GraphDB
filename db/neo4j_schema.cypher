// ============================================================================
// Neo4j — Schema per Twitch Gamers Network
// ============================================================================
// Modello a grafo nativo. Un solo tipo di nodo (:Channel) e una sola
// relazione (:FOLLOWS). Il grafo originale e' NON diretto (follower mutui):
// in Neo4j memorizziamo UNA sola relazione per coppia e poi, nelle query e
// negli algoritmi GDS, la trattiamo come UNDIRECTED. Questo dimezza lo storage
// rispetto alla doppia direzione e sfrutta il fatto che ogni relazione in
// Neo4j e' comunque attraversabile in entrambi i sensi a costo identico
// (i record di relazione hanno puntatori al nodo di partenza E di arrivo).
//
// INDEX-FREE ADJACENCY (il concetto chiave da spiegare all'esame):
// in Neo4j ogni nodo memorizza fisicamente il puntatore alla testa della
// propria lista di relazioni; ogni relazione punta ai due nodi e alle
// relazioni precedente/successiva. "I vicini di X" si ottengono seguendo
// puntatori in memoria — NESSUN indice, NESSUNA ricerca, costo O(grado(X)).
// Gli indici qui sotto servono SOLO a TROVARE il nodo di partenza per id
// (il "punto di ingresso" nel grafo), non per attraversare.
// ============================================================================

// ----------------------------------------------------------------------------
// VINCOLO di unicita' su id -> crea anche un indice
// ----------------------------------------------------------------------------
// Il constraint garantisce l'unicita' e, come effetto collaterale, crea un
// indice range che rende O(log N) il lookup MATCH (c:Channel {id: $x}).
// E' il modo corretto: un constraint, non un indice "nudo", perche' id e'
// una chiave naturale.
CREATE CONSTRAINT channel_id_unique IF NOT EXISTS
FOR (c:Channel) REQUIRE c.id IS UNIQUE;

// ----------------------------------------------------------------------------
// INDICE su language
// ----------------------------------------------------------------------------
// Serve alla metrica 5 (assortativita') e a qualunque query che parta da
// "tutti i canali in lingua X". Non serve all'attraversamento.
CREATE INDEX channel_language IF NOT EXISTS
FOR (c:Channel) ON (c.language);

// ----------------------------------------------------------------------------
// CARICAMENTO
// ----------------------------------------------------------------------------
// Per il benchmark il caricamento avviene via `neo4j-admin database import`
// (offline, il piu' veloce: bypassa il transaction log e scrive direttamente
// gli store) OPPURE via driver Python a batch. La pipeline usa il secondo
// per uniformita' di misura con gli altri DB. Schema di riferimento dei nodi:
//
//   (:Channel {
//      id: int, views: int, mature: bool, life_time: int,
//      dead_account: bool, language: string, affiliate: bool
//   })
//
// e delle relazioni:
//
//   (:Channel)-[:FOLLOWS]->(:Channel)
//
// Esempio di load incrementale a batch via Cypher (usato dal driver):
//
//   UNWIND $rows AS row
//   MATCH (a:Channel {id: row.src})
//   MATCH (b:Channel {id: row.dst})
//   MERGE (a)-[:FOLLOWS]->(b);
//
// I MATCH sfruttano l'indice del constraint per agganciare i due estremi;
// il MERGE crea la relazione. Su 6.8M archi si batcha a ~10-50k per
// transazione per non far esplodere l'heap.
