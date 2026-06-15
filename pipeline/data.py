"""
data.py — Download, decompressione e normalizzazione del dataset.

Dataset: Twitch Gamers Network (Stanford SNAP)
  https://snap.stanford.edu/data/twitch_gamers.html
  168.114 nodi, 6.797.557 archi, NON diretto, 1 sola componente connessa,
  nessun attributo mancante.

Contenuto dello zip:
  large_twitch_edges.csv     -> colonne: numeric_id_1, numeric_id_2
  large_twitch_features.csv  -> colonne: views, mature, life_time, created_at,
                                updated_at, numeric_id, dead_account,
                                language, affiliate

Nota: i campi 'mature', 'dead_account', 'affiliate' sono 0/1 nel CSV e vanno
convertiti in booleani veri prima del load. 'created_at'/'updated_at' non
servono alle metriche e vengono scartati.
"""
from __future__ import annotations
import csv
import io
import os
import sys
import zipfile
from dataclasses import dataclass

DATA_URL = "https://snap.stanford.edu/data/twitch_gamers.zip"
EDGES_FILE = "large_twitch_edges.csv"
FEATURES_FILE = "large_twitch_features.csv"


@dataclass
class Dataset:
    data_dir: str
    sample_nodes: int | None = None  # se valorizzato, sottocampiona il grafo

    @property
    def edges_path(self) -> str:
        return os.path.join(self.data_dir, EDGES_FILE)

    @property
    def features_path(self) -> str:
        return os.path.join(self.data_dir, FEATURES_FILE)


def download_and_extract(data_dir: str) -> None:
    """Scarica lo zip da SNAP (se mancante) e lo decomprime."""
    os.makedirs(data_dir, exist_ok=True)
    edges = os.path.join(data_dir, EDGES_FILE)
    feats = os.path.join(data_dir, FEATURES_FILE)
    if os.path.exists(edges) and os.path.exists(feats):
        print(f"[data] file gia' presenti in {data_dir}, salto il download")
        return

    # requests e' opzionale; se manca usiamo urllib della stdlib.
    print(f"[data] scarico {DATA_URL} ...")
    try:
        import requests  # type: ignore
        resp = requests.get(DATA_URL, timeout=300)
        resp.raise_for_status()
        blob = resp.content
    except ImportError:
        from urllib.request import urlopen
        with urlopen(DATA_URL, timeout=300) as r:
            blob = r.read()

    print(f"[data] decomprimo ({len(blob)/1e6:.1f} MB) ...")
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        for member in z.namelist():
            name = os.path.basename(member)
            if name in (EDGES_FILE, FEATURES_FILE):
                with z.open(member) as src, open(os.path.join(data_dir, name), "wb") as dst:
                    dst.write(src.read())
    print("[data] pronto.")


def _to_bool(v: str) -> bool:
    """Converte i 0/1 (e varianti testuali) del CSV in booleano Python."""
    return str(v).strip().lower() in ("1", "true", "t", "yes")


def iter_nodes(ds: Dataset, keep: set[int] | None = None):
    """Genera tuple normalizzate dei nodi.
    keep: se fornito, restituisce solo i nodi con id in keep (sottocampione).
    """
    with open(ds.features_path, newline="") as f:
        for row in csv.DictReader(f):
            nid = int(row["numeric_id"])
            if keep is not None and nid not in keep:
                continue
            views = row.get("views", "")
            life = row.get("life_time", "")
            yield {
                "id": nid,
                "views": int(views) if views not in ("", None) else 0,
                "mature": _to_bool(row.get("mature", "0")),
                "life_time": int(life) if life not in ("", None) else 0,
                "dead_account": _to_bool(row.get("dead_account", "0")),
                "language": (row.get("language") or "UNK").strip(),
                "affiliate": _to_bool(row.get("affiliate", "0")),
            }


def iter_edges(ds: Dataset, keep: set[int] | None = None):
    """Genera coppie (a, b) NON ordinate-uniche cosi' come stanno nel file.
    Il file SNAP elenca ogni arco non diretto UNA volta.
    keep: se fornito, tiene solo archi con entrambi gli estremi nel sottoinsieme.
    """
    with open(ds.edges_path, newline="") as f:
        r = csv.reader(f)
        next(r)  # header
        for a, b in r:
            a, b = int(a), int(b)
            if keep is not None and (a not in keep or b not in keep):
                continue
            yield a, b


def select_sample(ds: Dataset) -> set[int] | None:
    """Se ds.sample_nodes e' impostato, ritorna l'insieme dei primi N id.
    Sottocampione deterministico per riproducibilita'. None = grafo intero.
    """
    if not ds.sample_nodes:
        return None
    ids = sorted(int(r["numeric_id"])
                 for r in csv.DictReader(open(ds.features_path, newline="")))
    chosen = set(ids[: ds.sample_nodes])
    print(f"[data] sottocampione di {len(chosen)} nodi (su {len(ids)})")
    return chosen


if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else "./data"
    download_and_extract(d)
    ds = Dataset(d)
    n = sum(1 for _ in iter_nodes(ds))
    e = sum(1 for _ in iter_edges(ds))
    print(f"nodi={n}  archi={e}  grado_medio={2*e/n:.2f}")
