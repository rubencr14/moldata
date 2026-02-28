# Moldata: Large-Scale Molecular Structure Dataset Manager

![Moldata Banner](./docs/moldata.png)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](#installation)
[![Tests](https://img.shields.io/badge/tests-89%20passed-green.svg)](#tests)

**Moldata** is an open-source tool for **downloading, storing, querying, and parsing** large-scale molecular structure datasets. It downloads bulk data from public archives (PDB, PDBBind, CrossDocking), uploads to MinIO/S3 with parallel transfers, retry, and resume support, and provides a SOLID-compliant parser framework that turns raw files into rich Python objects.

Target use case: materialize huge molecular datasets into object storage (`molfun-data/datasets/`) and query subsets by biological criteria (Pfam family, EC number, organism) for ML pipelines — designed as the **data layer** for [molfun](https://github.com/rubencr14/molfun).

---

## What problem does Moldata solve?

Training molecular ML models requires **hundreds of thousands of protein structures** organized, stored, and queryable by biological properties. Downloading the full PDB (~200K structures), uploading to object storage, and then selecting subsets for training is tedious, error-prone, and hard to resume when things fail.

Moldata handles all of this:

```python
from moldata.parsers import StructureDataset, CIFParser
from moldata.query import MinIOQuery

# Query human kinases from MinIO (only downloads what you need)
q = MinIOQuery("manifests/pdb.parquet")
paths = q.fetch_collection("kinases_human", max_structures=200, resolution_max=2.5)

# Parse into rich Structure objects
ds = StructureDataset.from_paths(paths)
for structure in ds:
    print(structure.entry_id, structure.resolution, structure.method)
    for chain in structure.chains:
        print(f"  Chain {chain.chain_id}: {chain.sequence[:60]}...")
```

Or download the entire PDB to MinIO in one command:

```bash
python examples/download_pdb.py
```

---

## Core Capabilities

### 1. Dataset Download & Upload

Three datasets are supported out of the box. Each supports parallel transfers, retry with exponential backoff, JSON-based checkpointing for resume, and automatic local cleanup after upload.

| Dataset | Download | Upload | Notes |
|---------|----------|--------|-------|
| **PDB** | rsync (EBI/RCSB port 33444), parallel HTTPS fallback | Raw files or tar shards | Full archive, yearly snapshots |
| **PDBBind** | Local (pre-download) | Parallel with retry | Requires license |
| **CrossDocking** | Official (Pitt server, auto-extract) or local | Parallel with retry | CrossDocked2020 v1.0/v1.3 |

**Upload strategy features:**

| Feature | Description |
|---------|-------------|
| Parallel uploads | `ThreadPoolExecutor` with configurable workers |
| Retry with backoff | Configurable `max_retries` and `retry_backoff` per file |
| Error isolation | Failed files don't abort the pipeline |
| Chunking | Batch processing with checkpoint flush after each batch |
| Resume | JSON checkpoints — interrupted uploads resume from where they stopped |
| Tar shards | Optional packing into `.tar` for fewer S3 objects |
| Auto-cleanup | Local staging deleted after successful upload (`--keep-local` to override) |

### 2. SOLID Structure Parsers

The parser framework follows all five SOLID principles. Every structure file becomes a rich `Structure` object with entities, chains, residues, and atoms.

```python
from moldata.parsers import CIFParser, PDBFormatParser, auto_parser

# Parse mmCIF
s = CIFParser().parse("4hhb.cif.gz")
s.entry_id          # "4HHB"
s.resolution         # 1.74
s.method             # "X-RAY DIFFRACTION"
s.entities           # [Entity(polymer), Entity(non-polymer), Entity(water)]
s.chains[0].sequence # "VLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTKTYFPHF..."
s.chains[0].residues[0].ca  # Atom(CA, x=6.204, y=16.869, z=4.854)
s.to_dict()          # flat dict for DataFrames

# Parse PDB format
s = PDBFormatParser().parse("4hhb.pdb")

# Auto-detect format
parser = auto_parser("structure.cif.gz")
s = parser.parse("structure.cif.gz")
```

**Object hierarchy:**

```
Structure (ABC)                    ← Abstract interface
├── CIFStructure                   ← mmCIF: .cif, .cif.gz
└── PDBStructure                   ← PDB:   .pdb, .ent, .ent.gz

Structure
├── metadata: StructureMetadata    ← entry_id, resolution, method, cell params...
├── entities: list[Entity]         ← polymer / non-polymer / water
├── chains: list[Chain]
│   └── residues: list[Residue]
│       └── atoms: list[Atom]      ← serial, name, element, x, y, z
└── atoms (flat view)
```

**SOLID principles applied:**

| Principle | Implementation |
|-----------|---------------|
| **Single Responsibility** | One parser per format: `CIFParser` → mmCIF, `PDBFormatParser` → PDB |
| **Open/Closed** | New formats registered via `register_parser()` without modifying existing code |
| **Liskov Substitution** | `CIFStructure` and `PDBStructure` are interchangeable via `Structure` |
| **Interface Segregation** | Small, focused types: `Atom`, `Residue`, `Chain`, `Entity`, `StructureMetadata` |
| **Dependency Inversion** | `StructureDataset` depends on `Structure` (abstraction), not concrete parsers |

### 3. StructureDataset

Load collections of structure files and iterate over parsed `Structure` objects:

```python
from moldata.parsers import StructureDataset

# From a directory
ds = StructureDataset.from_directory("/data/pdb/mmCIF", pattern="*.cif.gz")

# From explicit paths
ds = StructureDataset.from_paths(["1abc.cif.gz", "2xyz.pdb"])

# Iterate
for s in ds:
    print(s.entry_id, s.resolution)

# Filter
xray = ds.filter(lambda s: s.method and "X-RAY" in s.method)
high_res = ds.filter(lambda s: s.resolution and s.resolution < 2.0)

# Summary
print(ds.summary())
# {"total": 1234, "resolution_mean": 2.1, "methods": {"X-RAY DIFFRACTION": 1100, ...}}
```

### 4. Query from MinIO

`MinIOQuery` combines your manifest (parquet) with the RCSB Search API to select structures by biological criteria, then downloads only the matching files from MinIO:

```python
from moldata.query import MinIOQuery

q = MinIOQuery("manifests/pdb.parquet")

# By Pfam family
paths = q.fetch_by_family("PF00069", resolution_max=2.5, max_structures=200)

# By EC number
paths = q.fetch_by_ec("2.7.10", max_structures=100)

# By organism
paths = q.fetch_by_taxonomy(9606, max_structures=300)  # human

# By keyword
paths = q.fetch_by_keyword("nanobody", max_structures=100)

# Combined filters (AND logic)
paths = q.fetch_combined(pfam_id="PF00069", taxonomy_id=9606, resolution_max=2.0)

# Pre-defined collection (same definitions as molfun)
paths = q.fetch_collection("kinases_human", max_structures=150)

# Direct manifest filtering (no RCSB call, for enriched manifests)
paths = q.fetch_filtered(method="X-RAY", resolution_max=2.0, max_structures=50)
```

**20+ pre-defined collections** matching molfun's definitions:

| Collection | Filter | Default resolution |
|------------|--------|--------------------|
| `kinases` | Pfam PF00069 | 3.0 Å |
| `kinases_human` | PF00069 + human | 2.5 Å |
| `gpcr` | Pfam PF00001 | 3.5 Å |
| `serine_proteases` | Pfam PF00089 | 2.5 Å |
| `nanobodies` | Pfam PF07686 + keyword | 3.0 Å |
| `sars_cov2` | Taxonomy 2697049 | 3.5 Å |
| ... | (20+ total) | |

```bash
# List all collections from CLI
python examples/query_structures.py --list-collections
```

### 5. RCSB API Integration

Full client for the three RCSB APIs:

```python
from moldata.rcsb import RCSBClient

client = RCSBClient()

# REST Data API
entry = client.get_entry("4HHB")
entity = client.get_polymer_entity("4HHB", "1")

# GraphQL
result = client.graphql("""
    { entry(entry_id: "4HHB") {
        rcsb_id
        rcsb_entry_info { resolution_combined }
    }}
""")

# Search API
from moldata.query import search_ids
ids = search_ids(pfam_id="PF00069", resolution_max=2.5, max_results=500)
```

---

## Quick Start

### Installation

```bash
git clone https://github.com/rubencr14/moldata.git
cd moldata
pip install -e .
```

### Configure MinIO

Copy `.env.example` to `.env` and edit:

```bash
cp .env.example .env
```

```env
MINIO_ENDPOINT=localhost
MINIO_PORT=9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=molfun-data
```

The `.env` file is loaded automatically. If `MINIO_ENDPOINT` or `MINIO_ACCESS_KEY` is set, the storage backend auto-switches to S3 — no need to set `MOLDATA_STORAGE_BACKEND` manually.

### Download PDB to MinIO

```bash
# Default: rsync from RCSB, upload to MinIO, clean up local staging
python examples/download_pdb.py

# Keep local files after upload
python examples/download_pdb.py --keep-local

# HTTPS fallback
python examples/download_pdb.py --method https

# Yearly snapshot (reproducible)
python examples/download_pdb.py --snapshot-year 2024

# Upload as tar shards (fewer S3 objects)
python examples/download_pdb.py --upload-format tar_shards --tar-shard-size 1000

# Build enriched manifest with mmCIF metadata
python examples/download_pdb.py --enriched

# Upload only: skip download, upload existing staging to MinIO (resumable)
# Use --keep-local to preserve staging; you can resume later when download completes
python examples/download_pdb.py --upload-only --keep-local
```

### Resumable download and upload

Download and upload are fully resumable. You can stop (`Ctrl+C`) at any time and continue later:

1. **During download** — rsync and HTTPS both skip already-downloaded files. Re-run the script to continue.
2. **Upload partial** — Run `python examples/download_pdb.py --upload-only --keep-local` to upload what you have now, then re-run later when more files are downloaded. Already-uploaded objects are skipped via checkpoint and `HEAD` checks.

### Query Structures

```bash
# Fetch human kinases from MinIO
python examples/query_structures.py --collection kinases_human --max 200

# Fetch by Pfam family
python examples/query_structures.py --pfam PF00069 --resolution 2.5

# List all collections
python examples/query_structures.py --list-collections
```

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost` | MinIO host |
| `MINIO_PORT` | `9000` | MinIO port |
| `MINIO_ACCESS_KEY` | — | Access key |
| `MINIO_SECRET_KEY` | — | Secret key |
| `MINIO_BUCKET` | `molfun-data` | Bucket name |
| `MINIO_SECURE` | `false` | Use HTTPS |
| `MOLDATA_DATASETS_PREFIX` | `datasets/` | Prefix under bucket |
| `MOLDATA_UPLOAD_MAX_WORKERS` | `16` | Parallel upload workers |
| `MOLDATA_UPLOAD_BATCH_SIZE` | `500` | Batch size for chunking |
| `MOLDATA_CHECKPOINT_DIR` | `/tmp/moldata/checkpoints` | Resume checkpoints |

---

## CLI Usage

```bash
# PDB
moldata pdb prepare \
  --staging /data/staging/pdb \
  --manifest manifests/pdb.parquet \
  --source rcsb --pdb-format mmcif --method rsync

# PDBBind
moldata pdbbind prepare \
  --staging /data/staging/pdbbind \
  --manifest manifests/pdbbind.parquet

# CrossDocking
moldata crossdocking prepare \
  --staging /data/staging/crossdocking \
  --manifest manifests/crossdocking.parquet \
  --mode official

# Splits
moldata splits random \
  --manifest manifests/pdb.parquet \
  --out manifests/pdb_splits.parquet \
  --seed 42 --train 0.8 --val 0.1 --test 0.1
```

All commands support `--keep-local` to preserve local staging files after upload.

---

## Architecture

```
moldata/
├── moldata/
│   ├── cli.py                    # Typer CLI
│   ├── config.py                 # Environment-based config (MINIO_*, MOLDATA_*)
│   ├── core/
│   │   ├── storage.py            # LocalStorage, S3Storage (Protocol-based)
│   │   ├── upload_utils.py       # Parallel upload, retry, chunking, checkpoint
│   │   ├── download_utils.py     # Parallel download (HTTPS + S3), retry, checkpoint
│   │   ├── manifest.py           # Parquet manifests
│   │   ├── splits.py             # Random splits
│   │   └── logging_utils.py      # Logging
│   ├── datasets/
│   │   ├── base.py               # BaseDataset interface + auto-cleanup
│   │   ├── pdb.py                # PDB (rsync, HTTPS, snapshots, tar shards)
│   │   ├── pdbbind.py            # PDBBind
│   │   └── crossdocking.py       # CrossDocked2020 (auto-extract)
│   ├── parsers/
│   │   ├── base.py               # Abstract: Structure, Atom, Chain, Residue, Entity
│   │   ├── mmcif.py              # CIFStructure + CIFParser
│   │   ├── pdb_format.py         # PDBStructure + PDBFormatParser
│   │   ├── dataset.py            # StructureDataset + auto_parser + registry
│   │   └── enrich.py             # mmCIF + RCSB API enrichment
│   ├── query/
│   │   ├── minio_query.py        # MinIOQuery: manifest + RCSB Search → MinIO download
│   │   ├── collections.py        # 20+ pre-defined protein family collections
│   │   └── rcsb_search.py        # RCSB Search API v2 query builders
│   └── rcsb/
│       ├── client.py             # Data API REST, GraphQL, Search API
│       └── data_api.py           # Convenience wrappers
├── examples/
│   ├── download_pdb.py           # Full PDB → MinIO
│   ├── download_pdbbind.py       # PDBBind → MinIO
│   ├── download_crossdocking.py  # CrossDocked2020 → MinIO
│   ├── query_structures.py       # Query by family, EC, organism, keyword
│   └── enrich_manifest.py        # Enrich manifest with RCSB API
├── tests/                        # 89 tests
├── .env.example
├── pyproject.toml
├── LICENSE
└── README.md
```

---

## Tests

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

89 tests covering: storage, parallel upload (retry, checkpoint, broken storage), parallel download, datasets (PDB, PDBBind, CrossDocking), mmCIF parser, PDB parser, SOLID structure classes, StructureDataset, query module (collections, RCSB search builders, MinIOQuery), and RCSB API client.

---

## Design Notes

- **Retry with backoff**: Every upload/download retries N times with exponential delay
- **Error isolation**: Single file failures are logged but don't abort the pipeline
- **Checkpoint per batch**: Resume state is flushed after each batch, not just at the end
- **Auto-cleanup**: Local staging is deleted after successful upload by default
- **SOLID parsers**: Abstract `Structure` interface with concrete `CIFStructure` / `PDBStructure` implementations
- **Manifest-driven queries**: `MinIOQuery` intersects RCSB Search results with your manifest, so you only download what you have
- **Pluggable storage**: Local or S3/MinIO via a simple `Storage` protocol
- **Zero heavy deps**: Parsers are pure Python — no BioPython, no Gemmi

---

## Integration with Molfun

Moldata is the **data layer** for [molfun](https://github.com/rubencr14/molfun). The typical workflow:

```python
# 1. Download PDB to MinIO (once)
# python examples/download_pdb.py --enriched

# 2. Query a subset for training
from moldata.query import MinIOQuery
from moldata.parsers import StructureDataset

q = MinIOQuery("manifests/pdb.parquet")
paths = q.fetch_collection("kinases_human", max_structures=200)

# 3. Use with molfun's StructureDataset for training
from molfun.data.datasets.structure import StructureDataset as MolfunDataset
ds = MolfunDataset(pdb_paths=paths)
```

---

## License

MIT — see [LICENSE](./LICENSE).

## Citation

```bibtex
@software{moldata,
  title  = {Moldata: Large-Scale Molecular Structure Dataset Manager},
  author = {Rubén Cañadas},
  year   = {2026},
  url    = {https://github.com/rubencr14/moldata}
}
```
