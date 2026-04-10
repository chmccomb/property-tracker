"""
NJ MOD-IV Property Assessment Enrichment
------------------------------------------
Reads HudsonTaxList.dbf from the NJOGIS Hudson County parcels download,
filters to Jersey City (muni code 0906), and produces two CSVs that
jc_heights_clean.py joins to add assessed-value signals.

Output (written to the working directory, i.e. data/processed/):
    modiiv_block.csv  — block-level median assessed value (joined by block)
    modiiv_lot.csv    — parcel-level assessed value (joined by block+lot)

Run from data/processed/:
    python ../../scripts/enrich_modiiv.py

Or pass an explicit DBF path:
    python ../../scripts/enrich_modiiv.py --dbf /path/to/HudsonTaxList.dbf
"""

import argparse
import csv
import statistics
import struct
from pathlib import Path

ROOT        = Path(__file__).resolve().parent.parent
DEFAULT_DBF = ROOT / "data" / "raw" / "parcels_shp_dbf_Hudson" / "HudsonTaxList.dbf"
BLOCK_OUT   = Path("modiiv_block.csv")
LOT_OUT     = Path("modiiv_lot.csv")

# Hudson County muni codes (from HudsonTaxList.dbf GIS_PIN prefixes)
# 0905=Hoboken, 0906=Jersey City, 0910=Union City, 0911=Weehawken
MUNI_NAMES = {"0905": "Hoboken", "0906": "Jersey City", "0910": "Union City", "0911": "Weehawken"}
DEFAULT_MUNI = "0906"

# NJ property classes to include (residential + condo)
# Class 2 = residential (1–4 family), 4A = apartment, 4C = condo
RESIDENTIAL_CLASSES = {"2", "2A", "2B", "2C", "4A", "4B", "4C"}


# ── DBF reader (no external library) ─────────────────────────────────────────

def iter_dbf(path: Path, filter_fn=None):
    """Yield row dicts from a DBF file, seeking to the correct data offset."""
    with open(path, "rb") as f:
        f.read(4)                                       # version + date
        num_records  = struct.unpack("<I", f.read(4))[0]
        header_size  = struct.unpack("<H", f.read(2))[0]
        record_size  = struct.unpack("<H", f.read(2))[0]
        f.read(20)                                      # reserved
        fields = []
        while f.tell() < header_size - 1:
            fd = f.read(32)
            if not fd or fd[0] == 0x0D:
                break
            name   = fd[:11].replace(b"\x00", b"").decode("ascii", errors="replace")
            ftype  = chr(fd[11])
            length = fd[16]
            fields.append((name, ftype, length))
        f.seek(header_size)
        for _ in range(num_records):
            rec = f.read(record_size)
            if not rec or len(rec) < record_size:
                break
            if rec[0] == 0x2A:      # deleted record marker
                continue
            offset = 1
            row = {}
            for name, _ftype, length in fields:
                row[name] = rec[offset:offset + length].decode("latin-1", errors="replace").strip()
                offset += length
            if filter_fn is None or filter_fn(row):
                yield row


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm(s: str) -> str:
    """Strip leading zeros: '05101' → '5101', '054' → '54', '0' → '0'."""
    try:
        return str(int(s))
    except (ValueError, TypeError):
        return (s or "").lstrip("0") or "0"


def parse_int(s: str) -> int:
    try:
        return int(float(s or "0"))
    except (ValueError, TypeError):
        return 0


def fmt_deed_date(s: str) -> str:
    """Convert YYMMDD → YYYY-MM-DD."""
    if s and len(s) == 6 and s.isdigit():
        yy = int(s[:2])
        yyyy = 2000 + yy if yy < 50 else 1900 + yy
        return f"{yyyy}-{s[2:4]}-{s[4:6]}"
    return ""


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Build MOD-IV assessment CSVs for MLS pipeline")
    parser.add_argument("--dbf", type=Path, default=DEFAULT_DBF,
                        help=f"Path to HudsonTaxList.dbf (default: {DEFAULT_DBF})")
    parser.add_argument("--muni", default=DEFAULT_MUNI,
                        help=f"4-digit Hudson County muni code (default: {DEFAULT_MUNI} = Jersey City; "
                             f"use 0910 for Hoboken)")
    args = parser.parse_args()

    muni_prefix = args.muni
    muni_name   = MUNI_NAMES.get(muni_prefix, f"muni {muni_prefix}")

    if not args.dbf.exists():
        print(f"ERROR: DBF not found: {args.dbf}")
        print("Download Hudson County parcels from NJOGIS:")
        print("  https://njogis-newjersey.opendata.arcgis.com/documents/884f1f948dcb4353aa0fe52cdb6bc9f5")
        return

    print(f"Reading {args.dbf.name} ({muni_name} parcels only) …")
    parcels = list(iter_dbf(args.dbf, filter_fn=lambda r: r.get("GIS_PIN", "").startswith(muni_prefix)))
    print(f"  Total {muni_name} parcels:  {len(parcels):,}")

    res = [p for p in parcels if p.get("PROP_CLASS", "").strip() in RESIDENTIAL_CLASSES]
    print(f"  Residential/condo:      {len(res):,}")

    # ── Lot-level rows ────────────────────────────────────────────────────────
    lot_rows = []
    block_vals: dict[str, list[int]] = {}     # block_norm → [net_value, …]

    for p in res:
        block = norm(p.get("BLOCK", ""))
        lot   = norm(p.get("LOT",   ""))
        qual  = p.get("QUALIFIER", "").strip()
        net   = parse_int(p.get("NET_VALUE", "0"))
        land  = parse_int(p.get("LAND_VAL",  "0"))
        impr  = parse_int(p.get("IMPRVT_VAL","0"))

        if net > 0:
            block_vals.setdefault(block, []).append(net)

        lot_rows.append({
            "block":      block,
            "lot":        lot,
            "qualifier":  qual,
            "prop_class": p.get("PROP_CLASS", "").strip(),
            "prop_loc":   p.get("PROP_LOC",   "").strip(),
            "net_value":  net,
            "land_val":   land,
            "impr_val":   impr,
            "last_sale":  parse_int(p.get("SALE_PRICE","0")),
            "deed_date":  fmt_deed_date(p.get("DEED_DATE", "")),
            "yr_built":   p.get("YR_CONSTR", "").strip(),
        })

    lot_fields = ["block","lot","qualifier","prop_class","prop_loc",
                  "net_value","land_val","impr_val","last_sale","deed_date","yr_built"]
    with open(LOT_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=lot_fields)
        writer.writeheader()
        writer.writerows(lot_rows)
    print(f"  Wrote {len(lot_rows):,} parcel rows → {LOT_OUT}")

    # ── Block-level aggregates ────────────────────────────────────────────────
    block_rows = []
    for block, vals in sorted(block_vals.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
        v = [x for x in vals if x > 0]
        if not v:
            continue
        block_rows.append({
            "block":             block,
            "n_parcels":         len(v),
            "median_net_value":  int(statistics.median(v)),
            "mean_net_value":    int(statistics.mean(v)),
            "p25_net_value":     int(statistics.quantiles(v, n=4)[0]) if len(v) >= 4 else int(min(v)),
            "p75_net_value":     int(statistics.quantiles(v, n=4)[2]) if len(v) >= 4 else int(max(v)),
        })

    block_fields = ["block","n_parcels","median_net_value","mean_net_value",
                    "p25_net_value","p75_net_value"]
    with open(BLOCK_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=block_fields)
        writer.writeheader()
        writer.writerows(block_rows)
    print(f"  Wrote {len(block_rows):,} block rows → {BLOCK_OUT}")

    # ── Summary ───────────────────────────────────────────────────────────────
    all_vals = [v for vals in block_vals.values() for v in vals]
    if all_vals:
        print(f"\nAssessed value distribution (JC residential):")
        print(f"  Median: ${statistics.median(all_vals):>10,.0f}")
        print(f"  Mean:   ${statistics.mean(all_vals):>10,.0f}")
        buckets = [200_000, 400_000, 600_000, 800_000]
        labels  = ["<$200k","$200-400k","$400-600k","$600-800k",">$800k"]
        counts  = [0] * len(labels)
        for v in all_vals:
            for i, cut in enumerate(buckets):
                if v < cut:
                    counts[i] += 1
                    break
            else:
                counts[-1] += 1
        for label, count in zip(labels, counts):
            print(f"  {label:>12}: {count:,}")


if __name__ == "__main__":
    main()
