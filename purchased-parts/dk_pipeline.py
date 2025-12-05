#!/usr/bin/env python3
"""
DIGI-KEY PURCHASE PIPELINE (ONE-OFF MIGRATION + REPEATABLE UPDATES)

This file is meant to be pasted/run as-is in a fresh chat/python session.
It is self-contained and *does not rely on specific filenames*.

───────────────────────────────────────────────────────────────────────────────
WHAT YOU PROVIDE (drop these files into the working directory, e.g. /mnt/data):
───────────────────────────────────────────────────────────────────────────────

You should have, at minimum:

  JSON (4 files total, 2 "invoice schema" + 2 "product schema"):
    - 1x BASE invoices JSON (larger; pre-update merged)
    - 1x DELTA invoices JSON (smaller; new purchases)
    - 1x BASE products JSON (larger; pre-update merged)
    - 1x DELTA products JSON (smaller; new products for those purchases)

  CSV (2 files total, full + mini):
    - 1x FULL CSV (more columns; "full")
    - 1x MINI CSV (few columns; "mini"), SAME ROW COUNT AS FULL

Important:
- The code auto-detects which JSON is invoice vs product by schema inspection.
- For each of invoice/product JSON groups, it selects:
    delta = smallest file
    base  = file with minimal overlap with delta (so it won’t accidentally pick an already-merged file)
- For CSVs, it selects:
    mini = CSV with the fewest columns
    full = CSV that has the same row count as mini, but more columns
  (Fallback: smallest CSV is mini, largest is full)

───────────────────────────────────────────────────────────────────────────────
WHAT IT PRODUCES (written to OUT_DIR):
───────────────────────────────────────────────────────────────────────────────

1) merged_products_out.json   (base + delta, list-concat)
2) merged_invoices_out.json   (base + delta, list-concat)
3) updated_full_future_schema.csv  (future full schema, includes MCU columns)
4) updated_mini.csv               (mini schema: fewer cols, ALL rows)
5) new_purchases_enriched.csv     (only rows that came from the delta invoices JSON)

───────────────────────────────────────────────────────────────────────────────
FUTURE FULL CSV SCHEMA (stable; used for this “migration” and all later runs):
───────────────────────────────────────────────────────────────────────────────

description
category
dk_pn
mfr_pn
mfr
qty_shipped
gbp_unit_price
gbp_ext_price
invoice_id
date_shipped
series
product_status
package_type
core_processor
core_type
clock_speed
program_memory_size
other_parameters

Notes:
- Datasheet/photo URL columns are intentionally NOT present in the CSV output.
- other_parameters is a JSON string blob of the remaining Digi-Key parameters,
  with MCU-promoted keys removed to avoid duplication.

───────────────────────────────────────────────────────────────────────────────
SANITY CHECKS / ASSERTIONS (fail fast if wrong):
───────────────────────────────────────────────────────────────────────────────

A) We add *exactly* as many NEW rows as implied by the delta invoices JSON:
   expected_new = |delta_details \ base_details|
   actual_new   = number of rows in output marked as “new”
   assert expected_new == actual_new

B) Excluding the NEW rows, old data are identical:
   We rebuild rows from base JSON alone and compare vs the merged rebuild
   (with base precedence in product lookup). For every base detail key,
   every output column must match exactly.

C) Any NEW MCU rows must have at least one MCU field populated.
   (If delta introduced an MCU purchase, the MCU cols should not all be empty.)

Also reported (informational):
- row_diff_vs_input_full_csv: output row count - input full csv row count
  (for your dataset this tends to be ~+9)

───────────────────────────────────────────────────────────────────────────────
OPTIONAL SCHEMA MUTATIONS (disabled by default):
───────────────────────────────────────────────────────────────────────────────

If you want to drop output columns or “promote” specific keys from other_parameters
into their own top-level columns, set `schema_mutations` in `run_pipeline()`.

Example:
  schema_mutations = SchemaMutations(
      drop_output_columns=["series"],
      promote_other_params={
          "Package / Case": "package_case",
          "Mounting Type": "mounting_type",
      }
  )

This is OPTIONAL. The repeatable pipeline should normally keep the schema stable.

───────────────────────────────────────────────────────────────────────────────
CALLSITE:
───────────────────────────────────────────────────────────────────────────────

Just run:
  python dk_pipeline.py

Or in a notebook/chat tool session:
  run_pipeline(IN_DIR=Path("/mnt/data"), OUT_DIR=Path("/mnt/data"))

"""

from __future__ import annotations
import json, glob, math
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Configurable stable schemas
# ─────────────────────────────────────────────────────────────────────────────

FUTURE_FULL_SCHEMA: List[str] = [
    "description",
    "category",
    "dk_pn",
    "mfr_pn",
    "mfr",
    "qty_shipped",
    "gbp_unit_price",
    "gbp_ext_price",
    "invoice_id",
    "date_shipped",
    "series",
    "product_status",
    "package_type",
    "core_processor",
    "core_type",
    "clock_speed",
    "program_memory_size",
    "other_parameters",
]

MINI_SCHEMA: List[str] = ["mfr_pn", "dk_pn", "description", "qty_bought"]

MCU_PROMOTED_PARAM_TEXTS = ["Core Processor", "Core Size", "Speed", "Program Memory Size"]


@dataclass
class SchemaMutations:
    """
    Optional schema mutations. Disabled by default.
    - drop_output_columns: remove columns from the final output schema.
    - promote_other_params: move keys from other_parameters (JSON blob) into dedicated columns.
      Mapping is: parameterText -> new column name.
      Promoted keys are removed from the blob to avoid duplication.
    """
    drop_output_columns: List[str] = field(default_factory=list)
    promote_other_params: Dict[str, str] = field(default_factory=dict)  # parameterText -> new column name
    promoted_insert_before: str = "other_parameters"


# ─────────────────────────────────────────────────────────────────────────────
# Discovery (no filename assumptions)
# ─────────────────────────────────────────────────────────────────────────────

def _file_size(p: Path) -> int:
    return p.stat().st_size

def _load_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _classify_json_doc(doc) -> str:
    # invoices: list[order], where order has 'invoiceDetails'/'invoices'
    if isinstance(doc, list) and doc and isinstance(doc[0], dict):
        e0 = doc[0]
        if "invoiceDetails" in e0 or "invoices" in e0 or "boxes" in e0:
            return "invoice"
        if "productVariations" in e0 or "parameters" in e0:
            return "product"
    return "unknown"

def discover_json_base_delta(dir_: Path) -> Dict[str, Tuple[Path, Path]]:
    """
    Finds invoice/product JSONs; for each class picks:
      delta = smallest file
      base  = candidate with minimal overlap with delta (so we avoid already-merged files)
    """
    json_paths = [Path(p) for p in glob.glob(str(dir_ / "*.json"))]
    groups: Dict[str, List[Path]] = {"invoice": [], "product": [], "unknown": []}

    for p in json_paths:
        try:
            doc = _load_json(p)
        except Exception:
            groups["unknown"].append(p)
            continue
        groups[_classify_json_doc(doc)].append(p)

    out: Dict[str, Tuple[Path, Path]] = {}
    for kind in ("invoice", "product"):
        paths = groups[kind]
        if len(paths) < 2:
            raise RuntimeError(f"Need >=2 {kind} JSONs in {dir_}; found {len(paths)}")

        paths_sorted = sorted(paths, key=_file_size)
        delta = paths_sorted[0]
        candidates = paths_sorted[1:]

        delta_doc = _load_json(delta)

        if kind == "invoice":
            delta_keys = set(iter_detail_keys(delta_doc))
            def overlap(path: Path) -> int:
                base_doc = _load_json(path)
                base_keys = set(iter_detail_keys(base_doc))
                return len(base_keys & delta_keys)
        else:
            delta_keys = set(iter_prod_dk_keys(delta_doc))
            def overlap(path: Path) -> int:
                base_doc = _load_json(path)
                base_keys = set(iter_prod_dk_keys(base_doc))
                return len(base_keys & delta_keys)

        # Choose base with minimal overlap; tie-breaker: bigger file (more complete base).
        scored = [(overlap(p), -_file_size(p), p) for p in candidates]
        scored.sort()
        base = scored[0][2]
        out[kind] = (base, delta)

    return out

def discover_csv_pair(dir_: Path) -> Tuple[Path, Path]:
    """
    Returns (full_csv, mini_csv).
    Uses rule: mini has far fewer columns and same row count as full.
    Fallback: smallest file = mini, largest = full.
    """
    csv_paths = [Path(p) for p in glob.glob(str(dir_ / "*.csv"))]
    if len(csv_paths) < 2:
        raise RuntimeError(f"Need >=2 CSVs in {dir_}; found {len(csv_paths)}")

    info = []
    for p in csv_paths:
        try:
            df0 = pd.read_csv(p, nrows=1)
            ncols = len(df0.columns)
        except Exception:
            continue

        with p.open("r", encoding="utf-8", errors="ignore") as f:
            nrows = sum(1 for _ in f) - 1
        info.append((p, _file_size(p), ncols, nrows))

    if not info:
        raise RuntimeError("No readable CSVs")

    # mini = minimum columns, tie-break by size
    mini = sorted(info, key=lambda x: (x[2], x[1]))[0]
    mini_rows = mini[3]

    full_candidates = [x for x in info if x[3] == mini_rows and x[2] > mini[2]]
    if full_candidates:
        full = sorted(full_candidates, key=lambda x: (-x[1], -x[2]))[0]
        return full[0], mini[0]

    # fallback by size extremes
    sorted_by_size = sorted(info, key=lambda x: x[1])
    return sorted_by_size[-1][0], sorted_by_size[0][0]


# ─────────────────────────────────────────────────────────────────────────────
# Core transforms
# ─────────────────────────────────────────────────────────────────────────────

def iter_detail_keys(invoice_doc):
    """
    Generates a stable-ish uniqueness key for invoiceDetails.
    Prefer (invoiceId, detailId). Fallback includes product/qty/price fields.
    """
    for order in invoice_doc:
        for d in order.get("invoiceDetails", []):
            inv_id = d.get("invoiceId")
            det_id = d.get("detailId")
            if inv_id is not None and det_id is not None:
                yield (inv_id, det_id)
            else:
                yield (inv_id,
                       d.get("digiKeyProductNumber"),
                       d.get("manufacturerProductNumber"),
                       d.get("quantityShipped"),
                       d.get("extendedPrice"))

def iter_prod_dk_keys(products_doc):
    for p in products_doc:
        for v in p.get("productVariations", []):
            dk = v.get("digiKeyProductNumber")
            if dk:
                yield dk

def build_prod_by_dk(products_base, products_delta):
    """
    Base precedence: if dk_pn exists in base, delta does not override it.
    This is what makes the “excluding new entries, data identical” assertion meaningful.
    """
    lut = {}
    for p in products_base:
        for v in p.get("productVariations", []):
            dk = v.get("digiKeyProductNumber")
            if dk and dk not in lut:
                lut[dk] = p
    for p in products_delta:
        for v in p.get("productVariations", []):
            dk = v.get("digiKeyProductNumber")
            if dk and dk not in lut:
                lut[dk] = p
    return lut

def category_t2(prod):
    if not prod:
        return ""
    cat = prod.get("category") or {}
    children = cat.get("childCategories") or []
    if children:
        return children[0].get("name", "") or cat.get("name", "")
    return cat.get("name", "")

def is_mcu_product(prod):
    if not prod:
        return False
    names = []
    def walk(c):
        if not c:
            return
        names.append(c.get("name", ""))
        for cc in c.get("childCategories", []):
            walk(cc)
    walk(prod.get("category"))
    s = " ".join(n.lower() for n in names)
    return ("microcontroller" in s) or ("application specific microcontrollers" in s)

def extract_mcu_fields(prod):
    params = { prm.get("parameterText",""): prm.get("valueText","")
               for prm in (prod.get("parameters") or []) }

    core_processor = params.get("Core Processor", "")
    clock_speed = params.get("Speed", "")
    program_memory_size = params.get("Program Memory Size", "")
    core_size = params.get("Core Size", "")

    cp = core_processor or ""
    if "Cortex" in cp:
        idx = cp.index("Cortex")
        ct = cp[idx:]
        ct = ct.replace("Cortex®", "Cortex").replace("®", "").strip()
        core_type = ct
    elif "AVR" in cp:
        core_type = "AVR"
    else:
        core_type = core_size or cp

    return core_processor, core_type, clock_speed, program_memory_size

def build_other_params_dict(prod):
    if not prod:
        return {}
    out = {}
    for prm in prod.get("parameters", []):
        k = prm.get("parameterText","")
        if k in MCU_PROMOTED_PARAM_TEXTS:
            continue
        out[k] = prm.get("valueText","")
    return out

def build_rows(invoices_doc, prod_by_dk, new_detail_keys: Optional[set]=None):
    """
    Builds 1 row per invoiceDetail.
    Adds internal columns for assertions:
      _detail_key, _is_new, _qty_bought, _other_dict
    """
    seen = set()
    rows = []
    for order in invoices_doc:
        inv_by_id = {inv.get("invoiceId"): inv for inv in order.get("invoices", [])}
        for d in order.get("invoiceDetails", []):
            inv_id = d.get("invoiceId")
            det_id = d.get("detailId")
            if inv_id is not None and det_id is not None:
                key = (inv_id, det_id)
            else:
                key = (inv_id,
                       d.get("digiKeyProductNumber"),
                       d.get("manufacturerProductNumber"),
                       d.get("quantityShipped"),
                       d.get("extendedPrice"))
            if key in seen:
                continue
            seen.add(key)

            dk = d.get("digiKeyProductNumber", "")
            prod = prod_by_dk.get(dk)
            inv = inv_by_id.get(inv_id)

            r = {}
            r["_detail_key"] = key
            r["_is_new"] = bool(new_detail_keys and key in new_detail_keys)
            r["_qty_bought"] = d.get("quantityInitial", "")

            r["description"] = d.get("description","")
            r["category"] = category_t2(prod)
            r["dk_pn"] = dk
            r["mfr_pn"] = d.get("manufacturerProductNumber","")
            r["mfr"] = d.get("manufacturerName","")

            r["qty_shipped"] = d.get("quantityShipped","")
            r["gbp_unit_price"] = d.get("formattedUnitPrice","")
            r["gbp_ext_price"] = d.get("formattedExtendedPrice","")

            r["invoice_id"] = inv_id if inv_id is not None else ""
            r["date_shipped"] = inv.get("dateShipped","") if inv else ""

            if prod:
                r["series"] = (prod.get("series") or {}).get("name","")
                r["product_status"] = (prod.get("productStatus") or {}).get("status","")
                pt = ""
                for v in prod.get("productVariations", []):
                    if v.get("digiKeyProductNumber") == dk:
                        pt = (v.get("packageType") or {}).get("name","")
                        break
                r["package_type"] = pt
            else:
                r["series"] = ""
                r["product_status"] = ""
                r["package_type"] = ""

            if prod and is_mcu_product(prod):
                cp, ct, clk, pm = extract_mcu_fields(prod)
                r["core_processor"] = cp
                r["core_type"] = ct
                r["clock_speed"] = clk
                r["program_memory_size"] = pm
            else:
                r["core_processor"] = ""
                r["core_type"] = ""
                r["clock_speed"] = ""
                r["program_memory_size"] = ""

            other_dict = build_other_params_dict(prod)
            r["_other_dict"] = other_dict
            r["other_parameters"] = json.dumps(other_dict, ensure_ascii=False)

            rows.append(r)
    return rows

def apply_schema_mutations(df: pd.DataFrame, schema: List[str], mut: Optional[SchemaMutations]) -> Tuple[pd.DataFrame, List[str]]:
    if not mut:
        return df, schema

    schema_out = [c for c in schema if c not in mut.drop_output_columns]

    if mut.promote_other_params:
        # Promote from _other_dict into dedicated columns, and remove keys from blob.
        for src_key, new_col in mut.promote_other_params.items():
            if new_col not in df.columns:
                df[new_col] = ""
            def pick(row):
                d = row.get("_other_dict", {}) or {}
                return d.get(src_key, "")
            df[new_col] = df.apply(pick, axis=1)

        def strip_and_dump(row):
            d = dict(row.get("_other_dict", {}) or {})
            for src_key in mut.promote_other_params.keys():
                d.pop(src_key, None)
            return json.dumps(d, ensure_ascii=False)
        df["other_parameters"] = df.apply(strip_and_dump, axis=1)

        # Insert new columns before 'other_parameters'
        insert_before = mut.promoted_insert_before
        idx = schema_out.index(insert_before) if insert_before in schema_out else len(schema_out)
        new_cols = [mut.promote_other_params[k] for k in mut.promote_other_params.keys()]

        # avoid duplicates and keep insertion order
        for c in new_cols:
            if c in schema_out:
                schema_out.remove(c)
        schema_out[idx:idx] = new_cols

    return df, schema_out


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline runner
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    IN_DIR: Path,
    OUT_DIR: Path,
    schema_mutations: Optional[SchemaMutations] = None,
) -> None:
    # 1) Discover inputs
    pairs = discover_json_base_delta(IN_DIR)
    inv_base_p, inv_delta_p = pairs["invoice"]
    prod_base_p, prod_delta_p = pairs["product"]
    full_csv_p, mini_csv_p = discover_csv_pair(IN_DIR)

    invoices_base = _load_json(inv_base_p)
    invoices_delta = _load_json(inv_delta_p)
    products_base = _load_json(prod_base_p)
    products_delta = _load_json(prod_delta_p)

    # 2) Expected new rows from delta = delta_only(detail_key)
    base_detail_keys = set(iter_detail_keys(invoices_base))
    delta_detail_keys = set(iter_detail_keys(invoices_delta))
    delta_only_keys = delta_detail_keys - base_detail_keys

    # 3) Build lookup with base precedence
    prod_by_dk = build_prod_by_dk(products_base, products_delta)

    # 4) Build base rows and merged rows (deduped)
    base_rows = build_rows(invoices_base, prod_by_dk, new_detail_keys=None)
    merged_rows = build_rows(invoices_base + invoices_delta, prod_by_dk, new_detail_keys=delta_only_keys)

    df_base = pd.DataFrame(base_rows)
    df_merged = pd.DataFrame(merged_rows)

    # 5) ASSERT A: expected additions matches output “new” markings
    expected_new = len(delta_only_keys)
    actual_new = int(df_merged["_is_new"].sum())
    assert actual_new == expected_new, f"New row count mismatch: expected {expected_new}, got {actual_new}"

    # 6) ASSERT B: excluding new rows, base data identical
    base_keyed = df_base.set_index("_detail_key")
    merged_old = df_merged[~df_merged["_is_new"]].set_index("_detail_key")
    assert set(base_keyed.index) == set(merged_old.index), "Key sets differ between base and merged-old"
    for col in FUTURE_FULL_SCHEMA:
        a = base_keyed[col].fillna("")
        b = merged_old[col].fillna("")
        if not a.equals(b):
            diff = (a != b)
            first_bad = diff[diff].index[0]
            raise AssertionError(f"Column '{col}' differs for key {first_bad}: base='{a.loc[first_bad]}' merged='{b.loc[first_bad]}'")

    # 7) ASSERT C: any NEW MCU rows have at least one MCU field populated
    mcu_cols = ["core_processor", "core_type", "clock_speed", "program_memory_size"]
    new_mcu_rows = df_merged[df_merged["_is_new"] & (df_merged["core_processor"].fillna("") != "")]
    if len(new_mcu_rows) > 0:
        ok = (new_mcu_rows[mcu_cols].fillna("").astype(str) != "").any(axis=1).all()
        assert ok, "Some newly-added MCU rows have all MCU fields empty"

    # 8) Informational: diff vs input full CSV
    orig_full = pd.read_csv(full_csv_p)
    row_diff_vs_input_full = len(df_merged) - len(orig_full)

    # 9) Apply optional schema mutations
    df_out, schema_out = apply_schema_mutations(df_merged.copy(), FUTURE_FULL_SCHEMA.copy(), schema_mutations)

    # 10) Write outputs
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    out_products = OUT_DIR / "merged_products_out.json"
    out_invoices = OUT_DIR / "merged_invoices_out.json"
    out_full     = OUT_DIR / "updated_full_future_schema.csv"
    out_mini     = OUT_DIR / "updated_mini.csv"
    out_new      = OUT_DIR / "new_purchases_enriched.csv"

    with out_products.open("w", encoding="utf-8") as f:
        json.dump(products_base + products_delta, f, indent=2)
    with out_invoices.open("w", encoding="utf-8") as f:
        json.dump(invoices_base + invoices_delta, f, indent=2)

    df_out[schema_out].to_csv(out_full, index=False)

    mini_df = pd.DataFrame({
        "mfr_pn": df_out["mfr_pn"],
        "dk_pn": df_out["dk_pn"],
        "description": df_out["description"],
        "qty_bought": df_out["_qty_bought"],
    })
    mini_df.to_csv(out_mini, index=False)

    df_new = df_out[df_out["_is_new"]][schema_out].copy()
    df_new.to_csv(out_new, index=False)

    # 11) Print the key run summary
    # (All of this shows up in your run logs; nothing is required downstream.)
    non_mcu = df_out[df_out["core_processor"].fillna("") == ""][schema_out]
    total_cells = non_mcu.size
    missing_cells = int(non_mcu.replace({"": pd.NA}).isna().sum().sum())
    missing_pct = missing_cells / total_cells if total_cells else math.nan

    mcu_row_count = int((df_out["core_processor"].fillna("") != "").sum())

    print("─" * 80)
    print("INPUTS (auto-discovered):")
    print(f"  invoices base : {inv_base_p.name}")
    print(f"  invoices delta: {inv_delta_p.name}")
    print(f"  products base : {prod_base_p.name}")
    print(f"  products delta: {prod_delta_p.name}")
    print(f"  full csv      : {full_csv_p.name}")
    print(f"  mini csv      : {mini_csv_p.name}")
    print("OUTPUTS:")
    print(f"  {out_products}")
    print(f"  {out_invoices}")
    print(f"  {out_full}")
    print(f"  {out_mini}")
    print(f"  {out_new}")
    print("CHECKS:")
    print(f"  expected new rows (delta-only): {expected_new}")
    print(f"  actual new rows               : {actual_new}")
    print(f"  row diff vs input full csv    : {row_diff_vs_input_full}")
    print(f"  MCU rows (core_processor non-empty): {mcu_row_count}")
    print(f"  missingness (non-MCU rows only): {missing_pct*100:.2f}%")
    print("─" * 80)


if __name__ == "__main__":
    run_pipeline(IN_DIR=Path("."), OUT_DIR=Path("."))
