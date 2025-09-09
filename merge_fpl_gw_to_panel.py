
#!/usr/bin/env python3
"""
Merge per-player, per-GW CSV files (stored in player subfolders) into one panel CSV.

Usage (examples):
  python merge_fpl_gw_to_panel.py --root "C:/path/to/players_root" --output "C:/path/to/players_2526_panel.csv"
  python merge_fpl_gw_to_panel.py --root "/Users/you/fpl/players" --output "/Users/you/fpl/players_2526_panel.csv" --demo "/path/to/players_2526_panel_demo.csv"

Assumptions (robust to variations):
- Your folder layout looks like:
    ROOT/
      123_Salah/
        gw1.csv
        gw2.csv
      456_Haaland/
        gw1.csv
        gw2.csv
  (Any folder/file names are fine; we try to sniff IDs/names from inside the CSV first, then from paths.)

- Each CSV ideally has columns like:
    - element (player id)      -> if missing, we try to parse from folder or filename
    - gw / round / event       -> GW number; if only 'round' or 'event' present, we rename to 'gw'
    - full_name (optional)     -> if missing, try to compose from first/second name or folder name

- We union all columns across files, then (optionally) align to the columns of a provided demo CSV
  so the final CSV matches your target schema. Missing columns are filled with NaN.
"""

import argparse
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def _safe_read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV robustly with fallback encodings and common quirks handled."""
    try:
        df = pd.read_csv(path, low_memory=False)
        return df
    except UnicodeDecodeError:
        # Fallback to latin-1 if UTF-8 fails
        return pd.read_csv(path, encoding="latin-1", low_memory=False)


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case-ish columns; strip whitespace."""
    new_cols = (
        df.columns.str.strip()
                  .str.replace(r"\s+", "_", regex=True)
                  .str.replace(r"[^\w]+", "_", regex=True)
                  .str.lower()
    )
    df = df.copy()
    df.columns = new_cols
    # Common renames to agree on a target schema:
    renames = {
        "round": "gw",
        "event": "gw",
        "gameweek": "gw",
        "player_id": "element",
        "id": "element",  # Sometimes the player id is just "id"; prefer "element"
        "name": "full_name",
        "web_name": "full_name",
        "secondname": "second_name",
        "firstname": "first_name",
        "surname": "second_name",
        "last_name": "second_name",
    }
    for k, v in renames.items():
        if k in df.columns and v not in df.columns:
            df.rename(columns={k: v}, inplace=True)
    return df


def _sniff_element_and_name_from_path(path: Path) -> Tuple[Optional[int], Optional[str]]:
    """
    Try to extract {element, full_name} from folder or file names.
    We look for patterns like '123_Salah', '456-Haaland', etc.
    """
    # Combine parts of the parent folder and file stem to search for an integer id and a plausible name chunk
    candidates = []
    if path.parent and path.parent != path:
        candidates.append(path.parent.name)
    candidates.append(path.stem)

    # Try to find a numeric id in any candidate
    element = None
    for s in candidates:
        m = re.search(r"(?<!\d)(\d{1,9})(?!\d)", s)
        if m:
            try:
                element = int(m.group(1))
                break
            except ValueError:
                pass

    # Build a name from alpha parts, skipping generic tokens
    stop = {"gw", "gameweek", "round", "event", "csv"}
    name = None
    for s in candidates:
        parts = re.split(r"[_\-\s]+", s)
        parts = [p for p in parts if p.isalpha() and len(p) > 1 and p.lower() not in stop]
        if parts:
            name = " ".join(parts)
            break

    return element, name


def _ensure_element_and_gw(df: pd.DataFrame, path: Path) -> pd.DataFrame:
    """Make sure 'element' and 'gw' columns exist; infer from path/filename if missing."""
    df = df.copy()

    # Element (player id)
    if "element" not in df.columns:
        element_from_path, name_from_path = _sniff_element_and_name_from_path(path)
        if element_from_path is not None:
            df["element"] = element_from_path

        # If still missing, last resort: if exactly one unique id-like column exists
        if "element" not in df.columns:
            id_like = [c for c in df.columns if c.endswith("_id") or c == "id"]
            if len(id_like) == 1:
                df.rename(columns={id_like[0]: "element"}, inplace=True)

    # GW (gameweek)
    if "gw" not in df.columns:
        # Try to parse from filename: gw12, GW_05, gameweek_3, etc.
        m = re.search(r"(?:^|[_\-])(?:gw|gameweek|round|event)[_\-]?(\d{1,2})(?:[_\-]|$)", path.stem, flags=re.I)
        if m:
            df["gw"] = int(m.group(1))

    # 'gw' must be integer
    if "gw" in df.columns:
        # Coerce to int where possible
        df["gw"] = pd.to_numeric(df["gw"], errors="coerce").astype("Int64")

    return df


def _ensure_full_name(df: pd.DataFrame, fallback_name: Optional[str]) -> pd.DataFrame:
    """Create 'full_name' if missing, using first/second name or fallback from path."""
    df = df.copy()
    if "full_name" in df.columns:
        return df

    if "first_name" in df.columns or "second_name" in df.columns:
        first = df.get("first_name", pd.Series(index=df.index, dtype=object)).fillna("").astype(str).str.strip()
        second = df.get("second_name", pd.Series(index=df.index, dtype=object)).fillna("").astype(str).str.strip()
        df["full_name"] = (first + " " + second).str.replace(r"\s+", " ", regex=True).str.strip()
    elif fallback_name:
        bad = {"gw", "gameweek", "round", "event"}
        if fallback_name.strip().lower() not in bad:
            df["full_name"] = fallback_name.strip()
    return df


def merge_folder(root: str, demo_csv: Optional[str] = None, csv_glob: str = "**/*.csv") -> pd.DataFrame:
    """
    Walk the root folder, read all CSVs, and vertically concatenate into a panel DataFrame.
    If demo_csv is provided, align the output columns to match the demo.
    """
    root_path = Path(root)
    files = sorted(root_path.rglob(csv_glob))

    if not files:
        raise FileNotFoundError(f"No CSV files found under: {root} (pattern={csv_glob})")

    frames = []
    for f in files:
        try:
            df = _safe_read_csv(f)
        except Exception as e:
            print(f"[WARN] Skipping {f} (read error: {e})")
            continue

        df = _standardize_columns(df)

        # Infer element and gw if missing
        element_from_path, name_from_path = _sniff_element_and_name_from_path(f)
        df = _ensure_element_and_gw(df, f)

        # Ensure full_name, preferring CSV info, else folder-derived
        df = _ensure_full_name(df, name_from_path)

        # If still missing critical keys, skip
        if "element" not in df.columns:
            print(f"[WARN] {f} missing 'element' (player id); skipping")
            continue
        if "gw" not in df.columns:
            print(f"[WARN] {f} missing 'gw' (gameweek); skipping")
            continue

        frames.append(df)

    if not frames:
        raise RuntimeError("No valid CSVs with both 'element' and 'gw' were found.")

    panel = pd.concat(frames, ignore_index=True, sort=False)

    # Drop obvious duplicates if a file was read twice
    panel = panel.drop_duplicates(subset=["element", "gw"] + [c for c in ["fixture", "minutes", "bps"] if c in panel.columns],
                                  keep="last")

    # Align to demo schema if provided
    if demo_csv:
        demo_cols = list(pd.read_csv(demo_csv, nrows=1).columns)
        for col in demo_cols:
            if col not in panel.columns:
                panel[col] = pd.NA
        panel = panel[demo_cols]

    # Sort by (element, gw) for readability
    if "gw" in panel.columns:
        panel = panel.sort_values(by=["element", "gw"]).reset_index(drop=True)

    return panel


def main():
    ap = argparse.ArgumentParser(description="Merge per-player GW CSVs into one panel CSV.")
    ap.add_argument("--root", required=True, help="Root folder containing per-player subfolders and CSVs")
    ap.add_argument("--output", required=True, help="Output CSV path for the merged panel")
    ap.add_argument("--demo", default=None, help="Optional: path to a demo CSV whose column order/schema to follow")
    ap.add_argument("--glob", default="**/*.csv", help="Optional: glob pattern for matching CSVs (default: **/*.csv)")
    args = ap.parse_args()

    panel = merge_folder(args.root, demo_csv=args.demo, csv_glob=args.glob)
    # Save
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(args.output, index=False)
    print(f"[OK] Wrote merged panel to: {args.output}")
    print(f"[INFO] Rows: {len(panel):,}  Cols: {panel.shape[1]}")
    if args.demo:
        print(f"[INFO] Aligned to demo: {args.demo}")


if __name__ == "__main__":
    main()
