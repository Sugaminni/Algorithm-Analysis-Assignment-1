import pandas as pd
from typing import List, Dict, Any
import os

# The r in front fixes pycharm's weird misread bug
INPUT_PATH = r"E:\Pycharm\AAAssignment1\Premier league.csv"
OUTPUT_PATH = r"E:\Pycharm\AAAssignment1\Premier_league_sorted.csv"

# Loads CSV
df = pd.read_csv(INPUT_PATH)

# Normalizes expected columns
col_map_candidates = {
    "team": ["Team Name", "Team", "team", "Club", "club, ", "Name"],
    "points": ["P", "Pts", "Points", "points"],
    "gf": ["GF", "Goals For", "For", "goals_for"],
    "ga": ["GA", "Goals Against", "Against", "goals_against"],
    "gd": ["GD", "Goal Difference", "goal_difference"],
    "wins": ["W", "Wins", "wins"],
}

def find_col(df_columns, candidates):
    for c in candidates:
        if c in df_columns:
            return c
    return None

resolved = {}
for key, cands in col_map_candidates.items():
    resolved[key] = find_col(df.columns, cands)

# Basic validation
missing_core = [k for k in ["team", "points", "wins"] if resolved[k] is None]
if resolved["gf"] is None:
    # If GF missing, tie-breaker
    missing_core.append("gf")
if resolved["ga"] is None and resolved["gd"] is None:
    # Need either GA or GD to calculate/apply GD sort
    missing_core.append("ga_or_gd")

# If GD column missing but have GF and GA, calculate it
if resolved["gd"] is None and resolved["gf"] is not None and resolved["ga"] is not None:
    df["__GD__"] = df[resolved["gf"]] - df[resolved["ga"]]
    resolved["gd"] = "__GD__"

# Casts numeric fields to numeric (treat NaNs as worst so sort is stable and safe)
for k in ["points", "gf", "ga", "gd", "wins"]:
    col = resolved.get(k)
    if col is not None and col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-10**9)

# Hard-require the key fields actually exist
if resolved["points"] is None or resolved["wins"] is None or resolved["gf"] is None:
    raise ValueError(f"Required columns missing for sort: {missing_core}")
if resolved["gd"] is None and resolved["ga"] is None:
    raise ValueError("Need either GD or (GF and GA) to apply the GD tie-breaker.")

# 3) Defines merge sort with custom comparator
def comes_before(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Returns True if row a should come before row b according to sort rules:
       Desc: Points, GD, GF, W
    """
    P_a, P_b = a[resolved["points"]], b[resolved["points"]]
    if P_a != P_b:
        return P_a > P_b

    GD_a, GD_b = a[resolved["gd"]], b[resolved["gd"]]
    if GD_a != GD_b:
        return GD_a > GD_b

    GF_a, GF_b = a[resolved["gf"]], b[resolved["gf"]]
    if GF_a != GF_b:
        return GF_a > GF_b

    W_a, W_b = a[resolved["wins"]], b[resolved["wins"]]
    if W_a != W_b:
        return W_a > W_b

    # Tie-breaker: by Team name ascending if available
    team_col = resolved.get("team")
    if team_col is not None:
        return str(a[team_col]) < str(b[team_col])
    return False

def merge(left: List[Dict[str, Any]], right: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    i = j = 0
    merged = []
    while i < len(left) and j < len(right):
        if comes_before(left[i], right[j]):
            merged.append(left[i])
            i += 1
        else:
            merged.append(right[j])
            j += 1
    while i < len(left):
        merged.append(left[i])
        i += 1
    while j < len(right):
        merged.append(right[j])
        j += 1
    return merged

def merge_sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(rows) <= 1:
        return rows
    mid = len(rows) // 2
    left = merge_sort_rows(rows[:mid])
    right = merge_sort_rows(rows[mid:])
    return merge(left, right)

# Prepares data as list of dicts to keep original columns
rows = df.to_dict(orient="records")
sorted_rows = merge_sort_rows(rows)
sorted_df = pd.DataFrame(sorted_rows, columns=df.columns)  # keeps original column order

# Adds Position column at the front
sorted_df.insert(0, "Position", range(1, len(sorted_df) + 1))

# Saves output
sorted_df.to_csv(OUTPUT_PATH, index=False)

# Shows result and a brief header with discovered columns
summary_info = {
    "Detected columns": resolved,
    "Missing core (if any)": missing_core,
    "Input path": INPUT_PATH,
    "Output path": OUTPUT_PATH,
}
print(summary_info)