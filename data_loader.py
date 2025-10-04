"""
Used for processing data currently from the official FPL API (25/26 season)
"""

import requests
import pandas as pd
import unicodedata as ud

SESSION = requests.Session()
API = "https://fantasy.premierleague.com/api"

OUT_COLS = [
    "element", "gw", "minutes", "expected_goal_involvements", "ict_index",
    "expected_goals", "expected_assists", "bps", "fixture", "starts",
    "clean_sheets", "assists", "creativity", "team_h_score", "total_points",
    "bonus", "penalties_missed", "opponent_team", "influence", "saves",
    "expected_goals_conceded", "red_cards", "team_a_score", "threat",
    "yellow_cards", "goals_conceded", "goals_scored", "points_per_game",
    "full_name",
    # NEW (team info)
    "team_id_current", "team_name_current",
    "team_id_gw", "team_name_gw",
    "opponent_team_name",
    "was_home","price_now",
    "position"
]

def strip_accents(s: str) -> str:
    if s is None:
        return ""
    return ud.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")

# --- bootstrap: players + teams + positions ---
boot = SESSION.get(f"{API}/bootstrap-static/").json()
elems = pd.json_normalize(boot["elements"])
teams = pd.json_normalize(boot["teams"])[["id","name","short_name"]].rename(columns={"id":"team_id"})
positions = pd.json_normalize(boot["element_types"])[["id","singular_name","plural_name"]].rename(columns={"id":"pos_id"})

# Player metadata with CURRENT team + pos_id
players_meta = (
    elems[["id", "first_name", "second_name", "points_per_game", "team", "element_type","now_cost"]]
    .rename(columns={"id": "element", "team": "team_id_current", "element_type": "pos_id"})
    .copy()
)

# full_name
players_meta["full_name"] = (
    players_meta["first_name"].astype(str).str.strip().fillna("") + " " +
    players_meta["second_name"].astype(str).str.strip().fillna("")
).str.replace(r"\s+", " ", regex=True).str.strip()
players_meta["full_name"] = players_meta["full_name"].map(strip_accents)

players_meta["price_now"] = (players_meta["now_cost"].astype("Int64") / 10).astype("Float64")

players_meta = players_meta[["element", "points_per_game", "full_name", "team_id_current", "pos_id", "price_now"]]

# Maps
team_name_map = dict(zip(teams["team_id"], teams["name"]))
pos_name_map = dict(zip(positions["pos_id"], positions["singular_name"]))

players_meta["team_name_current"] = players_meta["team_id_current"].map(team_name_map)
players_meta["position"] = players_meta["pos_id"].map(pos_name_map)

# --- fixtures (for per-GW team inference) ---
# We’ll fetch all fixtures once; map fixture_id -> (team_h, team_a)
fixtures = SESSION.get(f"{API}/fixtures/").json()
fx_df = pd.json_normalize(fixtures)
if not fx_df.empty:
    fx_map = fx_df.set_index("id")[["team_h","team_a"]].to_dict(orient="index")
else:
    fx_map = {}

# --- per-player history ---
rows = []
for i, pid in enumerate(players_meta["element"].tolist(), 1):
    try:
        resp = SESSION.get(f"{API}/element-summary/{int(pid)}/", timeout=15)
        resp.raise_for_status()
        hist = resp.json().get("history", [])
        for h in hist:
            fixture_id = h.get("fixture")
            was_home = h.get("was_home")
            opp_id = h.get("opponent_team")

            # derive per-GW team from fixture + was_home
            team_id_gw = None
            if fixture_id in fx_map and was_home is not None:
                t_h = fx_map[fixture_id]["team_h"]
                t_a = fx_map[fixture_id]["team_a"]
                team_id_gw = t_h if was_home else t_a

            rows.append({
                "element": pid,
                "gw": h.get("round"),
                "minutes": h.get("minutes"),
                "expected_goal_involvements": h.get("expected_goal_involvements"),
                "ict_index": h.get("ict_index"),
                "expected_goals": h.get("expected_goals"),
                "expected_assists": h.get("expected_assists"),
                "bps": h.get("bps"),
                "fixture": fixture_id,
                "starts": h.get("starts"),
                "clean_sheets": h.get("clean_sheets"),
                "assists": h.get("assists"),
                "creativity": h.get("creativity"),
                "team_h_score": h.get("team_h_score"),
                "total_points": h.get("total_points"),
                "bonus": h.get("bonus"),
                "penalties_missed": h.get("penalties_missed"),
                "opponent_team": opp_id,
                "influence": h.get("influence"),
                "saves": h.get("saves"),
                "expected_goals_conceded": h.get("expected_goals_conceded"),
                "red_cards": h.get("red_cards"),
                "team_a_score": h.get("team_a_score"),
                "threat": h.get("threat"),
                "yellow_cards": h.get("yellow_cards"),
                "goals_conceded": h.get("goals_conceded"),
                "goals_scored": h.get("goals_scored"),
                "was_home": was_home,
                # placeholders; will fill after merge
                "team_id_gw": team_id_gw,
            })
    except requests.exceptions.RequestException:
        continue
    except ValueError:
        continue

panel = pd.DataFrame(rows)

# Merge player meta (current team + name + ppg)
panel = panel.merge(players_meta, on="element", how="left")

# Map opponent and per-GW team names
panel["opponent_team_name"] = panel["opponent_team"].map(team_name_map)
panel["team_name_gw"] = panel["team_id_gw"].map(team_name_map)

# Guarantee schema
for col in OUT_COLS:
    if col not in panel.columns:
        panel[col] = pd.NA

panel = panel.sort_values(["element", "gw"], kind="mergesort")[OUT_COLS].reset_index(drop=True)

# add season tag like you had
panel["season"] = "2526"

# write
out_path = r"C:\Users\Asus\Desktop\fpl_data\archive\panels\players2526_panel.csv"
panel.to_csv(out_path, index=False)
print("Wrote:", panel.shape, "->", out_path)
