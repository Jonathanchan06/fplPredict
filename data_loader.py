"""
Used for processing data currently from the official FPL API (25/26 season
"""

import requests
import pandas as pd
import unicodedata as ud

SESSION = requests.Session()

OUT_COLS = [
    "element", "gw", "minutes", "expected_goal_involvements", "ict_index",
    "expected_goals", "expected_assists", "bps", "fixture", "starts",
    "clean_sheets", "assists", "creativity", "team_h_score", "total_points",
    "bonus", "penalties_missed", "opponent_team", "influence", "saves",
    "expected_goals_conceded", "red_cards", "team_a_score", "threat",
    "yellow_cards", "goals_conceded", "goals_scored", "points_per_game",
    "full_name"
]


def strip_accents(s: str) -> str:
    """

    :param s: accentuated text
    :return: text with no accent
    """
    if s is None:
        return ""
    return ud.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")


# retrieve 'element'/player id, names, ppg
boot = SESSION.get(f"{"https://fantasy.premierleague.com/api"}/bootstrap-static/").json()
elems = pd.json_normalize(boot["elements"])

players_meta = (
    elems[["id", "first_name", "second_name", "points_per_game"]]
    .rename(columns={"id": "element"})
    .copy()
)
# combine first name and second name
players_meta["full_name"] = (
        players_meta["first_name"].astype(str).str.strip().fillna("") + " " +
        players_meta["second_name"].astype(str).str.strip().fillna("")
).str.replace(r"\s+", " ", regex=True).str.strip()
players_meta["full_name"] = players_meta["full_name"].map(strip_accents)
players_meta = players_meta[["element", "points_per_game", "full_name"]]

# retrieve live platyer history
rows = []
for i, pid in enumerate(players_meta["element"].tolist(), 1):
    try:
        resp = SESSION.get(f"{"https://fantasy.premierleague.com/api"}/element-summary/{int(pid)}/", timeout=15)
        resp.raise_for_status()
        hist = resp.json().get("history", [])
        for h in hist:
            rows.append({
                "element": pid,
                "gw": h.get("round"),
                "minutes": h.get("minutes"),
                "expected_goal_involvements": h.get("expected_goal_involvements"),
                "ict_index": h.get("ict_index"),
                "expected_goals": h.get("expected_goals"),
                "expected_assists": h.get("expected_assists"),
                "bps": h.get("bps"),
                "fixture": h.get("fixture"),
                "starts": h.get("starts"),
                "clean_sheets": h.get("clean_sheets"),
                "assists": h.get("assists"),
                "creativity": h.get("creativity"),
                "team_h_score": h.get("team_h_score"),
                "total_points": h.get("total_points"),
                "bonus": h.get("bonus"),
                "penalties_missed": h.get("penalties_missed"),
                "opponent_team": h.get("opponent_team"),
                "influence": h.get("influence"),
                "saves": h.get("saves"),
                "expected_goals_conceded": h.get("expected_goals_conceded"),
                "red_cards": h.get("red_cards"),
                "team_a_score": h.get("team_a_score"),
                "threat": h.get("threat"),
                "yellow_cards": h.get("yellow_cards"),
                "goals_conceded": h.get("goals_conceded"),
                "goals_scored": h.get("goals_scored"),
            })
    except requests.exceptions.RequestException:
        continue
    except ValueError:
        continue

# merge columns
panel = pd.DataFrame(rows).merge(players_meta, on="element", how="left")
for col in OUT_COLS:  # guarantee schema even if some cols are missing
    if col not in panel.columns:
        panel[col] = pd.NA
panel = panel.sort_values(["element", "gw"], kind="mergesort")[OUT_COLS].reset_index(drop=True)
panel['season'] = '2526'
panel.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players2526_panel.csv"
             , index=False)
print("Wrote:", panel.shape, "->", r"C:\Users\Asus\Desktop\fpl_data\archive\players2526_panel.csv"
      )
