from operator import index

import requests, pandas as pd, unicodedata as ud



#2526 processing
boot = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()
players = pd.json_normalize(boot["elements"])   # all player metadata
teams = pd.json_normalize(boot["teams"])
events = pd.json_normalize(boot["events"])

players.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2526.csv", index=False) #Visualization of Api data
df_players2526 = pd.read_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2526.csv")

df_players2526["full_name"] = (                                                  #combining first and last names
        df_players2526["first_name"].astype(str).str.strip().fillna("") + " " +
        df_players2526["second_name"].astype(str).str.strip().fillna("")
).str.replace(r"\s+", " ", regex=True).str.strip()

df_players2526_cleaned = df_players2526.drop(columns=["first_name", "second_name", "birth_date", "can_select", "can_transact",
                             "code", "corners_and_indirect_freekicks_order","corners_and_indirect_freekicks_text",
                             "cost_change_event","cost_change_event_fall","cost_change_start","cost_change_start_fall",
                             "creativity_rank","creativity_rank_type","direct_freekicks_order","direct_freekicks_text",
                             "dreamteam_count","element_type","ep_next","ep_this","event_points","form","form_rank",
                             "form_rank_type","has_temporary_code","ict_index_rank","ict_index_rank_type",
                             "in_dreamteam","influence_rank","influence_rank_type","mng_clean_sheets","mng_draw",
                             "mng_goals_scored","mng_loss","mng_underdog_draw","mng_underdog_win","mng_win",
                             "news","news_added","now_cost","now_cost_rank","now_cost_rank_type","opta_code",
                             "own_goals","penalties_missed","penalties_order","penalties_saved","penalties_text",
                             "photo","points_per_game_rank","points_per_game_rank_type","region","removed","saves",
                             "selected_by_percent","selected_rank","selected_rank_type","special","squad_number",
                             "starts","status","team","team_code","team_join_date","threat_rank","threat_rank_type",
                             "transfers_in","transfers_in_event","transfers_out","transfers_out_event","value_form",
                             "value_season","web_name"], errors="ignore")
print(df_players2526_cleaned.columns)

#2425 processing
df_players2425 = pd.read_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425.csv")
df_players2425["full_name"] = (                                                  #combining first and last names
        df_players2425["first_name"].astype(str).str.strip().fillna("") + " " +
        df_players2425["second_name"].astype(str).str.strip().fillna("")
).str.replace(r"\s+", " ", regex=True).str.strip()

df_players2425_cleaned = df_players2425.drop(columns=["first_name", "second_name", "birth_date",
                             "chance_of_playing_next_round","chance_of_playing_this_round","can_select", "can_transact",
                             "code", "corners_and_indirect_freekicks_order","corners_and_indirect_freekicks_text",
                             "cost_change_event","cost_change_event_fall","cost_change_start","cost_change_start_fall",
                             "creativity_rank","creativity_rank_type","direct_freekicks_order","direct_freekicks_text",
                             "dreamteam_count","element_type","ep_next","ep_this","event_points","form","form_rank",
                             "form_rank_type","has_temporary_code","ict_index_rank","ict_index_rank_type",
                             "in_dreamteam","influence_rank","influence_rank_type","mng_clean_sheets","mng_draw",
                             "mng_goals_scored","mng_loss","mng_underdog_draw","mng_underdog_win","mng_win",
                             "news","news_added","now_cost","now_cost_rank","now_cost_rank_type","opta_code",
                             "own_goals","penalties_missed","penalties_order","penalties_saved","penalties_text",
                             "photo","points_per_game_rank","points_per_game_rank_type","region","removed","saves",
                             "selected_by_percent","selected_rank","selected_rank_type","special","squad_number",
                             "starts","status","team","team_code","team_join_date","threat_rank","threat_rank_type",
                             "transfers_in","transfers_in_event","transfers_out","transfers_out_event","value_form",
                             "value_season","web_name"],errors="ignore")
print(df_players2425_cleaned.columns)

#removing accents of names
df_players2425_cleaned["full_name"] = df_players2425_cleaned["full_name"].apply(lambda x: x if pd.isna(x) else
    "".join(ch for ch in ud.normalize("NFKD", str(x)) if not ud.combining(ch)
    ).translate(str.maketrans({
        "Ø":"O","ø":"o",
        "Ł":"L","ł":"l",
        "Đ":"D","đ":"d",
        "Æ":"AE","æ":"ae",
        "Œ":"OE","œ":"oe",
        "ß":"ss",
        "Þ":"Th","þ":"th",
        "Ŋ":"N","ŋ":"n",
        "ƒ":"f",
    }))
)
df_players2526_cleaned["full_name"] = df_players2526_cleaned["full_name"].apply(lambda x: x if pd.isna(x) else
    "".join(ch for ch in ud.normalize("NFKD", str(x)) if not ud.combining(ch)
    ).translate(str.maketrans({
        "Ø":"O","ø":"o",
        "Ł":"L","ł":"l",
        "Đ":"D","đ":"d",
        "Æ":"AE","æ":"ae",
        "Œ":"OE","œ":"oe",
        "ß":"ss",
        "Þ":"Th","þ":"th",
        "Ŋ":"N","ŋ":"n",
        "ƒ":"f",
    }))
)

df_players2425_panel = pd.read_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425_panel.csv")
df_players2324_panel = pd.read_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2324_panel.csv")

df_players2425_panel["full_name"] = df_players2425_panel["full_name"].apply(lambda x: x if pd.isna(x) else
    "".join(ch for ch in ud.normalize("NFKD", str(x)) if not ud.combining(ch)
    ).translate(str.maketrans({
        "Ø":"O","ø":"o",
        "Ł":"L","ł":"l",
        "Đ":"D","đ":"d",
        "Æ":"AE","æ":"ae",
        "Œ":"OE","œ":"oe",
        "ß":"ss",
        "Þ":"Th","þ":"th",
        "Ŋ":"N","ŋ":"n",
        "ƒ":"f",
    }))
)

df_players2324_panel["full_name"] = df_players2324_panel["full_name"].apply(lambda x: x if pd.isna(x) else
    "".join(ch for ch in ud.normalize("NFKD", str(x)) if not ud.combining(ch)
    ).translate(str.maketrans({
        "Ø":"O","ø":"o",
        "Ł":"L","ł":"l",
        "Đ":"D","đ":"d",
        "Æ":"AE","æ":"ae",
        "Œ":"OE","œ":"oe",
        "ß":"ss",
        "Þ":"Th","þ":"th",
        "Ŋ":"N","ŋ":"n",
        "ƒ":"f",
    }))
)

#final conversion of modified data
df_players2526_cleaned.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2526_cleaned.csv", index=False)
df_players2425_cleaned.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425_cleaned.csv", index=False)
df_players2425_panel.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425_panel", index=False)
df_players2324_panel.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2324_panel.csv", index=False)
