import math
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
import xgboost as xgb

# ---------- CONFIG ----------
panels_engineered = r"C:\Users\Asus\Desktop\fpl_data\archive\all_panels_engineered.csv"
gw_predictions = r"C:\Users\Asus\Desktop\fpl_data\archive\curr_gw_predictions.csv"
team_predictions = r"C:\Users\Asus\Desktop\fpl_data\archive\curr_gw_team.csv"

train_seasons = [2324, 2425]
curr_season = 2526
train_gw_until = 6
test_gw = 7

#SET INPUT FORMATION
input_formation = {"Goalkeeper": 1, "Defender": 3, "Midfielder": 4, "Forward": 3}
budget = 100.0
max_players_per_team = 3
# ---------------------------

#Load data
df = pd.read_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\all_panels.csv")

# make season sortable
if df["season"].dtype == object:
    if df["season"].astype(str).str.contains("/").any():
        df["season"] = df["season"].astype(str).str.replace("/", "", regex=False).astype(int)
    else:
        df["season"] = df["season"].astype(int)
else:
    df["season"] = df["season"].astype(int)

df["gw"] = df["gw"].astype(int)
df = df.sort_values(["element", "season", "gw"]).reset_index(drop=True)

#---------------
df["total_points_next"] = df.groupby(["element", "season"])["total_points"].shift(-1)

#----------
#Forming features from 3week, 5week, 8week lags
form_cols = [
    "minutes",
    "expected_goal_involvements",
    "bps",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "creativity"
]
for n in [5, 8]: ##Adjust lag values as needed. Removed '3' as felt too reliant on recent 'purple patch' form
    for col in form_cols:
        df[f"{col}_mean{n}"] = (
            df.groupby("element")[col].transform(lambda x: x.rolling(n, min_periods=1).mean())
        )

df["xgi_momentum_3_8"] = df["expected_goal_involvements_mean5"] - df["expected_goal_involvements_mean8"]
df["minutes_momentum_3_8"] = df["minutes_mean5"] - df["minutes_mean8"]
df["minutes_lag1"] = df.groupby("element")["minutes"].shift(1)
df["xgi_lag1"] = df.groupby("element")["expected_goal_involvements"].shift(1)

#incomplete data
df.drop(columns=["was_home"], inplace=True)

#-----------
#Train/test
train_mask = (
        df["season"].isin(train_seasons) |
        ((df["season"] == curr_season) & (df["gw"] <= train_gw_until))
)
test_mask = (df["season"] == curr_season) & (df["gw"] == test_gw)

train_df = df[train_mask].copy()
test_df = df[test_mask].copy()

train_df = train_df.dropna(subset=["total_points_next"])


#------------
#Features
drop_cols = [
    "total_points_next",
    "total_points",
    "full_name",
    "team_name_current",
    "team_name_gw",
    "opponent_team_name",
    "season",
    "fixture",
    "element",
    "gw",
    "team_id_current", "team_id_gw",
    "opponent_team",
    "position"
]

# keep original position for picking, one-hot a copy for the model
train_df_model = train_df.copy()
test_df_model = test_df.copy()
train_df_model["position_copy"] = train_df_model["position"]
test_df_model["position_copy"] = test_df_model["position"]

train_df_model = pd.get_dummies(train_df_model, columns=["position_copy"], drop_first=True)
test_df_model = pd.get_dummies(test_df_model, columns=["position_copy"], drop_first=True)

target = "total_points_next"
features = [c for c in train_df_model.columns if c not in drop_cols + [target]]

# align test columns
test_df_model = test_df_model.reindex(columns=train_df_model.columns, fill_value=0)

X_train = train_df_model[features].fillna(0)
y_train = train_df_model[target].astype(float)
X_test = test_df_model[features].fillna(0)
y_test = test_df_model[target].astype(float)

#----------
#Xgboost model training
model = xgb.XGBRegressor(
    objective="reg:squarederror",
    tree_method="hist",   # use 'gpu_hist' if you have a GPU
    n_estimators=500,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    eval_metric="rmse",
    random_state=42
)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)


test_df = test_df.copy()
test_df["predicted_points_next"] = y_pred

#-----------
#Saving predictions
df.to_csv(panels_engineered, index=False)
test_df.sort_values("predicted_points_next", ascending=False).to_csv(gw_predictions, index=False)
print(f"[Saved] Engineered -> {panels_engineered}")
print(f"[Saved] Predictions -> {gw_predictions}")

#-----------
#Picking team
pick_pool = test_df.copy()
pick_pool = pick_pool[
    pick_pool["predicted_points_next"].notna()
    & pick_pool["price_now"].notna()
    & (pick_pool["price_now"] > 0)
].copy()

#normalize
pick_pool["team_name_current"] = pick_pool["team_name_current"].astype(str).replace({"nan": "Unknown", "None": "Unknown"})

# sort by predicted points descending
pick_pool = pick_pool.sort_values("predicted_points_next", ascending=False)

remaining_budget = float(budget)
team_counts = {}
picked_rows = []


for pos in ["Goalkeeper", "Defender", "Midfielder", "Forward"]:
    need = int(input_formation.get(pos, 0))
    if need <= 0:
        continue

    pos_pool = pick_pool[pick_pool["position"] == pos]

    for _, row in pos_pool.iterrows():
        if need == 0:
            break
        price = float(row["price_now"])
        team = str(row["team_name_current"])

        if price > remaining_budget:
            continue
        if team_counts.get(team, 0) >= max_players_per_team:
            continue

        picked_rows.append(row)
        remaining_budget -= price
        team_counts[team] = team_counts.get(team, 0) + 1
        need -= 1

team_343 = pd.DataFrame(picked_rows)

# sort team by position then predicted points
if not team_343.empty:
    team_343 = team_343.sort_values(["position", "predicted_points_next"], ascending=[True, False])
    cols_show = [c for c in [
        "full_name", "position", "team_name_current", "price_now",
        "predicted_points_next", "minutes_mean5", "expected_goal_involvements_mean5"
    ] if c in team_343.columns]

    print("\n=== Selected 3-4-3 XI  ===")
    print(team_343[cols_show].to_string(index=False))

    team_343.to_csv(team_predictions, index=False)
    print(f"\n[Saved] XI -> {team_predictions}")
else:
    print("No feasible team selected. Check data, positions, budget, or team caps.")
