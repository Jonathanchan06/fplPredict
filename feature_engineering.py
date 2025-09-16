"""
Combining all data from multiple seasons, then engineering the features /preprocessing for the machine learning model
"""
import pandas as pd
from pathlib import Path

#merging all data into one csv file
all_files = list(Path(r"C:\Users\Asus\Desktop\fpl_data\archive\panels").rglob("*.csv"))

dfs = [pd.read_csv(p, dtype=str, low_memory=False) for p in all_files]
merged = pd.concat(dfs, ignore_index=True)

merged["full_name"] = merged["full_name"].astype(str).str.strip()
merged = merged[merged["full_name"].ne("") & merged["full_name"].notna()]
merged = merged.sort_values("full_name", key=lambda s: s.str.casefold(), kind="mergesort")
merged.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\all_panels.csv", index=False)

#feature engineering
df = pd.read_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\all_panels.csv")

#dropping unneeded columns
df = df.drop(columns=['transfers_in', 'team_h_score', 'points_per_game'])

#removing players that are not in the league anymore in 25/26 (Note that this is not perfect as some players who left
# the PL is still in the FPL API

df["season"] = df["season"].astype(str).str.strip()
valid = df.loc[df["season"] == "2526", "full_name"].unique()
df = df[df["full_name"].isin(valid)]
df.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\all_panels.csv", index=False)


#reassgining IDs to players as IDs are mixed up due to data over multiple seasons
df["element"] = None
name_to_id = {name: i for i, name in enumerate(df["full_name"].unique(), start=1)}
df["element"] = df["full_name"].map(name_to_id)

df.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\all_panels.csv", index=False)

#finding missing values(if any)
print(df.isnull().sum())

#add player position (missing from original data)
