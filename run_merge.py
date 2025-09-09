from merge_fpl_gw_to_panel import merge_folder

# panel2425 = merge_folder(
#     root=r"C:\Users\Asus\Desktop\fpl_data\archive\players2425_weeklydata",
#     demo_csv=r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425_panel_demo.csv",  # optional but recommended
#     csv_glob="**/*.csv"  # change if your files have a special pattern
# )
# panel2425.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425_panel.csv", index=False)
# print(panel2425.shape)  # rows, cols

panel2324 = merge_folder(
    root=r"C:\Users\Asus\Desktop\fpl_data\archive\players2324_weeklydata",
    demo_csv=r"C:\Users\Asus\Desktop\fpl_data\archive\players_panel_format.csv",  # optional but recommended
    csv_glob="**/*.csv"  # change if your files have a special pattern
)
panel2324.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2324_panel.csv", index=False)
print(panel2324.shape)  # rows, cols


##THIS IS TO MERGE THE DATA FILES FROM EACH PLAYER ONTO ONE CSV FILE
