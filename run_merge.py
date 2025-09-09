from merge_fpl_gw_to_panel import merge_folder

panel = merge_folder(
    root=r"C:\Users\Asus\Desktop\fpl_data\archive\players2425_weeklydata",
    demo_csv=r"C:\Users\Asus\Desktop\fpl_data\archive\players_2425_panel_demo.csv",  # optional but recommended
    csv_glob="**/*.csv"  # change if your files have a special pattern
)
panel.to_csv(r"C:\Users\Asus\Desktop\fpl_data\archive\players_2526_panel.csv", index=False)
print(panel.shape)  # rows, cols
