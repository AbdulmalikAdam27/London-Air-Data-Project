import sqlite3
import pandas as pd

conn = sqlite3.connect(r".\data\london_air.db")
df = pd.read_sql_query(
    "select site_code, site_name, species_code, species_name, aq_index, data_end, raw_species_json "
    "from readings limit 5",
    conn,
)

print(df[["site_code","site_name","species_code","species_name","aq_index","data_end"]])

if len(df) > 0 and "raw_species_json" in df.columns:
    print("\nRAW SPECIES JSON SAMPLE:\n")
    print(df["raw_species_json"].iloc[0][:800])

conn.close()
