# 2. Database Import Program
# import_to_db.py

import pandas as pd
import sqlite3

def checker(data, name="DateFrame"):
    print(f"{name} head \n", data.head())
    print(f"{name} columns \n", data.columns)
    print(f"{name} info \n", data.info())
    print(f"{name} dtypes \n", data.dtypes)
    print(f"{name} isna sum \n", data.isna().sum())
    print(f"{name} duplicated sum \n", data.duplicated().sum())
    print(f"{name} describe() \n", data.describe())

def checker_read_csv(data):
    try:
        df = pd.read_csv(data)
        return df
    except Exception as e:
        print(f"error {data}: {e}")
        return None

df_years_american_league = checker_read_csv('db/df_years_american_league.csv')
if df_years_american_league is None or df_years_american_league.empty:
    print("df_years_american_league is empty!")
    exit()

df_years_national_league = checker_read_csv('db/df_years_national_league.csv')
if df_years_national_league is None:
    exit()

df_main_data_american_league = checker_read_csv('db/df_main_data_american_league.csv')
if df_main_data_american_league is None:
    exit()

# df_years_all = pd.concat(
#     [df_years_american_league, df_years_national_league],
#     ignore_index=True
# )
df_years_american_league.columns = df_years_american_league.columns.str.strip()
df_years_national_league.columns = df_years_national_league.columns.str.strip()

df_years_all = pd.concat(
    [df_years_american_league, df_years_national_league],
    ignore_index=True
)
df_years_all.to_csv("df_years.csv", index=False)


checker(df_years_all, "df_years_all")
checker(df_main_data_american_league, "df_main_data_american_league")

# copy
df_years_all_copy = df_years_all.copy()
df_main_data_american_league_copy = df_main_data_american_league.copy()

# cleaning
df_main_data_american_league_copy.columns = df_main_data_american_league_copy.columns.str.strip()
df_years_all_copy.columns = df_years_all_copy.columns.str.strip()

df_main_data_american_league_copy["value"] = pd.to_numeric(df_main_data_american_league_copy["value"], errors="coerce")
df_main_data_american_league_copy["year"] = pd.to_numeric(df_main_data_american_league_copy["year"], errors="coerce")

df_years_all_copy['year'] = pd.to_numeric(df_years_all_copy['year'], errors="coerce")


#df_main_data_american_league_copy = df_main_data_american_league_copy.dropna()
before_rows = len(df_main_data_american_league_copy)
df_main_data_american_league_copy = df_main_data_american_league_copy.dropna(subset=['year', 'value'])
after_rows = len(df_main_data_american_league_copy)
print(f"Dropped {before_rows - after_rows} rows with missing year or value")

df_years_all_copy = df_years_all_copy.dropna()

checker(df_main_data_american_league_copy, "df_main_data_american_league_copy")
checker(df_years_all_copy, "df_years_all_copy")


with sqlite3.connect('db/mlb_history.db') as conn:
    print("db created and connected successfully.")
    cursor = conn.cursor()

    conn.execute("PRAGMA foreign_keys = ON;")

    cursor.execute("DROP TABLE IF EXISTS main_data_american_league")
    cursor.execute("DROP TABLE IF EXISTS years")
    #cursor.execute("DROP TABLE IF EXISTS years_american_league")
    #cursor.execute("DROP TABLE IF EXISTS years_national_league")

    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS years_american_league(
    #     year INTEGER PRIMARY KEY,
    #     league TEXT
    # );
    # """)

    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS years_national_league(
    #     year INTEGER PRIMARY KEY,
    #     league TEXT
    # );
    # """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS years(
    year INTEGER,
    league TEXT,
    PRIMARY KEY (year, league)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS main_data_american_league(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER,
        name_of_league TEXT,
        name_of_review TEXT,
        statistic TEXT,
        name TEXT,
        team TEXT,
        value REAL,
        FOREIGN KEY (year, name_of_league) REFERENCES years(year, league)
    );
    """)

    print("Checking that all years in main_data exist in years table...")
    missing_years = set(df_main_data_american_league_copy['year'].unique()) - set(df_years_all_copy['year'].unique())
    if  missing_years:
        print("years missing in 'years' table:", missing_years)

    df_years_all_copy.to_sql('years', conn, if_exists='append', index=False)
    print('years added successfully.')

    df_main_data_american_league_copy.to_sql('main_data_american_league', conn, if_exists='append', index=False)
    print('main_data_american_league added successfully.')

    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    print("tables", tables)

    for table_name in ['years', 'main_data_american_league']:
        count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn)['cnt'][0]
        print(f"Table {table_name} has {count} rows")

