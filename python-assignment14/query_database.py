# 3. Database Query Program

import sqlite3

def main():
    try:
        conn = sqlite3.connect('db/mlb_history.db')
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()

        print("Connected to database.")

        def querys(query, entereddata):
            cursor.execute(query, (entereddata,))
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
            else:
                print("No data found")

        # year = input("Enter year: ")
        # query = """
        # SELECT year, name, team, statistic, value
        # FROM main_data_american_league
        # WHERE year = ?
        # """
        # querys(query, year)

        # city = input("Enter the city of the team: ")
        # query = """
        # SELECT team, year, value
        # FROM main_data_american_league
        # WHERE team = ?
        # """
        # querys(query, city)

        name = input("Enter name: ").strip()
        if not name or name.isdigit():
            print("Name wrong or empty")
            exit()
        query = """
        SELECT
            m.year,
            y.league,
            m.name,
            m.value
        FROM main_data_american_league m
        JOIN years y
        ON m.year = y.year AND m.name_of_league = y.league
        WHERE m.name = ?
        """
        querys(query, name)

    except sqlite3.Error as e:
        print("Database error:", e)

    finally:
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()