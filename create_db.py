import sqlite3
import pandas as pd


def load_csv_data(file_path) -> pd.DataFrame:
    df = pd.read_csv(file_path, sep=';')

    # Split 'Datum' column into 'ev', 'honap', and 'nap'
    datum_columns = df['Datum'].str.split('.', expand=True)
    df[['ev', 'honap', 'nap']] = datum_columns.astype(int)

    # Clean and convert 'gyemant' column to integer
    df['gyemant'] = df['gyemant'].replace(',', '.', regex=True)
    df['gyemant'] = df['gyemant'].astype(float).astype(int)

    # Return df, further cleaning is not necessary
    return df


def create_database(db_path, table_query):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop existing table if it exists and create a new one
    cursor.execute("DROP TABLE IF EXISTS termeles")
    cursor.execute(table_query)

    conn.commit()
    conn.close()


def insert_data_to_database(db_path: str, df):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO termeles (ev, honap, nap, aranytermeles, ezusttermeles, gyemanttermeles) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row['ev'], row['honap'], row['nap'], row.get('arany', 0), row.get('ezust', 0),
              row['gyemant']))

    conn.commit()
    conn.close()


def main(csv_file_path: str, db_file_path: str):
    # Define the SQL query to create the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS termeles (
        azon INTEGER PRIMARY KEY,
        ev INTEGER,
        honap INTEGER,
        nap INTEGER,
        aranytermeles BIGINT,
        ezusttermeles BIGINT,
        gyemanttermeles BIGINT
    );
    """

    # Load data from CSV
    df = load_csv_data(csv_file_path)

    # Create database and table
    create_database(db_file_path, create_table_query)

    # Insert data into the database
    insert_data_to_database(db_file_path, df)


main('Hofeherke.csv', 'termeles.db')
