import sqlite3
import pandas as pd


def load_csv_data(file_path: str) -> pd.DataFrame:
    """
    Load data from a CSV file, parse date columns, and clean the data.

    Parameters:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Processed DataFrame with separate columns for year, month, day, and cleaned numeric fields.
    """
    df = pd.read_csv(file_path, sep=';')

    # Split 'Datum' column into 'ev', 'honap', and 'nap'
    df[['ev', 'honap', 'nap']] = df['Datum'].str.split('.', expand=True).astype(int)

    # Clean and convert 'gyemant' column to integer
    df['gyemant'] = df['gyemant'].str.replace(',', '.').astype(float).astype(int)

    return df


def create_database(db_path: str) -> None:
    """
    Create an SQLite database file if it doesn't exist.

    Parameters:
        db_path (str): Path to the SQLite database file.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Database created at {db_path} with connection established.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the database: {e}")
    finally:
        if conn:
            conn.close()


def setup_table(db_path: str, target_table_name: str) -> None:
    """
    Set up a table with a specified schema in the SQLite database.

    Parameters:
        db_path (str): Path to the SQLite database file.
        target_table_name (str): Name of the table to create.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {target_table_name} (
        azon INTEGER PRIMARY KEY,
        ev INTEGER,
        honap INTEGER,
        nap INTEGER,
        aranytermeles BIGINT DEFAULT 0,
        ezusttermeles BIGINT DEFAULT 0,
        gyemanttermeles BIGINT
    );
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table {target_table_name} successfully created in {db_path}.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the table: {e}")
    finally:
        if conn:
            conn.close()


def insert_data_to_database(db_path: str, target_table_name: str, data_frame: pd.DataFrame) -> None:
    """
    Insert data from a DataFrame into an SQLite database.

    Parameters:
        db_path (str): Path to the SQLite database file.
        target_table_name (str): Name of the table to insert data into.
        data_frame (pd.DataFrame): DataFrame containing the data to insert.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for _, row in data_frame.iterrows():
            cursor.execute(f"""
                INSERT INTO {target_table_name} (ev, honap, nap, aranytermeles, ezusttermeles, gyemanttermeles) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['ev'],
                row['honap'],
                row['nap'],
                row.get('arany', 0),  # Use default value if 'arany' is missing
                row.get('ezust', 0),  # Use default value if 'ezust' is missing
                row['gyemant']
            ))

        conn.commit()
        print(f"Data successfully inserted into {target_table_name}.")
    except sqlite3.Error as e:
        print(f"An error occurred while inserting data into the database: {e}")
    finally:
        if conn:
            conn.close()


def main(csv_file_path: str, db_file_path: str, target_table_name: str = 'termeles') -> None:
    """
    Main function to load CSV data, create a database, and insert data into it.

    Parameters:
        csv_file_path (str): Path to the CSV file with production data.
        db_file_path (str): Path to the SQLite database file.
        target_table_name (str): Name of the table to create and insert data into.
    """
    # Load data from CSV
    df = load_csv_data(csv_file_path)

    # Create database
    create_database(db_file_path)

    # Set up the table in the database
    setup_table(db_file_path, target_table_name)

    # Insert data into the database
    insert_data_to_database(db_file_path, target_table_name, df)


# Execute main function with specified file paths and table name
if __name__ == "__main__":
    # Hardcoded values are removed; the user can now specify file paths and table name as parameters
    csv_file = 'Hofeherke.csv'
    db_file = 'termeles.db'
    table_name = 'termeles'
    main(csv_file, db_file, table_name)
