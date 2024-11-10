from sqlalchemy import create_engine, MetaData, Table
import pandas as pd

# Database connection strings
source_db_url = "source_database_connection_string"
destination_db_url = "destination_database_connection_string"

def copy_all_tables(source_db_url, destination_db_url):
    # Create engines for source and destination databases
    source_engine = create_engine(source_db_url)
    destination_engine = create_engine(destination_db_url)

    # Reflect the source database schema
    source_metadata = MetaData(bind=source_engine)
    source_metadata.reflect(bind=source_engine)

    # Iterate over each table in the source database
    for table_name, source_table in source_metadata.tables.items():
        print(f"Copying table: {table_name}")

        # Read data from the source table into a DataFrame
        with source_engine.connect() as source_connection:
            query = source_connection.execute(source_table.select())
            data = pd.DataFrame(query.fetchall(), columns=query.keys())

        # Copy schema to destination database
        destination_metadata = MetaData(bind=destination_engine)
        destination_table = Table(table_name, destination_metadata, autoload_with=source_engine)

        # Create table in destination if it doesn’t exist
        if not destination_engine.dialect.has_table(destination_engine, table_name):
            destination_metadata.create_all(destination_engine, tables=[destination_table])
            print(f"Table {table_name} created in destination database.")

        # Insert data into the destination table
        with destination_engine.connect() as destination_connection:
            data.to_sql(table_name, destination_connection, if_exists="replace", index=False)
            print(f"Data copied for table: {table_name}")

    print("All tables copied successfully.")

# Run the function to copy data
copy_all_tables(source_db_url, destination_db_url)
