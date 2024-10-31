import psycopg2
import pandas as pd
import csv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

def dump_table(username, password, database, table, host='localhost', port='5432'):
    try:
        # 使用 SQLAlchemy 建立引擎
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

        fetch_table_sql = f"SELECT * FROM {table};"
        df = pd.read_sql_query(fetch_table_sql, engine)
        return df
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()

def list_tables(user, password, dbname, host='localhost', port='5432'):
    try:
        # Create an engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        
        # Use SQLAlchemy's inspector to retrieve all tables
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        # Print the list of tables
        if tables:
            print("Tables in the database:")
            for table in tables:
                print(table)
        else:
            print("No tables found in the database.")
    
    except SQLAlchemyError as e:
        print(f"An error occurred while listing tables: {e}")
    
    finally:
        # Dispose the engine to close all connections
        engine.dispose()
        print("Connection closed.")


def check_table_exists(table_name, username, password, host='localhost', port='5432'):
    try:
        # Create an engine
        dbname = f"{username}_stock_db"
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        
        # Use SQLAlchemy's inspector to check if the table exists
        inspector = inspect(engine)
        table_exists = table_name in inspector.get_table_names()
        
        if table_exists:
            print(f"Table '{table_name}' exists in the database '{dbname}'.")
        else:
            print(f"Table '{table_name}' does not exist in the database '{dbname}'.")

        return table_exists

    except SQLAlchemyError as e:
        print(f"An error occurred while checking for the table: {e}")
        return False
    
    finally:
        # Dispose the engine to close the connection
        engine.dispose()
        print("Connection closed.")


def create_table(username, password, database, create_table_sql, host='localhost', port='5432'):
    try:
        connection = psycopg2.connect(
            user=username,
            password=password,
            dbname=database,
            host=host,
            port=port
        )
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            connection.commit()
            print("Table created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        connection.rollback()
    finally:
        if connection:
            connection.close()


#def insert_data(username, password, database, insert_sql, values, host='localhost', port='5432'):
#    try:
#        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
#
#        print(f"db_connect {values}")
#        with engine.connect() as connection:
#            # Ensure insert_sql is a text object
#            if not isinstance(insert_sql, text):
#                insert_sql = text(insert_sql)
#            
#            result = connection.execute(insert_sql, values)
#
#            if result.returns_rows:
#                serial_id = result.fetchone()[0]
#                return serial_id
#            return None
#
#    except SQLAlchemyError as e:
#        print(f"Error occurred during insertion: {e}")
#        return None

# insert data work 2024/10/20
def insert_data(username, password, database, insert_sql, values, host='localhost', port='5432'):
    try:
        # Use SQLAlchemy to create an engine
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

        # Use the engine to connect and execute the SQL insert statement
        with engine.connect() as connection:
            result = connection.execute(text(insert_sql), values)
            connection.commit()

            # If there's a value to return, like a serial ID
            if result.returns_rows:
                serial_id = result.fetchone()[0]  # Assuming RETURNING id returns the id in the first column
                return serial_id
            return None

    except SQLAlchemyError as e:
        print(f"Error occurred during insertion: {e}")
        return None

#def insert_data(username, password, database, insert_sql, values, host='localhost', port='5432'):
#    try:
#        # 使用 SQLAlchemy 建立引擎
#        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
#
#        print(f"db_connect {values}")
#        # 使用引擎連接，執行 SQL 插入語句
#        with engine.connect() as connection:
#            result = connection.execute(text(insert_sql), values)
#
#            # 如果有需要回傳的值，比如 serial ID
#            if result.returns_rows:
#                serial_id = result.fetchone()[0]  # 假設 RETURNING id 返回的第一列是 id
#                return serial_id
#            return None
#
#    except SQLAlchemyError as e:
#        print(f"Error occurred during insertion: {e}")
#        return None


def update_data(username, password, database, update_sql, values, host='localhost', port='5432'):
    try:
        # Use SQLAlchemy to create an engine
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

        # Use the engine to connect and execute the SQL insert statement
        with engine.connect() as connection:
            result = connection.execute(text(update_sql), values)
            connection.commit()

            # If there's a value to return, like a serial ID
            if result.returns_rows:
                serial_id = result.fetchone()[0]  # Assuming RETURNING id returns the id in the first column
                return serial_id
            return None

    except SQLAlchemyError as e:
        print(f"Error occurred during insertion: {e}")
        return None
