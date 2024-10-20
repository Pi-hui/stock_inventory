import psycopg2
import pandas as pd
import csv
from sqlalchemy import create_engine, text
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



def check_table_exists(table_name, username, password, host='localhost', port='5432'):
    try:
        connection = get_connection()
        cursor = connection.cursor()

        # 查詢表是否存在
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """
        cursor.execute(check_table_query, (table_name,))
        exists = cursor.fetchone()[0]

        if exists:
            print(f"Table '{table_name}' exists.")
        else:
            print(f"Table '{table_name}' does not exist.")
        
        return exists

    except psycopg2.Error as e:
        print(f"Error while checking if table exists: {e}")

    finally:
        if cursor:
            cursor.close()

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

def insert_data(username, password, database, insert_sql, values, host='localhost', port='5432'):
    try:
        # Use SQLAlchemy to create an engine
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

        print(f"db_connect {values}")
        # Use the engine to connect and execute the SQL insert statement
        with engine.connect() as connection:
            result = connection.execute(text(insert_sql), values)

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


def update_data(update_sql, values):
    connection = get_connection()
    if connection is None:
        print("No connection to database. Please connect first.")
        return

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_sql, values)
            connection.commit()
            print("Data updated successfully.")
    except psycopg2.Error as e:
        print(f"Error updating data: {e}")
        connection.rollback()

"""
"""
