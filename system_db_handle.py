import psycopg2
from psycopg2 import sql

from sqlalchemy import create_engine # For create_engine
db_system_user = 'ckyeh'
db_system_password = 'woodgate'

def create_user_database(new_database_name):
    # 連接到預設的 postgres 資料庫
    connection = psycopg2.connect(
        dbname="postgres",    # 連接到預設的 postgres 資料庫
        user=db_system_user, # 替換為你的 PostgreSQL 用戶名
        password=db_system_password, # 替換為你的 PostgreSQL 密碼
        host="localhost",      # 如果 PostgreSQL 在本地運行，否則替換為正確的主機
        port='5432'
    )

    # 設定自動提交模式
    connection.autocommit = True

    # 建立資料庫
    try:
        cursor = connection.cursor()

        # 使用 SQL 指令來創建資料庫
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(new_database_name)  # 替換為你想要的資料庫名稱
        ))

        print("Database created successfully.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # 關閉游標和連接
        cursor.close()
        connection.close()

def check_database_exists(database_name, username, password, host='localhost', port='5432'):
    try:
        with psycopg2.connect(
            user=username,
            password=password,
            dbname='postgres',
            host=host,
            port=port
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT datname FROM pg_database;")
                databases = cursor.fetchall()

        for db in databases:
            if db[0] == database_name:
                print(f"Database '{database_name}' exists.")
                return True

        print(f"Database '{database_name}' does not exist.")
        return False

    except psycopg2.Error as e:
        print(f"Error while checking database: {e}")
        return False

def list_databases(username, password, host='localhost', port='5432'):
    try:
        with psycopg2.connect(
            user=username,
            password=password,
            dbname='postgres',
            host=host,
            port=port
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
                databases = cursor.fetchall()

        print("Databases on the server:")
        for db in databases:
            print(db[0])

    except psycopg2.Error as e:
        print(f"Error while listing databases: {e}")
    finally:
        # 關閉游標和連接
        cursor.close()
        connection.close()

def list_postgresql_users(host='localhost', port='5432', dbname='postgres'):
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            user=db_system_user,
            password=db_system_password,
            host=host,
            port=port,
            dbname=dbname  # Usually, we connect to the 'postgres' system database
        )
        
        cursor = connection.cursor()
        
        # Query to list all users (roles) from PostgreSQL
        cursor.execute("SELECT rolname FROM pg_roles;")
        
        # Fetch all results
        users = cursor.fetchall()
        
        # Print or return the list of users
        for user in users:
            print(user[0])

    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()

def list_postgresql_users_by_engine(user, password, host='localhost', port='5432', dbname='postgres'):
    # Create an SQLAlchemy engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    
    # SQL query to list all users
    query = "SELECT rolname FROM pg_roles;"
    
    try:
        # Execute the query
        with engine.connect() as connection:
            result = connection.execute(query)
            
            # Fetch and print all users
            users = [row['rolname'] for row in result]
            for user in users:
                print(user)
                
    except Exception as e:
        print(f"Error: {e}")


def create_user(username, password, new_user, new_user_password, host='localhost', port='5432'):
    try:
        # 使用 'postgres' 數據庫建立初始連接
        connection = psycopg2.connect(
            user=username,
            password=password,
            dbname='postgres',
            host=host,
            port=port
        )
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        # 創建用戶
        cursor.execute(f"CREATE USER {new_user} WITH PASSWORD '{new_user_password}';")
        print(f"User '{new_user}' created successfully.")

    except psycopg2.Error as e:
        print(f"Error while creating user: {e}")
        connection.rollback()
    
    finally:
        if cursor:
            cursor.close()

def assign_database_to_user(database_name, username, password, host='localhost', port='5432'):
    try:
        # 使用 'postgres' 數據庫建立初始連接
        connection = psycopg2.connect(
            user=username,
            password=password,
            dbname='postgres',
            host=host,
            port=port
        )
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with connection.cursor() as cursor:
            # 授予權限
            cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {database_name} TO {username};")
            print(f"Granted privileges to user '{username}' on database '{database_name}'.")

    except psycopg2.Error as e:
        print(f"Error while creating database and user: {e}")
    finally:
        if connection:
            connection.close()
