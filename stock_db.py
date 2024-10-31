import psycopg2
from db_connection import *
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

db_system_name = 'ck'
db_system_password = 'woodgate'
db_default_database = 'postgres'

def create_stock_user(new_user_name, new_user_passowrd):
    create_user(db_system_name, db_system_password, new_user_name, new_user_passowrd)
    
def create_stock_db(data_base_name):
    create_database(data_base_name, db_system_name, db_system_password)

# Function to add a yearly transaction (insert data)
def add_year_transaction(year, transaction_data):
    insert_sql = f"""
    INSERT INTO year_{year}_transactions (
        transaction_date, 
        stock_symbol, 
        buy_id, sell_id, 
        quantity, 
        profit_or_loss
    ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    insert_data(insert_sql, transaction_data)

# Function to add stock transaction (insert into a specific stock's table)
def add_stock_transaction(stock_id, transaction_data):
    insert_sql = f"""
    INSERT INTO transactions_{stock_id} (
        transaction_date, 
        quantity, 
        transaction_price, 
        transaction_type, 
        transaction_tax, 
        securities_transaction_tax
    ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    insert_data(insert_sql, transaction_data)

# Function to delete inventory by ID
def delete_inventory(id):
    delete_sql = "DELETE FROM inventory WHERE id = %s;"
    insert_data(delete_sql, (id,))

# Function to update inventory details
def update_inventory(inventory_id, new_values):
    update_sql = """
    UPDATE inventory
    SET remaining_quantity = %s, remaining_cost = %s
    WHERE id = %s;
    """
    update_data(update_sql, (*new_values, inventory_id))

# Function to add new inventory record
def add_inventory(user, password, database, date, stock_id, quantity, price, tax, cost, stock_fk):
    inventory_data = (stock_id, date.strftime('%Y-%m-%d'), quantity, price, tax, quantity, cost, stock_fk)
    insert_sql = """
    INSERT INTO inventory (
        stock_symbol, 
        buy_date, 
        quantity, 
        buy_price, 
        transaction_tax, 
        remaining_quantity, 
        remaining_cost, 
        stock_id_fk)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    insert_data(user, password, database, insert_sql, inventory_data)


def insert_transaction_stock_buy(user, password, database, 
        date, stock_id, quantity, price, transaction_tax):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_buy_data = (date.strftime('%Y-%m-%d'), quantity, price, 'buy', transaction_tax)
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        quantity, 
        transaction_price, 
        transaction_type,
        transaction_tax 
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
    """
    serial_id = insert_data(user, password, database, insert_sql, insert_buy_data)
    return serial_id

def insert_transaction_year_sell(user, password, database, date, 
        stock_id, quantity, profit_or_loss, buyID, sellID):
    transaction_table = f"transactions_year_{date.year}"
    
    insert_year_data = {
        'transaction_date': date.strftime('%Y-%m-%d'),
        'stock_symbol': stock_id,
        'quantity': quantity,
        'profit_or_loss': profit_or_loss,
        'buy_id': buyID,
        'sell_id': sellID
    }
    
    insert_sql = f"""
        INSERT INTO {transaction_table} (
            transaction_date, 
            stock_symbol,
            quantity,
            profit_or_loss, 
            buy_id,
            sell_id
        ) VALUES (
            :transaction_date, 
            :stock_symbol, 
            :quantity, 
            :profit_or_loss, 
            :buy_id, 
            :sell_id
        ) 
        RETURNING id;
    """
    
    serial_id = insert_data(user, password, database, insert_sql, insert_year_data)
    return serial_id

def insert_transaction_stock_sell(user, password, database, date, stock_id, 
        quantity, price, transaction_tax, securities_transaction_tax):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_sell_data = {
        'transaction_date': date.strftime('%Y-%m-%d'),
        'quantity': quantity,
        'transaction_price': price,
        'transaction_type': 'sell',
        'transaction_tax': transaction_tax,
        'securities_transaction_tax': securities_transaction_tax
    }
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        quantity, 
        transaction_price, 
        transaction_type,
        transaction_tax,
        securities_transaction_tax 
        ) VALUES (
        :transaction_date, 
        :quantity, 
        :transaction_price, 
        :transaction_type,
        :transaction_tax,
        :securities_transaction_tax
        )
        RETURNING id;
    """
    serial_id = insert_data(user, password, database, insert_sql, insert_sell_data)
    return serial_id
#def insert_transaction_stock_sell(user, password, database, date, stock_id, 
#        quantity, price, transaction_tax, securities_transaction_tax):
#    transaction_table = f"transactions_stock_{stock_id}"
#    insert_buy_data = (date.strftime('%Y-%m-%d'), quantity, price, 'sell', 
#        transaction_tax, securities_transaction_tax)
#    
#    insert_sql = f"""
#    INSERT INTO {transaction_table} (
#        transaction_date, 
#        quantity, 
#        transaction_price, 
#        transaction_type,
#        transaction_tax,
#        securities_transaction_tax 
#        ) VALUES (%s, %s, %s, %s, %s, %s)
#        RETURNING id;
#    """
#    serial_id = insert_data(user, password, database, insert_sql, insert_buy_data)
#    return serial_id

def insert_transaction_stock_stock_dividend(user, password, database, date, stock_id, quantity):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_stock_dividend_data = (date.strftime('%Y-%m-%d'), quantity, 'dividend')
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        quantity, 
        transaction_type
        ) VALUES (%s, %s, %s)
        RETURNING id;
    """
    serial_id = insert_data(user, password, database, insert_sql, insert_stock_dividend_data)
    return serial_id

def insert_transaction_stock_cash_dividend(user, password, database, date, stock_id, price):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_buy_data = (date.strftime('%Y-%m-%d'), price, 'dividend')
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        transaction_price, 
        transaction_type
        ) VALUES (%s, %s, %s)
        RETURNING id;
    """
    serial_id = insert_data(user, password, database, insert_sql, insert_buy_data)
    return serial_id
    
def insert_transaction_year_cash_dividend(user, password, database, date, stock_id, profit_or_loss, sellID):
    transaction_table = f"transactions_year_{date.year}"
    insert_year_data = (date.strftime('%Y-%m-%d'), stock_id, profit_or_loss, sellID)
    
    insert_sql = f"""
        INSERT INTO {transaction_table} (
            transaction_date, 
            stock_symbol,
            profit_or_loss, 
            sell_id
            ) VALUES (%s, %s, %s, %s)
            RETURNING id;
        """
    serial_id = insert_data(user, password, database, insert_sql, insert_year_data)
    return serial_id

#def insert_transaction_year_sell(user, password, database, date, 
#        stock_id, quantity, profit_or_loss, buyID, sellID, host='localhost', port='5432'):
#    
#    # Build the connection string
#    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
#    
#    # Prepare data for insertion (wrap the tuple in a list)
#    transaction_table = f"transactions_year_{date.year}"
#    #insert_year_data = (date.strftime('%Y-%m-%d'), stock_id, quantity, profit_or_loss, buyID, sellID)
#    insert_year_data = {
#        'transaction_date': date.strftime('%Y-%m-%d'),
#        'stock_symbol': stock_id,
#        'quantity': quantity,
#        'profit_or_loss': profit_or_loss,
#        'buy_id': buyID,
#        'sell_id': sellID
#    }
#    insert_sql = text(f"""
#        INSERT INTO {transaction_table} (
#            transaction_date, 
#            stock_symbol,
#            quantity,
#            profit_or_loss, 
#            buy_id,
#            sell_id
#        ) VALUES (
#            :transaction_date, 
#            :stock_symbol, 
#            :quantity, 
#            :profit_or_loss, 
#            :buy_id, 
#            :sell_id
#        ) 
#        RETURNING id;
#    """)
# 
#    # Define the SQL insert statement
#    #insert_sql = f"""
#    #INSERT INTO {transaction_table} (
#    #    transaction_date, 
#    #    stock_symbol,
#    #    quantity,
#    #    profit_or_loss, 
#    #    buy_id,
#    #    sell_id
#    #) VALUES (%s, %s, %s, %s, %s, %s)
#    #RETURNING id;
#    #"""
#    
#    try:
#        # Connect and execute the SQL statement
#        with engine.connect() as connection:
#            print("database connection")
#            print(f"insert_sql: {insert_sql}")
#            print(f"insert_year_data: {insert_year_data}")
#            result = connection.execute(insert_sql, insert_year_data)
#            serial_id = result.fetchone()[0]  # Get the returning id
#            print(f"serial_id {serial_id}")
#        if serial_id is not None:
#            print(f"Transaction successfully inserted with id: {serial_id}")
#        else:
#            print("Failed to insert transaction")
#        return serial_id
#
#    except Exception as e:
#        print(f"Error during transaction insert: {e}")
#        return None

#def insert_transaction_year_sell(user, password, database, date, 
#        stock_id, quantity, profit_or_loss, buyID, sellID, host='localhost', port='5432'):
#    
#    # Build the connection string
#    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
#    
#    # Prepare data for insertion
#    transaction_table = f"transactions_year_{date.year}"
#    insert_year_data = (date.strftime('%Y-%m-%d'), stock_id, quantity, profit_or_loss, buyID, sellID)
#    
#    # Define the SQL insert statement
#    insert_sql = f"""
#    INSERT INTO {transaction_table} (
#        transaction_date, 
#        stock_symbol,
#        quantity,
#        profit_or_loss, 
#        buy_id,
#        sell_id
#    ) VALUES (%s, %s, %s, %s, %s, %s)
#    RETURNING id;
#    """
#    
#    try:
#        # Connect and execute the SQL statement
#        with engine.connect() as connection:
#            result = connection.execute(insert_sql, insert_year_data)
#            serial_id = result.fetchone()[0]  # Get the returning id
#        return serial_id
#
#    except Exception as e:
#        print(f"Error during transaction insert: {e}")
#        return None

#def insert_transaction_year_sell(user, password, database, date, 
#        stock_id, quantity, profit_or_loss, buyID, sellID):
#    transaction_table = f"transactions_year_{date.year}"
#    print(f"sellID {sellID}")
#    #insert_year_data = (date.strftime('%Y-%m-%d'), stock_id, quantity, profit_or_loss, buyID, sellID)
#    # Prepare the data to insert
#    insert_year_data = {
#        'transaction_date': date.strftime('%Y-%m-%d'),
#        'stock_symbol': stock_id,
#        'quantity': quantity,
#        'profit_or_loss': profit_or_loss,
#        'buy_id': buyID,
#        'sell_id': sellID
#    }
#    insert_sql = f"""
#        INSERT INTO {transaction_table} (
#            transaction_date, 
#            stock_symbol,
#            quantity,
#            profit_or_loss, 
#            buy_id,
#            sell_id
#            ) VALUES (:transaction_date, :stock_symbol, :quantity, :profit_or_loss, :buy_id, :sellID) 
#            RETURNING id;
#        """
#    serial_id = insert_data(user, password, database, insert_sql, insert_year_data)
#    return serial_id

def insert_transaction_year_stock_dividend(user, password, database, date, stock_id, profit_or_loss, sellID):
    transaction_table = f"transactions_year_{date.year}"
    insert_year_data = (date.strftime('%Y-%m-%d'), stock_id, profit_or_loss, sellID)
    
    insert_sql = f"""
        INSERT INTO {transaction_table} (
            transaction_date, 
            stock_symbol,
            profit_or_loss, 
            sell_id
            ) VALUES (%s, %s, %s, %s)
            RETURNING id;
        """
    serial_id = insert_data(user, password, database, insert_sql, insert_year_data)
    return serial_id

# Function to create a table for a specific stock
def create_stock_id_table(user, password, database, stock_id):
    create_stock_id_table_sql = f"""
    CREATE TABLE IF NOT EXISTS transactions_stock_{stock_id} (
        id SERIAL PRIMARY KEY,
        transaction_date DATE NOT NULL,
        quantity INTEGER,
        transaction_price DECIMAL(12, 3),
        transaction_type VARCHAR(12) CHECK (transaction_type IN ('buy', 'sell', 'dividend')),
        transaction_tax DECIMAL(12, 3),
        securities_transaction_tax DECIMAL(12, 3)
    );
    """
    create_table(user, password, database, create_stock_id_table_sql)

# Function to create the inventory table
def create_inventory_table(user, password, database):
    create_inventory_table_sql = """
    CREATE TABLE IF NOT EXISTS inventory (
        id SERIAL PRIMARY KEY,
        stock_symbol VARCHAR(10) NOT NULL,
        buy_date DATE NOT NULL,
        quantity INTEGER NOT NULL,
        buy_price DECIMAL(12, 3),
        transaction_tax DECIMAL(12, 3),
        remaining_quantity INTEGER NOT NULL,
        remaining_cost DECIMAL(12, 3),
        stock_id_fk INTEGER
    );
    """
    create_table(user, password, database, create_inventory_table_sql)

# Function to create a yearly transaction table
def create_transaction_year_table(user, password, database, year):
    create_year_transactions_table_sql = f"""
    CREATE TABLE IF NOT EXISTS transactions_year_{year} (
        id SERIAL PRIMARY KEY,
        transaction_date DATE NOT NULL,
        stock_symbol VARCHAR(10) NOT NULL,
        quantity INTEGER,
        profit_or_loss DECIMAL(12, 3),
        buy_id INTEGER,
        sell_id INTEGER
    );
    """
    create_table(user, password, database, create_year_transactions_table_sql)


def fetch_inventory_data(user, password):
    data_base_name = f"{user}_stock_db"
    df = dump_table(user, password, data_base_name, 'inventory')
    return df 

def fetch_group_inventory(user, password):
    data_base_name = f"{user}_stock_db"
    df = dump_table(user, password, data_base_name, 'inventory')
    grouped_df = df.groupby('stock_symbol').agg(
            total_remaining_cost=('remaining_cost', 'sum'),
            total_remaining_quantity=('remaining_quantity', 'sum')
            ).reset_index()

    # 計算平均成本
    grouped_df['average_cost'] = grouped_df['total_remaining_cost'] / grouped_df['total_remaining_quantity']
    return grouped_df

def fetch_stock_inventory(user, password, stock_symbol):
    data_base_name = f"{user}_stock_db"
    df = dump_table(user, password, data_base_name, 'inventory')
    # 篩選 stock_symbol 
    filtered_df = df[df['stock_symbol'] == stock_symbol]
    return filtered_df



