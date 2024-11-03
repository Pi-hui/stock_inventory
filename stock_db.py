import psycopg2
from db_connection import *
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from load_stock_list import get_stock_name_by_code, get_stock_list 


db_system_name = 'ck'
db_system_password = 'woodgate'
db_default_database = 'postgres'

def create_stock_user(new_user_name, new_user_passowrd):
    create_user(db_system_name, db_system_password, new_user_name, new_user_passowrd)
    
def create_stock_db(database_name):
    create_database(database_name, db_system_name, db_system_password)

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
def delete_inventory(user, password, inventory_id):
    dbname = f"{user}_stock_db"
    delete_inventory_id = {'id': inventory_id}
    delete_sql = "DELETE FROM inventory WHERE id = :id;"
    update_data(user, password, dbname, delete_sql, delete_inventory_id)

# Function to update inventory details
def update_inventory(user, password, remain_quantity, remain_cost, inventory_id):

    update_inventory_data = {
        'new_quantity': remain_quantity, 
        'new_cost': remain_cost, 
        'id': inventory_id}

    dbname = f"{user}_stock_db"
    print(f"update_inventory_data {update_inventory_data}")

    update_sql = """
    UPDATE inventory
    SET remaining_quantity = :new_quantity, 
        remaining_cost = :new_cost 
    WHERE id = :id;
    """
    update_data(user, password, dbname, update_sql, update_inventory_data)

# Function to add new inventory record
def add_inventory(user, password, database, date, stock_id, quantity, price, tax, cost, stock_fk):
    inventory_data = {'id':stock_id, 
            'date': date.strftime('%Y-%m-%d'), 
            'quantity': quantity, 
            'price': price, 
            'tax': tax, 
            'cost': cost, 
            'fk': stock_fk}
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
        VALUES (:id, :date, :quantity, :price, :tax, :quantity, :cost, :fk)
        RETURNING id;
    """
    insert_data(user, password, database, insert_sql, inventory_data)


def insert_transaction_stock_buy(user, password, database, 
        date, stock_id, quantity, price, transaction_tax):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_buy_data = {'date': date.strftime('%Y-%m-%d'), 
        'quantity': quantity, 
        'price':    price, 
        'type':     'buy', 
        'transaction_tax': transaction_tax}
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        quantity, 
        transaction_price, 
        transaction_type,
        transaction_tax 
        ) VALUES (
            :date, :quantity, :price, :type, :transaction_tax)
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

def insert_transaction_stock_stock_dividend(user, password, database, date, stock_id, quantity):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_stock_dividend_data = {'date': date.strftime('%Y-%m-%d'), 
        'quantity': quantity, 
        'type': 'dividend'}
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        quantity, 
        transaction_type
        ) VALUES (:date, :quantity, :type)
        RETURNING id;
    """
    serial_id = insert_data(user, password, database, insert_sql, insert_stock_dividend_data)
    return serial_id

def insert_transaction_stock_cash_dividend(user, password, database, date, stock_id, price):
    transaction_table = f"transactions_stock_{stock_id}"
    insert_buy_data = {'date': date.strftime('%Y-%m-%d'), 
            'price': price, 
            'type':  'dividend'}
    
    insert_sql = f"""
    INSERT INTO {transaction_table} (
        transaction_date, 
        transaction_price, 
        transaction_type
        ) VALUES (:date, :price, :type)
        RETURNING id;
    """
    serial_id = insert_data(user, password, database, insert_sql, insert_buy_data)
    return serial_id
    
def insert_transaction_year_cash_dividend(user, password, database, date, stock_id, profit_or_loss, sellID):
    transaction_table = f"transactions_year_{date.year}"
    insert_year_data = {'date': date.strftime('%Y-%m-%d'), 
        'id': stock_id, 
        'profit_or_loss': profit_or_loss, 
        'sell_id': sellID}
    
    insert_sql = f"""
        INSERT INTO {transaction_table} (
            transaction_date, 
            stock_symbol,
            profit_or_loss, 
            sell_id
            ) VALUES (:date, :id, :profit_or_loss, :sell_id)
            RETURNING id;
        """
    serial_id = insert_data(user, password, database, insert_sql, insert_year_data)
    return serial_id

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
    database_name = f"{user}_stock_db"
    df = dump_table(user, password, database_name, 'inventory')
    return df 

def fetch_group_inventory(user, password):
    database_name = f"{user}_stock_db"
    stock_list = get_stock_list()
    df = dump_table(user, password, database_name, 'inventory')
    grouped_df = df.groupby('stock_symbol').agg(
            total_remaining_cost=('remaining_cost', 'sum'),
            total_remaining_quantity=('remaining_quantity', 'sum')
            ).reset_index()

    # 計算平均成本
    grouped_df['average_cost'] = grouped_df['total_remaining_cost'] / grouped_df['total_remaining_quantity']
    grouped_df['average_cost'] = grouped_df['average_cost'].round(1)
    grouped_df['name'] = grouped_df['stock_symbol'].map(stock_list.set_index('stock_code')['stock_name'])
    #grouped_df = grouped_df[['stock_symbol', 'name', 'total_remaining_cost', 'total_remaining_quantity', 'average_cost']]
    grouped_df['name'] = grouped_df['name'].str.ljust(grouped_df['name'].str.len().max())

    print(grouped_df.to_string(index = False))
    return grouped_df

def fetch_stock_inventory(user, password, stock_symbol):
    database_name = f"{user}_stock_db"
    df = dump_table(user, password, database_name, 'inventory')
    # 篩選 stock_symbol 
    filtered_df = df[df['stock_symbol'] == stock_symbol]
    return filtered_df





