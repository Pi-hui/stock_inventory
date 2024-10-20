import psycopg2
from stock_db import *
from db_connection import *
from load_stock_list import get_stock_name_by_code, fetch_stock_list_as_df
import math

# 連接到 PostgreSQL 資料庫
# 插入一筆 "Buy" 交易到 Transactions_Stock_2330，並取得生成的 stock_serial_id
def add_buy_transaction(user, password, date, 
        stock_id, quantity, price, transaction_tax, transaction_cost):
    user_database_name = f"user_{user}_stock_db"
    user_inventory_table = f"inventory"
    user_transaction_stock_table = f"transactions_stock_{stock_id}"
    user_transaction_year_table = f"transactions_year_{date.year}"
    print(f"{user_inventory_table}_{user_transaction_stock_table}_{user_transaction_year_table}")
    connect_to_db(user, password, user_database_name)
    print(check_table_exists(user_transaction_stock_table, user, password))
    if not check_table_exists(user_transaction_stock_table, user, password):
        print(f"table {user_transaction_stock_table} is not exist") 
        create_stock_id_table(user, password, user_database_name, stock_id)

    if not check_table_exists(user_transaction_year_table, user, password):
        print(f"table {user_transaction_year_table} is not exist") 
        create_transaction_year_table(user, password, user_database_name, date.year)

    if not check_table_exists('inventory', user, password):
        print(f"table inventory is not exist") 
        create_inventory_table(user, password, user_database_name)

    serial_id = insert_transaction_stock_buy(user, password, user_database_name, 
        date, stock_id, quantity, price, transaction_tax)
    add_inventory(user, password, user_database_name, 
        date, stock_id, quantity, price, transaction_tax, transaction_cost, serial_id)
    print(f"serial id {serial_id}")


def add_cash_dividend(user, password, date, stock_id, price):
    user_database_name = f"user_{user}_stock_db"
    transID = insert_transaction_stock_cash_dividend(user, password, user_database_name,
        date, stock_id, price)
    insert_transaction_year_cash_dividend(user, password, user_database_name, 
        date, stock_id, price, transID) 
    
def add_stock_dividend(user, password, date, stock_id, quantity):
    user_database_name = f"user_{user}_stock_db"
    transID = insert_transaction_stock_stock_dividend(user, password, user_database_name,
        date, stock_id, quantity)
    add_inventory(user, password, user_database_name, 
        date, stock_id, quantity, 0, 0, 0, transID)
    

def load_sell_info_by_id(df, trans_id, sell_quantity):
    if not trans_id.isdigit():
        print(f"Wrong {trans_id}, please input digit only")
        return False 

    if not sell_quantity.isdigit():
        print(f"Wrong {sell_quantity}, please input digit only")
        return False 

    trans_id = int(trans_id)
    sell_quantity = int(sell_quantity)
    if int(trans_id) not in df['id'].values:
        print("input wrong id, please check again.")
        return False 
    
    quantity_value = df.loc[df['id'] == trans_id, 'quantity'].values[0]
    print(f"id {trans_id} quantity {quantity_value}")
    if quantity_value < sell_quantity:
        print("input wrong quantity, please check again.")
        return False

    return df.loc[df['id'] == trans_id]
    

    
def add_sell_transaction(user, password, date,
        stock_id, quantity, price, transaction_tax, securities_transaction_tax):

    company_name = get_stock_name_by_code(stock_id)
    TransactionAmount = math.floor(price * quantity) 
    NetSettlementAmount = TransactionAmount - transaction_tax - securities_transaction_tax

    stock_df = fetch_stock_inventory(user, password, stock_id)
    print(stock_df.to_string(index=False))  
    total_sell_quantity = quantity
    total_sell_cost = NetSettlementAmount 
    remaining_sell_quantity = quantity
    remaining_sell_cost = NetSettlementAmount
    total_inventroy_quantity = stock_df['quantity'].sum()
    
    transaction_stock_columns = ['id', 'transaction_date','quantity',
        'transaction_price', 'transaction_type', 'transaction_tax', 
        'securities_transaction_tax'] 
    
    inventory_columns = ['id', 'stock_symbol', 'buy_date', 'quantity', 'buy_price',
        'transaction_tax', 'remaining_quantity', 'remaining_cost', 'stock_id_fk']
   
    stock_year_columns = ['id', 'transaction_date', 'stock_symbol', 
        'quantity', 'profit_or_loss', 'buy_id', 'sell_id']  
    
    sell_transaction_fd = pd.DataFrame(columns=transaction_stock_columns)
    sell_transaction_fd.loc[0] = [0, date.strftime('%Y-%m-%d'), 
        quantity, price, 'sell', transaction_tax, securities_transaction_tax]

    transaction_sell_record = pd.DataFrame(columns=transaction_stock_columns)

    entry_stock_year_recode = pd.DataFrame(columns=stock_year_columns)

    remain_inventory_recode = pd.DataFrame(columns=inventory_columns)
    
    if total_inventroy_quantity < quantity:
        print(f"total {stock_id} has {total_inventroy_quantity} in inventory")
        print(f"NOT enough to sell {quantity}, please check again.")
        return False

    sell_transaction_fd['Amount'] = sell_transaction_fd['transaction_price'] * sell_transaction_fd['quantity'] - (
        transaction_tax + securities_transaction_tax)

    print(f"sell stock transaction: ")
    print(sell_transaction_fd)
    print("輸入指定賣出id與數量, 使用\",\"分隔。如賣出id 4, 10股請輸入\'4,10\'")
    

    while remaining_sell_quantity > 0:
        user_input = input(f"剩餘股數 {remaining_sell_quantity}\n id, 數量:")
        values = user_input.split(',')
        values = [value.strip() for value in values]
            
        buy_df = load_sell_info_by_id(stock_df, values[0], values[1])
        if isinstance(buy_df, bool):
            print("wrong")
            continue 

        entry_quantity = int(values[1])
        if entry_quantity > remaining_sell_quantity:
            print(f"Remaining quantity {remaining_sell_quantity}, not enough input {entry_quantity}")
            continue
        
        allocated_buy_rate = entry_quantity / buy_df['remaining_quantity'].iloc[0]
        allocated_buy_cost = math.ceil(allocated_buy_rate * buy_df['remaining_cost'].iloc[0]) 
        buy_id = buy_df['stock_id_fk'].iloc[0]


        remaining_buy_cost = buy_df['remaining_cost'].iloc[0] - allocated_buy_cost
        remaining_buy_quantity = buy_df['remaining_quantity'].iloc[0] - entry_quantity

        remain_inventory_recode.loc[len(remain_inventory_recode)] = buy_df.iloc[0]
        remain_inventory_recode.at[len(remain_inventory_recode) - 1, 'remaining_quantity'] = remaining_buy_quantity
        remain_inventory_recode.at[len(remain_inventory_recode) - 1, 'remaining_cost'] = remaining_buy_cost
        print(remain_inventory_recode)
        print(f"計算損益成本 {entry_quantity} {allocated_buy_cost}")
        
        # transaction year table
        allocated_sell_rate = entry_quantity / remaining_sell_quantity 
        allocated_sell_cost = math.ceil(allocated_sell_rate * remaining_sell_cost)
        remaining_sell_cost = remaining_sell_cost - allocated_sell_cost
        remaining_sell_quantity = remaining_sell_quantity - entry_quantity
        
        profit_or_loss = allocated_sell_cost - allocated_buy_cost
        entry_stock_year_recode.loc[len(entry_stock_year_recode)] = [len(entry_stock_year_recode),  date.strftime('%Y-%m-%d'), 
            stock_id, entry_quantity, profit_or_loss, buy_id, 0]
        print(entry_stock_year_recode)
        print(f"計算損益獲利 {entry_quantity} {allocated_sell_cost}")
        print('-' * 20)

    print(f"total profit or loss: {entry_stock_year_recode['profit_or_loss'].sum()}")
    is_write_to_db = input("Do you want to write to database?(y/n)")
    if is_write_to_db.lower() != "y":
        return

    print("Start to write to DB...")
    # insert transaction stock to get sell ID
    user_database_name = f"user_{user}_stock_db"
    ransaction_sell_id = insert_transaction_stock_sell(user, password, user_database_name,
       date, stock_id, quantity, price, transaction_tax, securities_transaction_tax)
    transaction_sell_id = 3
    print(f"transaction_sell_id: {transaction_sell_id}") 

    # update inventory table
    #for index, row in remain_inventory_recode.iterrows():
    #    print(f"第 {index + 1} 列資料: {row.to_dict()}")
    #    insert_transaction_year_sell(user, password, transaction_year_table_name, date, 
    #        stock_id, row['quantity'], row['profit_or_loss'], row['buy_id'], transaction_sell_id)
    # insert transaction year table
    transaction_year_table_name = f"transactions_year_{date.year}"
    database_name = f"user_{user}_stock_db"
    for index, row in entry_stock_year_recode.iterrows():
        print(f"第 {index + 1} 列資料: {row.to_dict()}")
        insert_transaction_year_sell(user, password, database_name, date, 
            stock_id, row['quantity'], row['profit_or_loss'], row['buy_id'], 
            transaction_sell_id)


def test_insert_stock_data(user, password, dbname, 
        date_data, stock_id, quantity, price, transaction_tax,
        host='localhost', port='5432'):
    table = f"transactions_stock_{stock_id}"
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=dbname
        )
        cursor = connection.cursor()
        
        # Define the SQL query to insert data
        insert_query = f"""
        INSERT INTO {table} (
            transaction_date,
            quantity,
            transaction_price,
            transaction_type,
            transaction_tax) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Execute the query with values
        cursor.execute(insert_query, 
            (date_data.strftime('%Y-%m-%d'),
                quantity,
                price,
                'buy', 
                transaction_tax 
            )
        )
        # Commit the transaction
        connection.commit()
        print("Data inserted successfully")
    
    except Exception as e:
        print(f"Error: {e}")
        if connection:
            connection.rollback()
    
    finally:
        # Close the connection
        if connection:
            cursor.close()
            connection.close()
