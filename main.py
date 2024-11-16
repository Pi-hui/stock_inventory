# main.py
#from db_connection import connect_to_db, dump_table, list_tables, list_databases, close_connection
import math
from system_db_handle import *
from datetime import datetime
from db_connection import *
from stock_db import *
from load_stock_list import * #get_stock_name_by_code
from stock_transaction import *
from BankOfTaiwan import *

import pandas as pd
pd.set_option('display.max_rows', None)  # 顯示所有列

"""
transaction stock  table 加入 Net Settlement Amount, 淨結算金額
買入用負數，賣出用正數
"""

user = "ck"
password = "woodgate"
def main_menu():
    """
    主選單函數，用來處理命令行選單選項
    """
    while True:
        print("\n----- Database Console Menu -----")
        print("1. 顯示庫存")
        print("2. 買入股票")
        print("3. 賣出股票")
        print("4. 現金除息")
        print("5. 股票除權")
        print("6. Exit")
        print("T. test function")
        choice = input("Please select an option (1-6): ")

        if choice == '1':
            fetch_group_inventory(user, password)
        elif choice == '2':
            buy_stock()
            #list_all_tables()
        elif choice == '3':
            sell_stock()
            #dump_table_data()
        elif choice == '4':
            cash_dividend()
            #list_all_databases()
        elif choice == '5':
            stock_dividend()
            #close_db_connection()
        elif choice == '6':
            print("Exiting the program.")
            break
        #elif choice == 'T' or choice == 't':
        #    test_function()
        else:
            print("Invalid choice, please select a valid option.")
    exit(0)

def test_function():
    stock_id = input("代號:")
    market_type = get_marcket_type(stock_id)
    stock_name = get_stock_name_by_code(stock_id)
    print(f"{stock_name} is {market_type}")
    return 

def connect_to_database():
    """
    連接到資料庫的功能
    """
    #username = input("Enter PostgreSQL username: ")
    #password = input("Enter PostgreSQL password: ")
    username = "ck"
    password = "woodgate"
    dbname = input("Enter the name of the database: ")
    connect_to_db(username, password, dbname)

# work 2024/10/31
def list_all_tables(user, password):
    """
    列出資料庫中的所有資料表
    """
    dbname = f"{user}_stock_db"
    list_tables(user, password, dbname)

def dump_table_data():
    """
    導出指定資料表的所有資料
    """
    table_name = input("Enter the table name to dump: ")
    output_format = input("Enter output format (print/csv): ").lower()
    if output_format == 'csv':
        output_file = input("Enter the CSV output file name: ")
        dump_table(table_name, output_format='csv', output_file=output_file)
    else:
        dump_table(table_name, output_format='print')

# work 2024/10/31
def list_all_databases(user, password):
    """
    列出 PostgreSQL 伺服器中的所有資料庫
    """
    list_databases(user, password)

def close_db_connection():
    """
    關閉資料庫連接
    """
    close_connection()

def buy_stock():
    date_string = input("輸入交易日期(YYYYMMDD):")
    date_data = datetime.strptime(date_string, "%Y%m%d").date()

    stock_id = input("代號:")
    user_input = input("股數:")
    quantity = int(user_input)

    user_input = input("價格:")
    price = float(user_input)
    
    amount = quantity * price
    
    default_tax = math.floor(amount * 0.001425 * 0.35)
    input_string = input(f"手續費({default_tax}): ")
    tax = int(input_string) if input_string else default_tax
    
    default_amount = math.floor(amount) + tax
    input_string = input(f"淨收付金額({default_amount}):")
    amount = int(input_string) if input_string else default_amount 
    
     
    print(f"BUY {get_stock_name_by_code(stock_id)} {quantity} @ {price}NTD with tax {tax} = {amount}")
    user_input = input("寫入資料庫(y/n):")
    if user_input.lower() != "y":
        print("未寫入資料庫")
        return
        
    print("寫入資料庫")
    add_buy_transaction(user, password, date_data, stock_id, quantity, price, tax, amount)
    

def sell_stock():
    date_string = input("輸入交易日期(YYYYMMDD):")
    date_data = datetime.strptime(date_string, "%Y%m%d").date()

    stock_id = input("代號:")
    user_input = input("股數:")
    quantity = int(user_input)

    user_input = input("價格:")
    price = float(user_input)
    
    amount = quantity * price
    
    default_tax = math.floor(amount * 0.001425 * 0.35)
    input_string = input(f"手續費({default_tax}): ")
    transaction_tax = int(input_string) if input_string else default_tax

    default_securities_transaction_tax = math.floor(amount * 0.003)
    input_string = input(f"證交稅({default_securities_transaction_tax}): ")
    securities_transaction_tax = int(input_string) if input_string else default_securities_transaction_tax 
    
    default_amount = math.floor(amount) - transaction_tax -securities_transaction_tax
    input_string = input(f"淨收付金額({default_amount}):")
    amount = int(input_string) if input_string else default_amount 
     
    print(f"Sell {get_stock_name_by_code(stock_id)} {quantity} @ {price}NTD with tax {transaction_tax} securities_transaction_tax {securities_transaction_tax} = {amount}")
    user_input = input("寫入資料庫(y/n):")
    if user_input.lower() != "y":
        print("未寫入資料庫")
        return
        
    print("寫入資料庫")
    add_sell_transaction(user, password, date_data, 
        stock_id, quantity, price, transaction_tax, 
        securities_transaction_tax)

def cash_dividend():
    date_string = input("輸入交易日期(YYYYMMDD):")
    date_data = datetime.strptime(date_string, "%Y%m%d").date()

    stock_id = input("代號:")

    input_string = input("除息金額:")
    amount = int(input_string)  
    user_input = input("寫入資料庫(y/n):")
    if user_input.lower() != "y":
        print("未寫入資料庫")
        return

    add_cash_dividend(user, password, date_data, stock_id, amount)


def stock_dividend():
    date_string = input("輸入交易日期(YYYYMMDD):")
    date_data = datetime.strptime(date_string, "%Y%m%d").date()

    stock_id = input("代號:")
    user_input = input("股數:")
    quantity = int(user_input)
    print(f"股票除權 {get_stock_name_by_code(stock_id)} {quantity}")
    user_input = input("寫入資料庫(y/n):")
    if user_input.lower() != "y":
        print("未寫入資料庫")
        return
    
    add_stock_dividend(user, password, date_data, stock_id, quantity)
    
if __name__ == "__main__":
    user
    main_menu()
    #fetch_group_inventory('ck', 'woodgate')
    #list_postgresql_users()
    #atabase_name = 'ck_stock_db'

    #user = 'ck'
    #password = 'woodgate'
    #stock_id = '8299'
    #price = 500
    #quantity = 50
    #date_data = datetime(2024, 11,20)
    #transaction_tax = math.floor(price * quantity * 0.001425 * 0.35)
    #securities_transaction_tax = math.floor(price * quantity * 0.003)
    
    #list_all_tables(user, password)
    #create_stock_id_table(user, password, 'user_ck_stock_db', stock_id)
    #create_stock_id_table(user, password, 'user_ck_stock_db', stock_id)
    #test_insert_stock_data(user, password, 'user_ck_stock_db', 
    #    date_data, stock_id, quantity, price, transaction_tax)
    
    #create_transaction_year_table(user, password, 'user_ck_stock_db', 2024)
    #connect_to_database()
    #exit(0)
    #insert_transaction_year(user, password, 'transaction_stock_2024',  
    #    date_data, stock_id, 0, price, 65, 65)
    #buy_price = 998.25
    #buy_quantity = 2
    #transaction_tax = math.floor((buy_quantity * buy_price * 0.001425) * 0.35)
    #if transaction_tax < 1:
    #    transaction_tax = 1

    #cost = math.floor(buy_price * buy_quantity) + transaction_tax
    #print(f"cost {cost}")

    #print(dump_table(user, password, 'ck_stock_db', 'transactions_year_2024'))
    #print(fetch_group_inventory(user, password))
    #
    #inventory = read_BOT_html_inventory('inventory.html')
    #dump_bot_inventroy(inventory)


    #date_data = datetime(2024, 10,16)
    #add_buy_transaction(user, password, date_data, '8069', 150, 305, 22, 45772)
    #add_buy_transaction(user, password, date_data, '00679B', 1000, 30.3, 20, 30320)
    #date_data = datetime(2024, 10,17)
    #add_buy_transaction(user, password, date_data, '8299', 49, 480.5, 11, 23555)
    #add_buy_transaction(user, password, date_data, '3293', 50, 1080, 26, 54026)
    #date_data = datetime(2024, 10,17)
    #add_buy_transaction(user, password, date_data, '2330', 4, 1088.27, 1, 4354)


    #stock_id = '2'
    #date_data = datetime(2024, 11,23)
    #price = 180
    #quantity = 3000
    #TradeValue = round(price * quantity, 3)
    #transaction_tax = math.floor(TradeValue * 0.001425 * 0.35)
    #securities_transaction_tax = math.floor(TradeValue * 0.003)
    #amount = math.floor(TradeValue + transaction_tax)
    #
    #add_buy_transaction(user, password, date_data, stock_id, quantity, price, 
    #    transaction_tax, amount)

    #date_data = datetime(2024, 11,29)
    #add_sell_transaction(user, password, date_data, 
    #    stock_id, quantity, price, transaction_tax, securities_transaction_tax)
    #insert_transaction_year_sell(user, password, 'ck_stock_db', date_data, 
    #    stock_id, quantity, 102, 3, 5)
    ###stock_table_name = f"transaction_stock_{stock_id}"
    ###connect_to_db('ckyeh', 'woodgate', 'postgres')
    ###if not (check_database_exists(user_database_name, 'ck', 'woodgate')):
    ###    print(f"database {user_database_name} NOT exist")
    ###    create_database(user_database_name, 'ckyeh', 'woodgate')
    ###    assign_database_to_user(user_database_name, user, password)
    ###
    ### 

    
    ##create_stock_id_table(str(2330))
    #name = get_stock_name_by_code('2330')
    #print(name)
   

