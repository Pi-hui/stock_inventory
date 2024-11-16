import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

#df = None
stock_list = None
def fetch_stock_list_as_df():
    username = 'ck'
    password = 'woodgate'
    host = 'localhost'
    port = 5432
    database = 'stock_info'
    table = 'stock_list'
    try:
        # 使用 SQLAlchemy 建立引擎
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

        fetch_table_sql = f"SELECT * FROM {table};"
        df = pd.read_sql_query(fetch_table_sql, engine)
        return df
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()

    finally:
        engine.dispose()

    return df

def get_stock_name_by_code(stock_code):
    global stock_list  # 聲明 stock_list 為全局變數
    if stock_list is None:  # 檢查 stock_list 是否為 None
        stock_list = fetch_stock_list_as_df()  # 初始化 stock_list

    # 檢查 stock_code 是否存在於 DataFrame 的 'stock_code' 列
    result = stock_list[stock_list['stock_code'] == stock_code]['stock_name']
    
    if not result.empty:  # 檢查結果是否為空
        return result.values[0]  # 返回第一個匹配的股票名稱
    else:
        return None  # 如果找不到，返回 None

def get_stock_list():
    global stock_list  # 聲明 stock_list 為全局變數
    if stock_list is None:  # 檢查 stock_list 是否為 None
        stock_list = fetch_stock_list_as_df()  # 初始化 stock_list
    return stock_list 

def get_marcket_type(stock_code):
    global stock_list  # 聲明 stock_list 為全局變數
    if stock_list is None:  # 檢查 stock_list 是否為 None
        stock_list = fetch_stock_list_as_df()  # 初始化 stock_list

    # 檢查 stock_code 是否存在於 DataFrame 的 'stock_code' 列
    result = stock_list[stock_list['stock_code'] == stock_code]['market_type']
    
    if not result.empty:  # 檢查結果是否為空
        return result.values[0]  # 返回第一個匹配的股票名稱
    else:
        return None  # 如果找不到，返回 None
    
    return
# Example usage:
#df = fetch_stock_list_as_df()

# Example usage:
# df = fetch_stock_list_as_df()  # Assuming you already fetched the DataFrame from the database
#stock_name = get_stock_name_by_code('2330')
#print(stock_name)
#stock_name = get_stock_name_by_code('00867B')
#print(stock_name)
#stock_name = get_stock_name_by_code('4431')
#print(stock_name)
#
