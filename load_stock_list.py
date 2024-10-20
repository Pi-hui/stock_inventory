import psycopg2
import pandas as pd

#df = None
stock_list = None
def fetch_stock_list_as_df():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="stock_info",  # Use your 'stock_info' database name
        user="ck",
        password="woodgate"
    )

    # SQL query to fetch all data from 'stock_list' table
    query = "SELECT * FROM stock_list;"

    # Use pandas to execute the query and fetch data into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()

    return df

def get_stock_name_by_code(stock_code):
    global stock_list  # 聲明 stock_list 為全局變數
    print("In get_stock_name_by_code")
    if stock_list is None:  # 檢查 stock_list 是否為 None
        stock_list = fetch_stock_list_as_df()  # 初始化 stock_list

    # 檢查 stock_code 是否存在於 DataFrame 的 'stock_code' 列
    result = stock_list[stock_list['stock_code'] == stock_code]['stock_name']
    
    if not result.empty:  # 檢查結果是否為空
        return result.values[0]  # 返回第一個匹配的股票名稱
    else:
        return None  # 如果找不到，返回 None

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
