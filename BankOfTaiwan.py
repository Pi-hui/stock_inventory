import pandas as pd
import os
import sys
import numpy as np

# Get the file path from the command line argument
def read_BOT_html_inventory(html_file_path):

    # Extract the directory and file name from the input path
    dir_path = os.path.dirname(html_file_path)
    file_name = os.path.splitext(os.path.basename(html_file_path))[0]

    # Read the HTML file and extract the table
    tables = pd.read_html(html_file_path, encoding='big5')

    # Assuming the first table is the one needed
    df = tables[0]

    # Split the "商品名稱" column into "公司名稱" and "代號"
    df[['公司名稱', '代號']] = df['商品名稱'].str.extract(r'(.+)\((.+)\)')

    # Drop the original "商品名稱" column if needed
    df = df.drop(columns=['商品名稱'])
    df = df[:-1]

    df_filtered = df[['成交日期', '公司名稱', '代號', '成交價','股數','手續費','投資成本']]

    # Convert '成交日期' from string (yyyy/mm/dd) to datetime format
    df_filtered['成交日期'] = pd.to_datetime(df_filtered['成交日期'], format='%Y/%m/%d')
    return df_filtered


def dump_bot_inventroy(inventory):
    for index, row in inventory.iterrows():
        print(f"Record    {index + 1}:")
        print(f"成交日期: {row['成交日期']}")
        print(f"公司名稱: {row['公司名稱']}")
        print(f"代號:     {row['代號']}")
        print(f"成交價:   {row['成交價']}")
        print(f"股數:     {row['股數']}")
        print(f"手續費:   {row['手續費']}")
        print(f"投資成本: {row['投資成本']}")
        print("-" * 40)  # Separator for better readability

