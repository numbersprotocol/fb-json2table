import fbjson2table
from tabulate import tabulate
from fbjson2table.table_class import TempDFs
from fbjson2table.func_lib import parse_fb_json

json_content = parse_fb_json("./example_json_content.json")
temp_dfs = TempDFs(json_content)

for df, table_name in zip(temp_dfs.df_list, temp_dfs.table_name_list):
    print(table_name,':')
    print(tabulate(df, headers='keys', tablefmt='psql'), '\n')
