import fbjson2table
from tabulate import tabulate
from fbjson2table.table_class import TempDFs

json_content = [ {
    "timestamp": 1563801255,
    "attachments": [],
    "data": [{"update_timestamp": 1563801255}],
    "title": "Good morning"
  },
  {
    "timestamp": 1543418957,
    "attachments": [],
    "data": [{"update_timestamp": 1543418957}],
    "title": "Good evening"
  },
  {
    "timestamp": 1528528001,
    "attachments": [
                        {"data": [
                           {"media": {"uri": "photos_and_videos/your_posts/fine.jpg",
                                                 "creation_timestamp": 1528527958,
                                           "media_metadata": {"photo_metadata": {"upload_ip": "31.2.7.58"}},
                                           "title": "",
                                           "description": "I am fine"}}]}
    ],
    "data": [{"post": "hey"}],
    "title": "thank you"
 }
]

temp_dfs = TempDFs(json_content)

for df, table_name in zip(temp_dfs.df_list, temp_dfs.table_name_list):
    print(table_name,':')
    print(tabulate(df, headers='keys', tablefmt='psql'), '\n')
