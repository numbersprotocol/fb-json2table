# fb-json2table
Parse Facebook archive JSON files to tables.

# requirement

pandas >= 0.24.1

# Setup
clone this repo

`git clone https://github.com/numbersprotocol/fb-json2table.git`

export path

`export PYTHONPATH=$PWD/fb-json2table/:$PYTHONPATH`

# TL;DR

```
from fbjson2table.func_lib import parse_fb_json
from fbjson2table.table_class import TempDFs


json_content = parse_fb_json($PATH_OF_JSON)
temp_dfs = TempDFs(json_content)

for df in temp_dfs.df_list:
    print(df)
```

and you will find that all the content in json turned to table-like(DataFrame)

# Introduction
This repo is based on the requierments of Numbers(https://github.com/numbersprotocol).

The final goal of this repo is to automate the parsing process of json in downloaded facebook data.

Thus, if you also want to analyze your own facebook data or you have some json that structure is like facebook data,

this repo can help you to turn the difficult to analyze json to easier to analyze table.

Note: the mean of structure is like facebook:

```
[
  {
    feature_1: feature_1_of_record_1,
    feature_2: feature_2_of_record_1,
    ...
   },
   {
    feature_1: feature_1_of_record_2,
    feature_2: feature_2_of_record_1,
    ...
   },
   ...
]
```

or you can refer to how this repo turn dict and list combination to table:

## why Facebook json is not friendly to analyze?

here is an example of Facebook json:

https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_facebook_json.json

We can find that if we want to analyze the relationship between reaction type ("LIKE" or "WOW") and time by using python,

we have to write code like:

```
timestamp = [x["timestamp"] for x in example_json["reactions"]]
reaction_type = [x["data"][0]["reactions"]["reactions"] for x in example_json["reactions"]]
```

We have to specify many keys in our code.
Furthermore, make things more difficult is that, if one record does not have one feature, in facebook json, it will not display instead of displaying "null"
, and we do not really know how many features Facebook records. Or at least, I do not find a formal document writed kinds of data recorded by Facebook.
Take a look of https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_json_content.json, this is example of your_post.json, we can
find that if one post does not have photo, that post will not have features of photo. If the json is too long for naked eyes, we may ignore some interesting 
data recorded by Facebook!

Finally, the most bothering and making automation almost impossible is that, the structure of Facebook json may change, and have changed! And the worst is that.
Facebook will not notice you!

For example, the data I download long time ago, I can find posts in "posts/your_posts.json/", and the content is like:

```
{
  "status_updates": [
    {
      "timestamp": 1415339550,
      ...
```

The data I download recently, if I want to find posts, I should go to "posts/your_posts_1.json/", the filename have changed, and the content is like:

```
[
  {
    "timestamp": 1575375973,
    ...
```

We can find that in new structure, we do not have to and shoud not to specify "status_updates", and if we load new json into our old code, it will raise many
"KeyError". Furthermore, there may be other changing in the json content.

## the goal of this repo

1. turn the Facebook json to table

2. decrease the things should be speficfied

3. make the code robust to changing of json structure, or make it easier to fix when json structure changes

# How to use

1. load json

    you can load json by your own method, or use the function we write for Facebook json to handling mojibake.
    
    ```
    from fbjson2table.func_lib import parse_fb_json
    
    json_content = parse_fb_json($PATH_OF_JSON)
    ```
    
2. feed it into "TempDFs", and take a look of "TempDFs.df_list" and "TempDFs.table_name_list",

    ```
    from tabulate import tabulate
    from fbjson2table.table_class import TempDFs
    
    temp_dfs = TempDFs(json_content)
    for df, table_name in zip(temp_dfs.df_list, temp_dfs.table_name_list):
        print(table_name, ':')
        print(tabulate(df, headers='keys', tablefmt='psql'), '\n')
    ```
    
    here is example of json_content: https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_json_content.json
    
    here is example of TempDFs.df_list and TempDFs.table_name_list: https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_df_list.txt
    
    #### explanation: 
    
    Every df has its own name, the first df is default named with "temp", for the follownig dfs will concat "__DICT_KEY " 
    as suffix.
    
    Every df has id of its own "depth(peeling)", and all ids of connected upper layer. The id of first depth is always named "id_0", and the
    following id is named with "id_DICT_KEY_DEPTH", example: "id_attachment_1".
    
    With the ids, we can do the "join" operation. For example, if we want to put "uri" of "media" and "timestamp" of posts in same table,
    the code will like:
    
    ```
    top_df = temp_posts_dfs[0].set_index("id_0", drop=False)
    append_df = temp_posts_df[4].set_index("id_0", drop=False)
    
    wanted_df = top_df.join(append_df) # What we want
    ```

    If you are lazy to find where is the data you want, and you confirm that the data is one-to-one relationship with "top_df",
    you can use "merge_one_to_one_sub_df."
    
    example:
    
    ```
    one_to_one_df = temp_dfs.merge_one_to_one_sub_df(
                    temp_dfs.df_list,
                    temp_dfs.table_name_list,
                    temp_dfs.id_column_names_list,
                    start_peeling=0) # start_peeling is the index of df we want to set as "top_df" in df_list
    ```
    
    note: in the "one_to_one_df", all column names of sub dfs will concat its depth dict key as prefix. For example,
    "id_media_3" => "media_id_media_3".
    
## explanation of terms

   #### depth(peeling)
   
   In the json, every dict will add one depth(peeling). We count depth from 0.
   
   For example, 
   
   `dummy_dict = {"a": "b", "c":{"aa": "bb", "cc": "d"}}` ,
   
   "a" is at depth 0, "aa" is at depth 1.
   
   Because in normal method, if we want to get "b" or "bb", we should write "dummy_dict["a"]" or "dummy_dict["c"]["aa"].
   We have to specify 1 or 2 keys, so the depth is 0 or 1.
   
   #### top_df, sub_df
   
   Sub_df are those dfs with table name containing the table name of the specifics. For example, "temp__attachments__data", "temp__attachments__data__media" and so on, are sub_dfs of "temp__attachments".
   
   The sub_df can be viewd as one column but recording mutilple value of one df.
   
   Take "dummy_dict" as example, this repo will turn it into,
   
   ```
   temp
   id_0| a |
   ----+---+
     0 | b |      
   ----+---+

   temp_c(table name)
   id_0|id_c_1| aa | cc |
   ----+------+----+----+
     0 |  0   | bb | d  |
   ----+------+----+----+
   ```
   
   but it can be viewed as
   
   ```
   temp
   id_0| a | c                      |
   ----+---+------------------------+
     0 | b |{"aa": "bb", "cc": "d"} |    
   ----+---+------------------------+
   ```

   The "temp_c" is like something growing from "temp", so I call it sub_df.
   
   Top_df refers to the base df when we want to merge sub_df.
