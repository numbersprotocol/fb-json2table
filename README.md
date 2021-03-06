# Abstract

   This repository helps you parse JSON files in the Facebook archive to tables so that it is easier to analyze your own Facebook data. In this README, we will first introduce the steps to setup the environment and to use the tool. You may find more information about the reason why you may (or may not) need this tool.


   The project was initiated during the collaboration of Spring App with [Bitmark Inc.](https://github.com/bitmark-inc). 
   If you are not a developer, have no idea how to use this tool but still hope to see your own Facebook data, you can download the Spring App from:
   * Android Play Store [download](https://play.google.com/store/apps/details?id=com.bitmark.spring)
   * iOS App Store [download](https://install.appcenter.ms/orgs/support-zzd0-28/apps/spring-inhouse/distribution_groups/users)

# Setup and start using it

## Requirement

   pandas >= 0.24.1

## Before you start

   In order to analyze your Facebook data, you first need to get a copy of your data from Facebook. Please follow the [instruction](https://www.facebook.com/help/1701730696756992?helpref=hc_global_nav) provided by Facebook and download. [This post](https://www.wired.com/story/download-facebook-data-how-to-read/) also provides good instructions on how to download your own Facebook data and how it looks.


   The downloaded data is a big zip archive with the size from a hundred MB to a few GB. Be patient and unzip the archive. Pick one **JSON file in the archive** as the `PATH_OF_JSON` and execute the sample code below. For example, you can use `$PATH_OF_ARCHIVE/ads/ads_interests.json` as `PATH_OF_JSON` to test the following Hello World sample.


The tree structure of the folder should look like


```
.
├ about_you
│   ├ face_recognition.json
│   ├ friend_peer_group.json
│   └ your_address_books.json
├ ads
│   ├ ads_interests.json
│   └ advertisers_who_uploaded_a_contact_list_with_your_information.json
├ apps_and_websites
│   ├ apps_and_websites.json
│   └ posts_from_apps_and_websites.json
├ calls_and_messages
│   └ no-data.txt
├ comments
│   └ comments.json

...

```

## Setup environment

### Install from PyPI

User-level installation

```
$ pip3 install --user fbjson2table
```

System-level installation

```
$ sudo pip3 install fbjson2table
```

### Install from Source

1. Clone this repository

    ```
    $ git clone https://github.com/numbersprotocol/fb-json2table.git
    ```

1. Export PYTHONPATH

    ```
    $ export PYTHONPATH=$PWD/fb-json2table/:$PYTHONPATH
    ```

    or you may use `virtualenv` to keep the dev environment clean
   
    ```
    $ git clone https://github.com/numbersprotocol/fb-json2table.git
    $ pip3 install virtualenv
    $ virtualenv -p python3 env
    $ source env/bin/activate
    $ (env) python3 setup.py bdist_wheel
    $ (env) pip3 install dist/fbjson2table-1.0.0-py3-none-any.whl
    
    # if you want to run example/examply.py
    $ (env) pip3 install tabulate
    ```

## TL;DR (The Hello World example)

```
from fbjson2table.func_lib import parse_fb_json
from fbjson2table.table_class import TempDFs


json_content = parse_fb_json($PATH_OF_JSON)
temp_dfs = TempDFs(json_content)

for df in temp_dfs.df_list:
    print(df)
```

the example above turns JSON files in your `PATH_OF_JSON` into table-like DataFrame.
and you will be able to start analyzing content in the `df`.

Please note! If your data contains special characters or non-English words, you will need to handle the encoding properly before you can analyze it.

# More introduction about fb-json2table

JSON is not friendly to data scientists, we love tables.

The purpose of this repository is to automate the parsing process of JSON files in the downloaded Facebook archive,
turn those JSON files (not easy-to-analyze) to tables (easy-to-analyze) so that it can be easier for data scientists to analyze. 
This repository is not only for data scientists, the ultimate goal is to reduce the entry barrier for anyone who wants to analyze their own data.

Actually, it can also be used to **turn any data with FB-LIKE JSON structure to table** as long as the data structure if similar.


Here is an example of **FB-LIKE JSON structure**

```
[
  {
    feature_1: feature_1_of_record_1,
    feature_2: feature_2_of_record_1,
    ...
   },
   {
    feature_1: feature_1_of_record_2,
    feature_2: feature_2_of_record_2,
    ...
   },
   ...
]
```

[This simple document](https://github.com/numbersprotocol/fb-json2table/blob/master/dict_list_combination_to_table.txt) shows the logic behind the JSON to Table conversions. If you are not sure whether or not your data is *FB-LIKE*, it might give you some hint.

## why Facebook json is not friendly to analyze?

here is an example of Facebook json: [example_facebook_json](https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_facebook_json.json)

We can find that if we want to analyze the relationship between reaction type ("LIKE" or "WOW") and time by using python,

we have to write code like:

```
timestamp = [x["timestamp"] for x in example_json["reactions"]]
reaction_type = [x["data"][0]["reactions"]["reactions"] for x in example_json["reactions"]]
```

We have to specify many keys in our code.
Furthermore, make things more difficult is that, if one record does not have one feature, in Facebook json, it will not display instead of displaying "null"
, and we do not really know how many features Facebook records. 

Or at least, I do not find a formal document writed kinds of data recorded by Facebook.

Take a look of [example_json_content](https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_json_content.json), this is example of `your_post.json`, we can
find that if one post does not have photo, that post will not have features of photo. If the json is too long for naked eyes, we may ignore some interesting 
data recorded by Facebook!

Finally, the most bothering and making automation almost impossible is that, the structure of Facebook json may change, and have changed! And the worst is that,

#### !!!!Facebook will not notify you!!!!

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
    
    here is example of [json_content](https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_json_content.json)
    
    here is example of [TempDFs.df_list and TempDFs.table_name_list](https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_df_list.txt)
    
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
   
   `dummy_dict = {"a": "b", "c":{"aa": "bb", "cc": "d"}}`,
   
   "a" is at depth 0, "aa" is at depth 1.
   
   Because in normal method, if we want to get "b" or "bb", we should write `dummy_dict["a"]` or `dummy_dict["c"]["aa"]`.
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
   
# Automation

   ## TL;DR
   
   For most case of Facebook json
   
   1.
   
   ```
   from fbjson2table.func_lib import parse_fb_json
   from fbjson2table.table_class import TempDFs


   json_content = parse_fb_json($PATH_OF_JSON)
   temp_dfs = TempDFs(json_content)
   one_to_one_df, _ = temp_dfs.temp_to_wanted_df(
                 wanted_columns=[]
                 ) 
   ```

   Take a look of one_to_one_df, and determine which columns we want.
   
   ```
   print(one_to_one_df.columns)
   ```
   
   2.
   
   ```
   wanted_columns = LIST_OF_WANTED_COLUMNS

   df, top_id = temp_dfs.temp_to_wanted_df(
            wanted_columns=wanted_columns
        )
   ```
   
   Then "df" is what we want.
   
   ## Take a look of `temp_to_wanted_df`
   
   For the reason that Facebook may change json structure, we developed a series of methods to handle this problem.
   
   The core concept is that, "putting all things in one table, then from the table take what we want."
   
   Take a look of: [link](https://github.com/numbersprotocol/fb-json2table/blob/3f5f4b8741727c8dd2aa3f4f95030e3f23d53554/fbjson2table/table_class.py#L529)
   
   The first thing we do is `get_routed_dfs`.
   
   => Because that "df" in "df_list" may diverge, we have to get the dfs have the same "route" of "top_df".
   
   Second, we do `get_start_peeling`.
   
   => We have to find the index of top_df in df_list for next step.
   
   Then, we do `merge_one_to_one_sub_df`.
   
   => Merge all one-to-one sub_dfs.
   
   Finally, we do `get_wanted_columns`.
   
   => Extract the columns what we want, and return NaN when the column do not exist.
   
   In `temp_to_wanted_df`, there are four parameters we should specify, `temp_to_wanted_df`, `wanted_columns`,`route_by_table_name`, `start_by`, and `regex`.
   
   So, how to input these value?
   
   The `wanted_columns` is a list of names of columns we want, or we can input `[]`, it will return all columns.
   
   The `route_by_table_name` is the last table name suffix of top_df. 
   
   It is be used when we want to choose one df as top_df, and that df is in a diverging branch of df_list.
   
   Take [example_df_list](https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_df_list.txt) for example,
   
   If we plot the relationship of df_list:
   
   ![Image of relation of df_list](https://github.com/numbersprotocol/fb-json2table/blob/master/images/example_df_list_relationship.png)
   
   We can find there are two route in the picture. When we want to merge `temp__attachments__data__media` and its sub_dfs, we have to 
   extract them in df_list. Or `temp__data` may be a problem when we do the `join` operation.
   
   When we set `route_by_table_name` eqauls `"media"`, we will get the route like:
   
   ![Image of route media](https://github.com/numbersprotocol/fb-json2table/blob/master/images/route_of_temp__attachments__data__media.png)
   
   The `start_by` is the unique column name of top_df in routed_df_list.
   
   When we do `join` operation, we have to choose one df in df_list as our top_df. Take the [example_df_list](https://github.com/numbersprotocol/fb-json2table/blob/master/example/example_df_list.txt), if we want to set "temp__attachments__data__media" as top_df, we have to find the unique column name of it. For example, "uri". By the way, the "title" will not work because "title" also appear in "temp."
   
   The `regex` is whether to use "regex" when find `start_by`.
   
   Thus, if we want get the "creation_timestamp", "description", "title", "uri", and "upload_ip" of photos of posts in one table, the whole process will be like:
   
   ```
   from fbjson2table.func_lib import parse_fb_json
   from fbjson2table.table_class import TempDFs


   json_content = parse_fb_json($PATH_OF_JSON)
   temp_dfs = TempDFs(json_content)
   one_to_one_df, _ = temp_dfs.temp_to_wanted_df(
                      route_by_table_name='media',
                      strat_by='uri'
                      )
   print(one_to_one_df.columns)
   ```
   
   get
   
   ```
   Index(['creation_timestamp', 'description', 'id_0', 'id_attachments_1',
       'id_data_2', 'id_media_3', 'title', 'uri', 'media_metadata_id_0',
       'media_metadata_id_attachments_1', 'media_metadata_id_data_2',
       'media_metadata_id_media_metadata_4', 'photo_metadata_id_0',
       'photo_metadata_id_attachments_1', 'photo_metadata_id_data_2',
       'photo_metadata_id_media_metadata_4',
       'photo_metadata_id_photo_metadata_5', 'photo_metadata_upload_ip'],
      dtype='object')
   ```
   
   Then,
   
   ```
   wanted_columns = ['creation_timestamp', 'description', 'title', 'uri',
                     'photo_metadata_upload_ip']
   photo_df, _ = temp_dfs.temp_to_wanted_df(
                          wanted_columns=wanted_columns,
                          route_by_table_name='media',
                          strat_by='uri'
                      )
   ```
   
   The `photo_df` is what we want.
