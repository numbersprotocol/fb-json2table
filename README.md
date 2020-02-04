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
```

We can find that in new structure, we do not have to and shoud not to specify "status_updates", and if we load new json into our old code, it will raise many
"KeyError". Furthermore, there may be other changing in the json content.
