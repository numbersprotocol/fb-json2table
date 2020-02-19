# Abstract

   This repository helps you parse JSON files in the Facebook archive to tables so that it is easier to analyze your own Facebook data. In this README, we will first introduce the steps to setup the environment and to use the tool. You may find more information about the reason why you may (or may not) need this tool.


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

## TL;DR (The first example)

After following the [installation guide](https://github.com/numbersprotocol/fb-json2table/wiki/Installation) in the wiki page, you may try to run the following the start the first analysis of your own Facebook data.

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

For more examples, please go to [Hello World and More](https://github.com/numbersprotocol/fb-json2table/wiki/Hello-World-and-More) wiki page).

# More information about fb-json2table

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


You may find our more in the wiki pages:

* [Why Facebook data is difficult to analyze with design concepts of this repo](https://github.com/numbersprotocol/fb-json2table/wiki/Design-Concepts)
* [How we deal with changing format in the Facebook data?](https://github.com/numbersprotocol/fb-json2table/wiki/Deal-with-changing-format)
* [A closer look of the flattened JSON](https://github.com/numbersprotocol/fb-json2table/wiki/Flattened-JSON)
* [Explanation of terms used in this repo](https://github.com/numbersprotocol/fb-json2table/wiki/Explaining-Terms)

# Special thanks
   The project was initiated during the collaboration of Spring App with [Bitmark Inc.](https://github.com/bitmark-inc). 
   If you are not a developer, have no idea how to use this tool but still hope to see your own Facebook data, you can download the Spring App from:
   * Android Play Store [download](https://play.google.com/store/apps/details?id=com.bitmark.spring)
   * iOS App Store [download](https://install.appcenter.ms/orgs/support-zzd0-28/apps/spring-inhouse/distribution_groups/users)
