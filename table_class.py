import json
import argparse
import os
import pandas as pd
import re


class TempDFs(object):

    def __init__(self,
                 json_content,
                 table_prefix='temp'
                 ):

        self.df_list, self.table_name_list, \
            self.id_column_names_list, self.column_names_list = \
            self.get_temp_dfs(json_content, table_prefix)

        self.debug_df_list, self.debug_table_name_list, \
            self.debug_id_column_names_list, self.debug_column_names_list = \
            None, None, None, None

    def get_temp_dfs(self, json_content, table_name_start, no_peek=False):

        if no_peek:
            row_table_list = self.go_through_dict_list_no_peek(
                json_content, table_names=[table_name_start])
        else:
            row_table_list = self.go_through_dict_list(
                json_content, table_names=[table_name_start])

        table_dict = self.row_table2table_row(row_table_list)
        sorted_table_dict = self.reassign_id(table_dict)
        easier_table_dict = self.make_easier_insert_row(sorted_table_dict)
        table_column_dict = self.get_total_column_names(easier_table_dict)
        for key, dvalue in table_column_dict.items():
            for kkey, ddvalue in dvalue.items():
                easier_table_dict[key][kkey] = ddvalue

        df_list = []
        table_name_list = []
        id_column_names_list = []
        column_names_list = []

        temp_df = pd.DataFrame()
        for key, dvalue in easier_table_dict.items():
            temp_df = temp_df.from_records(dvalue['row_list'])
            df_list.append(temp_df)
            table_name_list.append(dvalue['table_names'][-1])
            id_column_names_list.append(dvalue['id_column_names'])
            column_names_list.append(dvalue['column_names'])

        df_list, table_name_list, \
            id_column_names_list, column_names_list = \
            self.sort_by_id_len(df_list, table_name_list,
                                id_column_names_list, column_names_list)

        return df_list, table_name_list, \
            id_column_names_list, column_names_list

    def go_through_dict_list(self, target, id_column_names=['id_0'], peeling=0,
                             id_counters=[0], table_names=['unnamed'],
                             id_count_from=0):
        # the method that get the key-value pair,
        # and record the structure of getting the pair

        results = []

        if isinstance(target, dict):

            column_names = []
            values = []

            for key, dvalue in target.items():

                if isinstance(dvalue, dict) | isinstance(dvalue, list):
                    temp = id_column_names + \
                        ['id_' + key + '_' + str(peeling + 1)]
                    temp2 = table_names + [table_names[-1] + '__' + str(key)]

                    # if the dvalue is a list containing no dict or list
                    if isinstance(dvalue, list) & \
                       (True not in [isinstance(x, dict) for x in dvalue]) & \
                       (True not in [isinstance(x, list) for x in dvalue]):

                        for dva in dvalue:
                            results = results + \
                                self.go_through_dict_list(
                                    {key: dva},
                                    id_column_names=temp,
                                    peeling=peeling + 1,
                                    table_names=temp2,
                                    id_counters=id_counters + [id_count_from],
                                    id_count_from=id_count_from)
                    else:
                        results = results + \
                            self.go_through_dict_list(
                                dvalue,
                                id_column_names=temp,
                                peeling=peeling + 1,
                                table_names=temp2,
                                id_counters=id_counters + [id_count_from],
                                id_count_from=id_count_from)

                else:
                    column_names.append(key)
                    values.append(dvalue)

            results = results + [[table_names, id_column_names,
                                  column_names, id_counters,
                                  values, peeling]]

            return results

        elif isinstance(target, list):

            pre_id_counter = id_count_from

            if False not in [isinstance(x, dict) for x in target]:
                dict_keys = []
                for targ in target:
                    for targ_key in targ.keys():
                        dict_keys.append(targ_key)

                appear_times = list(map(dict_keys.count, dict_keys))
                if sum(appear_times) == len(appear_times):
                    new_targ = {}
                    for targ in target:
                        for targ_key, targ_value in targ.items():
                            new_targ[targ_key] = targ_value
                    target = [new_targ]

            for targ in target:
                id_counters = id_counters + []
                id_counters[-1] = pre_id_counter

                if isinstance(targ, dict):
                    results = results + \
                        self.go_through_dict_list(
                            targ,
                            id_column_names=id_column_names,
                            peeling=peeling,
                            id_counters=id_counters,
                            table_names=table_names,
                            id_count_from=id_count_from)

                    pre_id_counter += 1

                else:
                    raise Exception()

            return results

    def go_through_dict_list_no_peek(self, target,
                                     id_column_names=['id_0'], peeling=0,
                                     id_counters=[0], table_names=['unnamed'],
                                     id_count_from=0):
        # the method that get the key-value pair,
        # and record the structure of getting the pair
        # but no peek the value

        results = []

        if isinstance(target, dict):

            column_names = []
            values = []

            for key, dvalue in target.items():

                if isinstance(dvalue, dict) | isinstance(dvalue, list):
                    temp = id_column_names + \
                        ['id_' + key + '_' + str(peeling + 1)]
                    temp2 = table_names + [table_names[-1] + '__' + str(key)]

                    # if the dvalue is a list containing no dict or list
                    if isinstance(dvalue, list) & \
                       (True not in [isinstance(x, dict) for x in dvalue]) & \
                       (True not in [isinstance(x, list) for x in dvalue]):

                        for dva in dvalue:

                            results = results + \
                                self.go_through_dict_list_no_peek(
                                    {key: "be elegant"},
                                    id_column_names=temp,
                                    peeling=peeling + 1,
                                    table_names=temp2,
                                    id_counters=id_counters + [id_count_from],
                                    id_count_from=id_count_from)
                    else:
                        results = results + \
                            self.go_through_dict_list_no_peek(
                                dvalue,
                                id_column_names=temp,
                                peeling=peeling + 1,
                                table_names=temp2,
                                id_counters=id_counters + [id_count_from],
                                id_count_from=id_count_from)

                else:
                    column_names.append(key)
                    values.append("be elegant")

            results = results + [[table_names, id_column_names,
                                  column_names, id_counters,
                                  values, peeling]]

            return results

        elif isinstance(target, list):

            pre_id_counter = id_count_from

            if False not in [isinstance(x, dict) for x in target]:
                dict_keys = []
                for targ in target:
                    for targ_key in targ.keys():
                        dict_keys.append(targ_key)

                appear_times = list(map(dict_keys.count, dict_keys))
                if sum(appear_times) == len(appear_times):
                    new_targ = {}
                    for targ in target:
                        for targ_key, targ_value in targ.items():
                            new_targ[targ_key] = targ_value
                    target = [new_targ]

            for targ in target:
                id_counters = id_counters + []
                id_counters[-1] = pre_id_counter

                if isinstance(targ, dict):
                    results = results + \
                        self.go_through_dict_list_no_peek(
                            targ,
                            id_column_names=id_column_names,
                            peeling=peeling,
                            id_counters=id_counters,
                            table_names=table_names,
                            id_count_from=id_count_from)

                    pre_id_counter += 1

                else:
                    print(type(targ))
                    print("something with no column name")
                    raise Exception()

            return results

    def row_table2table_row(self, row_table):
        # row_table records every single row accompany its table info
        # here we rewrite it into tables with tables' rows

        table_dict = {}
        for table_names, id_column_names, \
                column_names, id_counters, \
                values, peeling in row_table:

            p_column_names = []

            # group is an illegal column_name and causes many problems in SQL
            for column_name in column_names:
                if column_name == 'group':
                    column_name = 'grouq'
                p_column_names.append(column_name)

            if table_names[-1] not in table_dict.keys():
                table_dict[table_names[-1]] = {
                    'id_column_names_list': [id_column_names],
                    'column_names_list': [p_column_names],
                    'values_list': [values],
                    'id_counters_list': [id_counters],
                    'table_names': table_names,
                    'peeling': peeling,
                    'column_name_rows': [p_column_names]}
            else:
                table_dict[table_names[-1]
                           ]['id_column_names_list'].append(id_column_names)
                table_dict[table_names[-1]
                           ]['column_names_list'].append(p_column_names)
                table_dict[table_names[-1]]['values_list'].append(values)
                table_dict[table_names[-1]
                           ]['id_counters_list'].append(id_counters)
                table_dict[table_names[-1]
                           ]['column_name_rows'].append(p_column_names)

        return table_dict

    def reassign_id(self, table_dict, id_count_from=0):
        reassign_id_table_dict = {}

        # sort the table by how many dicts or lists be unpacked
        table_list = []
        for key, dvalue in table_dict.items():
            table_list.append([key, dvalue.copy()])
        table_list.sort(key=lambda x: x[1]['peeling'])

        # reassign id due to that almost tables' foreign keys will change
        sorted_table_dict = {}
        id_map = {}

        for table_name, table in table_list:

            id_map[table_name] = {}
            present_id = id_count_from
            reassigned_id_counters_list = []

            for id_counters in table['id_counters_list']:
                if len(id_counters) == 1:

                    id_map[table_name][str(id_counters)] = [present_id]
                else:

                    after_id_counters = id_counters + []
                    after_id_counters[-1] = present_id
                    up_id_map = id_map[table['table_names'][-2]]
                    after_id_counters[:-1] = \
                        up_id_map[str(after_id_counters[:-1])]
                    id_map[table_name][str(id_counters)] = after_id_counters

                present_id += 1

                reassigned_id_counters_list.append(
                    id_map[table_name][str(id_counters)])

            table['id_counters_list'] = reassigned_id_counters_list
            reassign_id_table_dict[table_name] = table

        return reassign_id_table_dict

    def make_easier_insert_row(self, table_dict):
        easier_table_dict = {}

        # sort the table by how many dicts or lists be unpacked
        table_list = []
        for key, dvalue in table_dict.items():
            table_list.append([key, dvalue.copy()])
        table_list.sort(key=lambda x: x[1]['peeling'])

        for table_name, table in table_list:
            table['row_list'] = []

            for column_names, values, \
                id_column_names, id_counters in zip(
                    table['column_names_list'], table['values_list'],
                    table['id_column_names_list'], table['id_counters_list']):

                row = {}

                for id_column_name, id_counter in zip(
                        id_column_names, id_counters):
                    row[id_column_name] = id_counter

                for column_name, value in zip(column_names, values):
                    row[column_name] = value

                table['row_list'].append(row)

            easier_table_dict[table_name] = table
        return easier_table_dict

    def get_total_column_names(self, table_dict):
        # here we figure out tables' total id_column_names and column_names

        table_column_dict = {}
        for key, dvalue in table_dict.items():
            id_column_names = []
            table_column_dict[key] = {}
            for i in dvalue['id_column_names_list']:
                for j in i:
                    if j not in id_column_names:
                        id_column_names.append(j)

            column_names = []
            for i in dvalue['column_names_list']:
                for j in i:
                    if j not in column_names:
                        column_names.append(j)

            table_column_dict[key]['id_column_names'] = id_column_names
            table_column_dict[key]['column_names'] = column_names

        return table_column_dict

    def sort_by_id_len(self, df_list, table_name_list,
                       id_column_names_list, column_names_list):

        sorted_df_list = [df for df, id_column_names in
                          sorted(zip(df_list, id_column_names_list),
                                 key=lambda x: len(x[1]))]

        sorted_table_name_list = [table_name for table_name, id_column_names in
                                  sorted(zip(table_name_list,
                                             id_column_names_list),
                                         key=lambda x: len(x[1]))]

        sorted_column_names_list = [column_names for column_names,
                                    id_column_names in
                                    sorted(zip(column_names_list,
                                               id_column_names_list),
                                           key=lambda x: len(x[1]))]
        sorted_id_column_names_list = sorted(id_column_names_list,
                                             key=lambda x: len(x))

        return sorted_df_list, sorted_table_name_list, \
            sorted_id_column_names_list, sorted_column_names_list

    def get_start_peeling(self, df_list, by='timestamp'):
        for i, df in enumerate(df_list):
            if str(by) in df.columns:
                return i
                break

    def merge_all_sub_df(self, df_list, table_name_list, id_column_names_list,
                         start_peeling=0):

        top_df = df_list[start_peeling]

        sub_df_list = df_list[start_peeling + 1:]
        sub_table_name_list = table_name_list[start_peeling + 1:]
        sub_id_column_names_list = id_column_names_list[start_peeling + 1:]

        temp_df = top_df.copy()

        for df, table_name, id_column_names in zip(
                sub_df_list, sub_table_name_list, sub_id_column_names_list):

            temp_df = temp_df.set_index(id_column_names[-2], drop=False)
            append_df = df.drop(
                labels=id_column_names[:-2], axis=1
            ).set_index(id_column_names[-2])

            temp_df = temp_df.join(append_df,
                                   rsuffix='_' + table_name.split('__')[-1])

        return temp_df.reset_index(drop=True)

    def merge_one_to_one_sub_df(
            self,
            df_list,
            table_name_list,
            id_column_names_list,
            start_peeling=0):

        top_df = df_list[start_peeling]
        top_id_column_names = id_column_names_list[start_peeling]

        sub_df_list = df_list[start_peeling + 1:]
        sub_table_name_list = table_name_list[start_peeling + 1:]
        sub_id_column_names_list = id_column_names_list[start_peeling + 1:]

        temp_df = top_df.copy()
        temp_df = temp_df.set_index(top_id_column_names[-1], drop=False)

        for df, table_name, id_column_names in zip(
                sub_df_list, sub_table_name_list, sub_id_column_names_list):
            if top_id_column_names[-1] in id_column_names:
                if all(df[top_id_column_names[-1]].value_counts() < 2):

                    append_df = df.set_index(top_id_column_names[-1])
                    append_df.columns = append_df.columns.map(
                        lambda x: str(table_name.split('__')[-1]) + '_' + x)
                    temp_df = temp_df.join(
                        append_df, rsuffix='_' + table_name.split('__')[-1])

        return temp_df.reset_index(drop=True)

    def get_start_peeling_regex(self, df_list, by='timestamp'):
        for i, df in enumerate(df_list):
            if any(list(map(lambda x: bool(re.match(by, x)), df.columns))):
                return i
                break

    def get_wanted_columns(self, df, wanted_columns=[]):
        returned_df = pd.DataFrame(columns=wanted_columns)
        for column in wanted_columns:
            if column in df.columns:
                returned_df[[column]] = df[[column]]
            else:
                pass
        return returned_df

    def append_date_weekday_column(self, df,
                                   timestamp_column_name='timestamp',
                                   unit='s'):

        df.loc[:, 'date'] = pd.to_datetime(df[timestamp_column_name],
                                           unit=unit)
        df.loc[:, 'weekday'] = df.loc[:, 'date'].dt.weekday
        df.loc[:, 'date'] = df.loc[:, 'date'].dt.tz_localize('UTC')
        df.loc[:, 'date'] = df.loc[:, 'date'].dt.strftime('%Y-%m-%d')

    def get_routed_dfs(self, df_list, table_name_list,
                       id_column_names_list, by_table_name=None):

        if by_table_name is None:
            return df_list, \
                table_name_list, \
                id_column_names_list

        for i, table_name in enumerate(table_name_list):
            if by_table_name in table_name.split('__'):
                anchor = i
                break

        routed_df_list = []
        routed_table_name_list = []
        routed_id_column_names_list = []

        for i in range(0, anchor):
            if table_name_list[i] in table_name_list[anchor]:

                routed_df_list.append(df_list[i])
                routed_table_name_list.append(table_name_list[i])
                routed_id_column_names_list.append(id_column_names_list[i])

        for i in range(anchor, len(table_name_list)):
            if table_name_list[anchor] in table_name_list[i]:
                routed_df_list.append(df_list[i])
                routed_table_name_list.append(table_name_list[i])
                routed_id_column_names_list.append(id_column_names_list[i])

        return routed_df_list, \
            routed_table_name_list, \
            routed_id_column_names_list

    def temp_to_wanted_df(self,
                          wanted_columns=[],
                          route_by_table_name=None,
                          start_by='timestamp',
                          regex=False):
        df_list, \
            table_name_list, \
            id_column_names_list = self.get_routed_dfs(
                self.df_list,
                self.table_name_list,
                self.id_column_names_list,
                by_table_name=route_by_table_name)

        if isinstance(start_by, int):
            start_peeling = start_by

        elif regex:
            start_peeling = self.get_start_peeling_regex(df_list,
                                                         by=start_by)
        else:
            start_peeling = self.get_start_peeling(df_list,
                                                   by=start_by)

        top_id = id_column_names_list[start_peeling][-1]
        one_to_one_df = self.merge_one_to_one_sub_df(
            df_list,
            table_name_list,
            id_column_names_list,
            start_peeling=start_peeling)

        if wanted_columns == []:
            return one_to_one_df, top_id

        else:
            wanted_columns = [top_id] + wanted_columns
            wanted_df = self.get_wanted_columns(one_to_one_df,
                                                wanted_columns)
            return wanted_df, top_id

    def get_debug_structure(self, json_content, table_prefix):
        self.debug_df_list, self.debug_table_name_list, \
            self.debug_id_column_names_list, self.debug_column_names_list = \
            self.get_temp_dfs(json_content, table_prefix, no_peek=True)


class PostsDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='your_posts'):
        super(PostsDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.posts_df, self.posts_top_id = self.get_posts_df()

        except Exception as e:
            print(e, "fail to parse post in your_posts")
            self.get_debug_structure(json_content, table_prefix)
            self.posts_df = None

        try:
            self.media_df, self.media_top_id = self.get_posts_media_df()

        except Exception as e:
            print(e, "fail to parse media in your_posts")
            self.get_debug_structure(json_content, table_prefix)
            self.media_df = None

        try:
            self.posts_df_append_media_attached()
        except Exception as e:
            print(e, "fail to append media_attached in posts")

        try:
            self.place_df, self.place_top_id = self.get_posts_place_df()
        except Exception as e:
            print(e, "fail to parse place in your_posts")
            self.get_debug_structure(json_content, table_prefix)
            self.place_df = None

        try:
            self.tags_df, self.tags_top_id = self.get_posts_tags_df()
        except Exception as e:
            print(e, "fail to parse tags in your_posts")
            self.get_debug_structure(json_content, table_prefix)
            self.tags_df = None

    def get_posts_df(self):

        wanted_columns = ['timestamp',
                          'data_update_timestamp',
                          'title',
                          'data_post',
                          'external_context_url',
                          'external_context_source',
                          'external_context_name',
                          'event_name',
                          'event_start_timestamp',
                          'event_end_timestamp']
        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns)

        df.rename(columns={
            top_id: 'post_id',
            "data_update_timestamp": "update_timestamp",
            "data_post": "post"}, inplace=True)

        self.append_date_weekday_column(df)

        return df, top_id

    def get_posts_media_df(self):

        wanted_columns = ['creation_timestamp',
                          'description',
                          'uri',
                          'title',
                          'thumbnail_uri',
                          self.posts_top_id]

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            route_by_table_name='media',
            start_by='uri')

        df.loc[:, 'filename_extension'] = \
            df['uri'].map(lambda x: os.path.splitext(x)[-1])

        df.rename(columns={
            top_id: 'pm_id',
            'uri': 'media_uri',
            self.posts_top_id: 'post_id'}, inplace=True)

        self.append_date_weekday_column(
            df, timestamp_column_name='creation_timestamp', unit='s')

        return df, top_id

    def posts_df_append_media_attached(self):

        self.posts_df.loc[:, 'media_attached'] = False

        if self.media_df is not None:
            posts_id_with_media = self.media_df['post_id']
            self.posts_df.loc[posts_id_with_media, 'media_attached'] = True

    def get_posts_place_df(self):

        wanted_columns = ['address',
                          'coordinate_latitude',
                          'coordinate_longitude',
                          'name',
                          self.posts_top_id]

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            route_by_table_name='place',
            start_by='address')

        df.rename(columns={
            top_id: 'pp_id',
            'coordinate_latitude': 'latitude',
            'coordinate_longitude': 'longitude',
            self.posts_top_id: 'post_id'}, inplace=True)

        return df, top_id

    def get_posts_tags_df(self):

        wanted_columns = ['tags',
                          self.posts_top_id]

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            start_by='tags')

        df.rename(columns={
            top_id: 'tf_id',
            self.posts_top_id: 'post_id'}, inplace=True)

        return df, top_id


class FriendsDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='friends'):
        super(FriendsDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.friends_df, self.friends_top_id = self.get_friends_df()
        except Exception as e:
            print(e, "fail to parse friends in friends")
            self.get_debug_structure(json_content, table_prefix)
            self.friends_df = None

    def get_friends_df(self):

        wanted_columns = ['name', 'timestamp']

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns)

        df.rename(columns={"name": "friend_name"}, inplace=True)

        # self.append_date_weekday_column(friends_df,
        #                           timestamp_column_name='timestamp',
        #                           unit='s')

        return df, top_id


class MessagesDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='messages'):
        super(MessagesDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.thread_df, self.thread_top_id = self.get_thread_df()

        except Exception as e:
            print(e, "fail to parse thread in messages")
            self.get_debug_structure(json_content, table_prefix)
            self.thread_df = None

        try:
            self.participants_df, self.participants_top_id = \
                self.get_participants_df()

        except Exception as e:
            print(e, "fail to parse participants in messages")
            self.get_debug_structure(json_content, table_prefix)
            self.participants_df = None

        try:
            self.messages_df, self.messages_top_id = self.get_messages_df()

        except Exception as e:
            print(e, "fail to parse messages in messages")
            self.get_debug_structure(json_content, table_prefix)
            self.messages_df = None

    def get_thread_df(self):

        wanted_columns = ['is_still_participant',
                          'thread_path',
                          'thread_type',
                          'title']

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            start_by='thread_path')

        df.rename(columns={top_id: 'thread_id'}, inplace=True)

        return df, top_id

    def get_participants_df(self):

        wanted_columns = ['name',
                          self.thread_top_id]

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            regex=True,
            start_by='id_participants')

        df.rename(columns={self.thread_top_id: 'thread_id',
                           top_id: 'participants_id'}, inplace=True)

        return df, top_id

    def get_messages_df(self):

        wanted_columns = ['content',
                          'sender_name',
                          'timestamp_ms',
                          self.thread_top_id]

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            start_by='timestamp_ms')

        df.rename(columns={self.thread_top_id: 'thread_id',
                           top_id: 'message_id'}, inplace=True)

        self.append_date_weekday_column(df,
                                        timestamp_column_name='timestamp_ms',
                                        unit='ms')

        return df, top_id


class ReactionsDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='reactions'):
        super(ReactionsDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.reactions_df, self.reactions_top_id = self.get_reactions_df()
        except Exception as e:
            print(e, "fail to parse reactions in reactions")
            self.get_debug_structure(json_content, table_prefix)
            self.reactions_df = None

    def get_reactions_df(self):

        wanted_columns = ['timestamp',
                          'title',
                          'reaction_actor',
                          'reaction_reaction']

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns
        )

        df.rename(columns={top_id: 'reaction_id',
                           'reaction_actor': 'actor',
                           'reaction_reaction': 'reaction'}, inplace=True)

        self.append_date_weekday_column(df)

        return df, top_id


class CommentsDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='comments'):
        super(CommentsDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.comments_df, self.comments_top_id = self.get_comments_df()

        except Exception as e:
            print(e, "fail to parse comments in comments")
            self.get_debug_structure(json_content, table_prefix)
            self.comments_df = None

    def get_comments_df(self):

        wanted_columns = ['timestamp',
                          'comment_author',
                          'comment_comment']

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns
        )

        df.rename(columns={top_id: 'comments_id',
                           'comment_author': 'author',
                           'comment_comment': 'comment'}, inplace=True)

        self.append_date_weekday_column(df)

        return df, top_id


class PhotosDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='photos'):
        super(PhotosDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.photos_df, self.photos_top_id = self.get_photos_df()
        except Exception as e:
            print(e, "fail to parse photos in photos")
            self.get_debug_structure(json_content, table_prefix)
            self.photos_df = None

    def get_photos_df(self):

        wanted_columns = ['creation_timestamp',
                          'uri',
                          'description']

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            regex=True,
            start_by='id_photos*')

        df.rename(columns={top_id: 'photo_id',
                           'uri': 'media_uri'}, inplace=True)

        self.append_date_weekday_column(
            df, timestamp_column_name='creation_timestamp')

        return df, top_id


class VideosDFs(TempDFs):

    def __init__(self,
                 json_content,
                 table_prefix='video'):
        super(VideosDFs, self).__init__(
            json_content,
            table_prefix=table_prefix)

        try:
            self.videos_df, self.videos_top_id = self.get_videos_df()
        except Exception as e:
            print(e, "fail to parse videos in videos")
            self.get_debug_structure(json_content, table_prefix)
            self.videos_df = None

    def get_videos_df(self):

        wanted_columns = ['creation_timestamp',
                          'uri',
                          'description',
                          'thumbnail_uri']

        df, top_id = self.temp_to_wanted_df(
            wanted_columns=wanted_columns,
            regex=True,
            start_by='id_videos*')

        df.rename(columns={top_id: 'video_id',
                           'uri': 'media_uri'}, inplace=True)

        self.append_date_weekday_column(
            df, timestamp_column_name='creation_timestamp')

        return df, top_id
