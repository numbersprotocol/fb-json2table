import json
import argparse
import os
import pandas as pd
import re
from fbjson2table.func_lib import get_args, parse_fb_json, \
    save_to_folder
from fbjson2table.table_class import TempDFs, PostsDFs, \
    FriendsDFs, MessagesDFs, ReactionsDFs, \
    CommentsDFs, PhotosDFs, VideosDFs


def get_app_dfs(inpath, debug_path):

    posts_json_list = []
    friends_json_list = []
    friends_filename_list = []
    messages_json_list = []
    reactions_json_list = []
    comments_json_list = []
    photos_json_list = []
    videos_json_list = []

    inpath_structure = []

    for root, dirs, files in os.walk(inpath, topdown=True):

        for f in files:
            filepath = os.path.join(root, f)
            inpath_structure.append(filepath)

            if os.path.splitext(f)[1] == '.json':
                if ("posts" in root) & bool(re.match('your_posts*', f)):
                    temp_dict = {
                        "filename": os.path.split(root)[-1] + '__' + f,
                        "filecontent": parse_fb_json(filepath)}
                    posts_json_list.append(temp_dict)

                if "friends" in root:
                    friends_json_list.append(parse_fb_json(filepath))
                    friends_filename_list.append(os.path.splitext(f)[0])

                if 'messages' in root:
                    messages_json_list.append(parse_fb_json(filepath))

                if ("likes_and_reactions" in root) & \
                   bool(re.match('posts_and_comments*', f)):
                    temp_dict = {
                        "filename": os.path.split(root)[-1] + '__' + f,
                        "filecontent": parse_fb_json(filepath)}
                    reactions_json_list.append(temp_dict)

                if ("comments" in root) & bool(re.match('comments*', f)):
                    temp_dict = {
                        "filename": os.path.split(root)[-1] + '__' + f,
                        "filecontent": parse_fb_json(filepath)}
                    comments_json_list.append(temp_dict)

                if 'album' in root:
                    photos_json_list.append(parse_fb_json(filepath))

                if ("photos_and_videos" in root) & \
                        bool(re.match('your_videos*', f)):
                    temp_dict = {
                        "filename": os.path.split(root)[-1] + '__' + f,
                        "filecontent": parse_fb_json(filepath)}
                    videos_json_list.append(temp_dict)

    posts_dfs = PostsDFs(posts_json_list)

    friends_df_list = []
    friends_dfs_list = []
    for json_content, filename in \
            zip(friends_json_list, friends_filename_list):

        temp_friends_dfs = FriendsDFs(json_content,
                                      table_prefix=filename)
        friends_dfs_list.append(temp_friends_dfs)
        temp_friends_df = temp_friends_dfs.friends_df.drop(columns=[
            temp_friends_dfs.friends_top_id])
        temp_friends_df.loc[:, 'friend_type'] = filename
        friends_df_list.append(temp_friends_df)

    try:
        friends_df = pd.concat(friends_df_list)
        friends_df = friends_df.reset_index(drop=True)
        friends_df.loc[:, 'friend_id'] = friends_df.index
    except Exception as e:
        print(e, 'no friends')
        friends_df = None

    messages_dfs = MessagesDFs(messages_json_list)
    reactions_dfs = ReactionsDFs(reactions_json_list)
    comments_dfs = CommentsDFs(comments_json_list)
    photos_dfs = PhotosDFs(photos_json_list)
    if photos_dfs.photos_df is not None:
        photos_df = photos_dfs.photos_df.drop(columns=['photo_id'],
                                              errors='ignore')
    else:
        photos_df = pd.DataFrame()
    videos_dfs = VideosDFs(videos_json_list)
    if videos_dfs.videos_df is not None:
        videos_df = videos_dfs.videos_df.drop(columns=['video_id'],
                                              errors='ignore')
    else:
        videos_df = pd.DataFrame()
    media_df = pd.concat([videos_df, photos_df])
    media_df.loc[:, 'media_id'] = media_df.reset_index().index
    dfs_list = [posts_dfs, messages_dfs, reactions_dfs, comments_dfs,
                photos_dfs, videos_dfs] + friends_dfs_list

    for i in dfs_list:
        if i.debug_df_list is not None:
            if not os.path.exists(debug_path):
                os.makedirs(debug_path)
            for df, table_name in \
                    zip(i.debug_df_list, i.debug_table_name_list):
                df.to_csv(os.path.join(debug_path, table_name + '.csv'))

            with open(os.path.join(debug_path, 'inpath_structure.txt'),
                      'w') as f:
                for path in inpath_structure:
                    f.write("%s\n" % path)

    return posts_dfs.posts_df, posts_dfs.place_df, \
        posts_dfs.media_df, posts_dfs.tags_df, \
        friends_df, \
        messages_dfs.thread_df, messages_dfs.participants_df, \
        messages_dfs.messages_df, \
        reactions_dfs.reactions_df, \
        comments_dfs.comments_df, \
        media_df, \
        inpath_structure


def tagged_link_to_friends(app_tagged_df, app_friend_df):
    for row in app_tagged_df.iterrows():
        matched_id = app_friend_df[
            (app_friend_df['friend_type'] == 'friends') &
            (app_friend_df['friend_name'] == row[1]['tags'])
        ]['friend_id'].tolist()
        if matched_id != []:
            app_tagged_df.loc[row[0], 'friend_id'] = matched_id[0]


def create_outputs(inpath, outpath):

    app_your_posts_df, df_app_place, posts_media_df, app_tagged_df, \
        app_friends_df, \
        app_thread_df, app_participants_df, app_messages_df, \
        app_reactions_df, \
        app_comments_df, \
        app_media_df, \
        inpath_structure = get_app_dfs(inpath, outpath)

    try:
        posts_media_df = posts_media_df[['post_id',
                                         'media_uri',
                                         'filename_extension',
                                         'pm_id']]
    except Exception as e:
        print(e, 'posts_media_df is None')
        pass

    try:
        tagged_link_to_friends(app_tagged_df, app_friends_df)

    except Exception as e:
        print(e, 'fail to link tags to friends')
        pass

    posts_folder = os.path.join(outpath, 'posts')
    save_to_folder(app_your_posts_df, posts_folder)

    place_folder = os.path.join(outpath, 'place')
    save_to_folder(df_app_place, place_folder)

    posts_media_folder = os.path.join(outpath, 'posts_media')
    save_to_folder(posts_media_df, posts_media_folder)

    friend_folder = os.path.join(outpath, 'friend')
    save_to_folder(app_friends_df, friend_folder)

    tags_folder = os.path.join(outpath, 'tags')
    save_to_folder(app_tagged_df, tags_folder)

    thread_folder = os.path.join(outpath, 'thread')
    participants_folder = os.path.join(outpath, 'participants')
    messages_folder = os.path.join(outpath, 'messages')

    save_to_folder(app_thread_df, thread_folder)
    save_to_folder(app_participants_df, participants_folder)
    save_to_folder(app_messages_df, messages_folder)

    reactions_folder = os.path.join(outpath, 'reactions')
    save_to_folder(app_reactions_df, reactions_folder)

    comments_folder = os.path.join(outpath, 'comments')
    save_to_folder(app_comments_df, comments_folder)

    media_folder = os.path.join(outpath, 'media')
    save_to_folder(app_media_df, media_folder)


if __name__ == '__main__':

    args = get_args()
    inpath = os.path.abspath(args.input)
    outpath = os.path.abspath(args.output)

    create_outputs(inpath, outpath)
