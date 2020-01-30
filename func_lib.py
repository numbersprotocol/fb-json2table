import json
import argparse
import os
import pandas as pd
import re


def get_args(argv=None):
    """ Prepare auguments for running the script """

    parser = argparse.ArgumentParser(
    )

    parser.add_argument(
        '-i',
        '--input',
        type=str,
        default='',
        help='input file path')

    parser.add_argument(
        '-o',
        '--output',
        type=str,
        default='',
        help='Output db path.')
    return parser.parse_args(argv)


def parse_fb_json(path):
    with open(path , 'r') as f:

        jf = json.load(f)
    try:
        jf = json.dumps(jf,ensure_ascii=False).encode("latin1").decode("utf8")
        jf = json.loads(jf)
    except:
        pass
    
    return jf


def save_to_folder(df, folder_path):

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    if df is not None:
        for i in df.index.tolist():
            df.loc[[i], :].to_json(os.path.join(folder_path, 
                                   str(i) + '.json'), 
                                   'records')
