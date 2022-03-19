#!/usr/bin/env python3
"""
Generate code to create hive databases for each user.
"""

import argparse
import os.path
import sys

SAVED_USERS = 'saved_users.csv'


def clean_user(user_name):
    if not user_name:
        return None
    temp = user_name.lower()
    parts = temp.split(' ')
    if len(parts) > 1:
        return parts[0]+parts[1][0]
    return parts[0]


def build_user_list(student_list):
    result = []
    for student_name in student_list:
        user_name = clean_user(student_name)
        if user_name:
            result.append(user_name)
    return result


def gen_create_hive_tables(user_name):
    return "hive -e \"CREATE DATABASE {0} LOCATION 's3a://hive-tbs-2020-01/{0}';\"".format(user_name)


def gen_user_provision_cmds(user_list):
    for user_name in user_list:
        print(' ')
        print('#  ----- User: {0} ----- '.format(user_name))
        user_dir = '/srv/hadoop/users/{0}'.format(user_name)
        print('mkdir {0}'.format(user_dir))
        print('cp -r /srv/hadoop/labs/* {0}/'.format(user_dir))
        print(gen_create_hive_tables(user_name))


def gen_onboarding_users(student_list):
    user_list = build_user_list(student_list)
    gen_user_provision_cmds(user_list)


def gen_offboarding_users(user_list):
    tables = ['links', 'movies', 'nasa_raw', 'ratings', 'tags', 'bike_rides']
    for user_name in user_list:
        if not user_name or user_name in ('default', 'marilson'):
            continue
        print('use {0};'.format(user_name))
        for user_tb in tables:
            print('drop table if exists {0};'.format(user_tb))
        print('drop database {0};'.format(user_name))
        print('show databases;')
        print(' ')


def parse_params():
    parser = argparse.ArgumentParser(prog='create_users')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--create', action='store_true')
    group.add_argument('--remove', action='store_true')

    parser.add_argument(
        '--input_file',
        type=str, help='file containing the list of students.')
    parser.add_argument('--dry_run', action='store_const', const=True, default=False)
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()
    return args


def load_users(input_file):
    print(input_file)
    if not input_file[0] in ['.', '/']:
        input_file = os.path.abspath(os.path.join('.', input_file))
    if not os.path.exists(input_file):
        print('File not found: {0}'.format(input_file))
        sys.exit(-1)
    result = []
    with open(input_file, 'r') as in_file:
        for line in in_file:
            result.append(line.strip())
    with open(SAVED_USERS, 'a') as saved_users:
        saved_users.write('-- --\n')
        saved_users.write('\n'.join(result))
        saved_users.flush()
    return result


def load_last_users():
    result = []
    with open(SAVED_USERS, 'r') as saved_users:
        for line in saved_users:
            if line.startswith('--'):
                result = []
            else:
                result.append(line)
    return result


def main():
    args = parse_params()
    if args.create:
        user_list = load_users(args.input_file)
        gen_onboarding_users(user_list)
    elif args.remove:
        user_list = load_last_users()
        gen_offboarding_users(user_list)




main()
