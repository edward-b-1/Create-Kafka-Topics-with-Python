#!/usr/bin/env python3

from my_admin_client_lib import get_admin_client
from my_admin_client_lib import create_topics
from my_admin_client_lib import get_fully_qualified_topic_name_uat_json

import argparse


def parse_args_get_topic_name() -> str:

    parser = argparse.ArgumentParser(
        prog='Create Topics',
        description='Helper program to create Kafka topics')

    parser.add_argument('--topic', action='store', metavar='topic_name', dest='topic_name', required=True)

    args = parser.parse_args()

    topic_name = args.topic_name

    return topic_name


def main():

    topic_name = parse_args_get_topic_name()

    fully_qualified_topic_name_data = \
        get_fully_qualified_topic_name_uat_json(topic_name)

    admin_client = get_admin_client()
    create_topics(admin_client, topic_name=fully_qualified_topic_name_data)


if __name__ == '__main__':
    main()
