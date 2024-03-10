#!/usr/bin/env python3

from my_admin_client_lib import get_admin_client
from my_admin_client_lib import create_topics
from my_admin_client_lib import TopicConfig

import argparse


def parse_args() -> str:

    parser = argparse.ArgumentParser(
        prog='Create Topics',
        description='Helper program to create Kafka topics')

    parser.add_argument('--topic', action='store', metavar='topic_name', dest='topic_name', required=True)
    parser.add_argument('--recreate', action=argparse.BooleanOptionalAction, metavar='recreate', dest='recreate', required=False)
    parser.add_argument('--mb', action='store', metavar='megabytes', dest='megabytes', required=False)

    args = parser.parse_args()

    return args

def parse_args_get_topic_name(args) -> str:
    topic_name = args.topic_name
    return topic_name

def parse_args_get_megabytes(args) -> str|None:
    megabytes = args.megabytes
    return megabytes

def parse_args_get_recreate(args) -> bool:
    recreate = args.recreate
    if recreate:
        return True
    else:
        return False


def main():

    args = parse_args()
    topic_name = parse_args_get_topic_name(args)
    megabytes_str = parse_args_get_megabytes(args)
    recreate = parse_args_get_recreate(args)


    topic_config = \
        TopicConfig.default(topic_name=topic_name) \
            .with_replication_factor(replication_factor=1)

    print(f'{megabytes_str}')
    if megabytes_str is not None:
        megabytes = int(megabytes_str)

        mb = 1024 * 1024
        max_message_bytes = megabytes * mb

        topic_config = \
            topic_config \
                .with_max_message_bytes(max_message_bytes=max_message_bytes)

    admin_client = get_admin_client()

    create_topics(
        admin_client=admin_client,
        recreate=recreate,
        topic_config=topic_config,
    )


if __name__ == '__main__':
    main()
