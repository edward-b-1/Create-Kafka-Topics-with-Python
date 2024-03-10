

from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import ConfigResource
from confluent_kafka.admin import ConfigEntry
from confluent_kafka.admin import ConfigSource
from confluent_kafka.admin import AlterConfigOpType
from confluent_kafka.admin import ResourceType

from lib_topic_config.topic_config import TopicConfig


def get_admin_client() -> AdminClient:

    config = {
        # TODO: edit this
        'bootstrap.servers': '192.168.0.239:9092'
    }

    admin_client = AdminClient(config)

    return admin_client


def _delete_topic(
    admin_client: AdminClient,
    config: dict,
) -> None:

    topic_name = config['topic_name']

    futures = admin_client.delete_topics(topics=[topic_name])

    for topic, future in futures.items():
        try:
            future.result()
            print(f'Deleted topic {topic}')
        except Exception as exception:
            print(f'Failed to delete topic {topic}, {exception}')


def _create_topic(
    admin_client: AdminClient,
    # topic_name:str,
    # num_partitions:int,
    # replication_factor:int,
    config:dict,
) -> None:

    topic_name = config['topic_name']
    num_partitions = config['num_partitions']
    replication_factor = config['replication_factor']

    # remaining_config = {
    #     **config
    # }

    # remaining_config.pop('topic_name')
    # remaining_config.pop('num_partitions')
    # remaining_config.pop('replication_factor')

    remaining_config = {
        #'max.message.bytes': config['max_message_bytes']
        **config,
    }

    remaining_config.pop('topic_name')
    remaining_config.pop('num_partitions')
    remaining_config.pop('replication_factor')

    # Create Topic
    new_topic = NewTopic(
        topic_name,
        num_partitions,
        replication_factor=replication_factor,
        config=remaining_config)

    futures = admin_client.create_topics(new_topics=[new_topic])

    for config_resource, future in futures.items():
        try:
            future.result()

            max_message_bytes = None
            if 'max_message_bytes' in config:
                max_message_bytes = config['max_message_bytes']

            print(f'Created topic {config_resource}, '
                  f'num partitions = {num_partitions}, '
                  f'replication_factor = {replication_factor} '
                  f'max_message_bytes = {max_message_bytes}')

        except Exception as exception:
            print(f'Failed to create topic {config_resource}, {exception}')

    # Use this to print some debug info
    # config_resource = ConfigResource(ResourceType.TOPIC, name=topic_name)
    # futures = admin_client.describe_configs([config_resource])

    # for config_resource, future in futures.items():
    #     print(f'config_resource={config_resource}')
    #     try:
    #         dictionary = future.result()
    #         print(dictionary)
    #     except Exception as exception:
    #         print(f'Failed to create topic {config_resource}, {exception}')

    # Update Topic Config
    # config_entry_min_insync_replicas = \
    #     ConfigEntry(
    #         name='min.insync.replicas',
    #         value='3',
    #         source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
    #         incremental_operation=AlterConfigOpType.SET)

    # resource = \
    #     ConfigResource(
    #         ResourceType.TOPIC,
    #         name=topic_name,
    #         set_config=None,
    #         described_configs=None,
    #         incremental_configs=[
    #             config_entry_min_insync_replicas
    #         ]
    #     )

    # futures = admin_client.incremental_alter_configs([resource])

    # for config_resource, future in futures.items():
    #     try:
    #         future.result()
    #         print(f'Updated topic config {config_resource}')
    #     except Exception as exception:
    #         print(f'Failed to update topic config for topic {config_resource}, {exception}')


def create_topics(
    admin_client: AdminClient,
    recreate: bool,
    #topic_name: str,
    topic_config: TopicConfig,
) -> None:

    config = topic_config.to_dict()

    if recreate:
        _delete_topic(
            admin_client=admin_client,
            config=config,
        )

    _create_topic(
        admin_client,
        #topic_name,
        #num_partitions=num_paritions_default,
        #replication_factor=replication_factor_default,
        config=config,
    )

