

from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import ConfigResource
from confluent_kafka.admin import ConfigEntry
from confluent_kafka.admin import ConfigSource
from confluent_kafka.admin import AlterConfigOpType
from confluent_kafka.admin import ResourceType


def get_fully_qualified_topic_name_uat_json(
    topic_name: str) -> str:

    uat_or_prod = f'uat'
    data_encoding = f'json'

    # Define whatever schema name for topics you want here
    return f'{uat_or_prod}.{topic_name}.{data_encoding}'


def get_admin_client() -> AdminClient:

    config = {
        # TODO: edit this
        'bootstrap.servers': 'localhost:29092'
    }

    admin_client = AdminClient(config)

    return admin_client


def _create_data_topic(admin_client: AdminClient, topic_name:str, num_partitions:int, replication_factor:int) -> None:

    # Create Topic
    new_topic = NewTopic(
        topic_name,
        num_partitions,
        replication_factor=replication_factor)

    futures = admin_client.create_topics(new_topics=[new_topic])

    for config_resource, future in futures.items():
        try:
            future.result()
            print(f'Created topic {config_resource}, num partitions = {num_partitions}, replication_factor = {replication_factor}')
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
    config_entry_min_insync_replicas = \
        ConfigEntry(
            name='min.insync.replicas',
            value='3',
            source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
            incremental_operation=AlterConfigOpType.SET)

    resource = \
        ConfigResource(
            ResourceType.TOPIC,
            name=topic_name,
            set_config=None,
            described_configs=None,
            incremental_configs=[
                config_entry_min_insync_replicas
            ]
        )

    futures = admin_client.incremental_alter_configs([resource])

    for config_resource, future in futures.items():
        try:
            future.result()
            print(f'Updated topic config {config_resource}')
        except Exception as exception:
            print(f'Failed to update topic config for topic {config_resource}, {exception}')


def create_topics(admin_client: AdminClient, topic_name: str) -> None:

    num_paritions_default = 14
    replication_factor_default = 3

    _create_data_topic(admin_client,
                       topic_name,
                       num_partitions=num_paritions_default,
                       replication_factor=replication_factor_default)
