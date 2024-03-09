
class TopicConfig():

    def __init__(self, topic_name:str) -> None:
        self.topic_name = topic_name
        self.number_of_partitions = None
        self.replication_factor = None
        self.max_message_bytes = None

    @classmethod
    def default(cls, topic_name:str):

        num_paritions_default = 14
        replication_factor_default = 3

        topic_config = \
            TopicConfig(topic_name=topic_name) \
                .with_num_partitions(number_of_partitions=num_paritions_default) \
                .with_replication_factor(replication_factor=replication_factor_default)

        return topic_config


    def clone(self):
        topic_config = TopicConfig(topic_name=self.topic_name)
        topic_config.number_of_partitions = self.number_of_partitions
        topic_config.replication_factor = self.replication_factor
        topic_config.max_message_bytes = self.max_message_bytes
        return topic_config

    def with_num_partitions(self, number_of_partitions:int):
        topic_config = self.clone()
        topic_config.number_of_partitions = number_of_partitions
        return topic_config

    def with_replication_factor(self, replication_factor:int):
        topic_config = self.clone()
        topic_config.replication_factor = replication_factor
        return topic_config

    def with_max_message_bytes(self, max_message_bytes:int):
        topic_config = self.clone()
        topic_config.max_message_bytes = max_message_bytes
        return topic_config

    def to_dict(self) -> dict:
        return {
            'topic_name': self.topic_name,
            'num_partitions': self.number_of_partitions,
            'replication_factor': self.replication_factor,
            'max_message_bytes': self.max_message_bytes,
        }