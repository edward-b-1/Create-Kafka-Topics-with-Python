# Create-Kafka-Topics-with-Python

Create Kafka Topics, and set configuration options with Python

Example code is included to:

- Create Kafka topics
- Modify Kafka topic configuration options
- Display Kafka topic configuration options (currently disabled by commenting out - it is in the library file if you want to use it)

This client uses the **Official** Confluent Kafka Client (Python). More information can be found in the documentation:

- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

# To modify more than one configuration option at a time

```
# Create multiple `ConfigEntry` objects

config_entry_min_insync_replicas = \
    ConfigEntry(
        name='min.insync.replicas',
        value='3',
        source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
        incremental_operation=AlterConfigOpType.SET)

config_entry_something_else = \
    ConfigEntry(
        name='something.else',
        value='some_value',
        source=ConfigSource.DYNAMIC_TOPIC_CONFIG,
        incremental_operation=AlterConfigOpType.SET)

# Add all `ConfigEntry` objects to a `ConfigResource` object
resource = \
    ConfigResource(
        ResourceType.TOPIC,
        name=topic_name,
        set_config=None,
        described_configs=None,
        incremental_configs=[
            config_entry_min_insync_replicas,
            config_entry_something_else,
                # add more here
        ]
    )
```

Further information can be found in the docs.

# Usage

```
./create_topics.py --topic topic_name
```

# Defaults

- Bootstrap Servers: `localhost:29092`
- Num Partitions: 14
- Replication Factor: 3
- Min Insync Replicas: 3


# Suggestions

First, you probably want to modify the library, unless you simply want to create single topics with the same defaults.

You might want to add library code for creating different types of topic, or creating topics in groups.

Second, you will want to change the bootstrap server, and possibly some of the default configuration options.

