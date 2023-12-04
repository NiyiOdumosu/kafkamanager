import pandas as pd
import json

df = pd.read_csv('application1/topics/topic_configs.csv')

topics_list = []

# Add document link to the future topic configs

for index, row in df.iterrows():
    topic_name = row['topic name']

    # Set defaults for topic configs
    cleanup_policy = 'delete' if str(row['cleanup.policy']) == "nan" else str(row['cleanup.policy'])
    partitions_count = '4' if str(row['partition count']) == "nan" else str(int(row['partition count']))
    replication_factor = '3' if str(row['replication factor']) == "nan" else str(int(row['replication factor']))
    compression_type = 'producer' if str(row['compression.type']) == "nan" else str(row['compression.type'])
    retention_ms = 86400000 if str(row['retention.ms']) == "nan" else int(row['retention.ms'])
    max_message_bytes = 1048588 if str(row['max.message.bytes']) == "nan" else int(row['max.message.bytes'])


    topic_dict = {
        f"{topic_name}" : {
        "topic_name": row['topic name'],
        "partitions_count": partitions_count,
        "replication_factor": replication_factor,
        "configs": [
            {
                "name": "cleanup.policy",
                "value": cleanup_policy
            },
            {
                "name": "compression.type",
                "value": compression_type
            },
            {
                "name": "retention.ms",
                "value": retention_ms
            },
            {
                "name": "max.message.bytes",
                "value": max_message_bytes
            }
        ]
      }
    }
    topics_list.append(topic_dict)

json_output = json.dumps(topics_list, indent=4)

print(json_output)

with open('application1/topics/topics.json', 'w') as json_file:
    json_file.write(json_output)
