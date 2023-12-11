
2023-12-07 16:23:53.695176 - {"resource_type": "TOPIC", "resource_name": "topic_a", "pattern_type": "LITERAL", "principal": "User:clientrestproxy", "host": "*", "operation": "DESCRIBE", "permission": "ALLOW"} has been successfully created
2023-12-07 16:23:53.712728 - {"resource_type": "TOPIC", "resource_name": "topic_a", "pattern_type": "LITERAL", "principal": "User:clientrestproxy", "host": "*", "operation": "CREATE", "permission": "ALLOW"} has been successfully created
2023-12-07 16:29:26.674025 - {'resource_type': 'TOPIC', 'resource_name': 'topic_b', 'pattern_type': 'LITERAL', 'principal': 'User:clientrestproxy', 'host': '*', 'operation': 'WRITE', 'permission': 'ALLOW'} has been successfully deleted
2023-12-07 16:29:26.689664 - {'resource_type': 'TOPIC', 'resource_name': 'topic_c', 'pattern_type': 'LITERAL', 'principal': 'User:clientrestproxy', 'host': '*', 'operation': 'WRITE', 'permission': 'ALLOW'} has been successfully deleted
Topic configs failed to be applied to the topic due to 400 this is the reason: {"error_code":400,"message":"Cannot deserialize value of type `AlterEntry>` from Object value (token `JsonToken.START_OBJECT`)"}
2023-12-11 15:49:22.587192 - The configs {"data":[{"name": "cleanup.policy", "value": "compact"}, {"name": "compression.type", "value": "zstd"}]} was successfully applied to topic_f

