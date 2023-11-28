from github import Github
from deepdiff import DeepDiff
import logging
import os
import requests
import json

# Constant variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}
REST_URL = os.getenv('REST_URL')
CLUSTER_ID = os.getenv('KAFKA_CLUSTER_ID')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_topics_from_branches():
    """
    Retrieve topic configurations from 'main' and 'test' branches of the Kafka Manager repository.

    This function connects to the Kafka Manager repository, retrieves the latest commit,
    and extracts the topic configurations from the 'main' and 'test' branches.

    Returns:
    tuple: A tuple containing two dictionaries representing topic configurations.
        - The first element: Dictionary representing topic configurations from the 'main' branch.
        - The second element: Dictionary representing topic configurations from the 'test' branch.
    Raises:
    Exception: If there is an error in connecting to the repository or retrieving the topic configurations.
    """
    try:
        g = Github(GITHUB_TOKEN)
        repo = g.get_repo("NiyiOdumosu/kafkamanager")
        feature_topic_content = repo.get_contents("application1/topics/topics.json", ref="test")
        main_topic_content = repo.get_contents("application1/topics/topics.json", ref="main")
        source_topics = json.loads(main_topic_content.decoded_content)
        feature_topics = json.loads(feature_topic_content.decoded_content)

        return source_topics, feature_topics
    except Exception as e:
        logger.error(f"Error getting latest commit diff: {e}")
        raise


def find_changed_topics(source_topics, feature_topics):
    """
    Compare source topics with feature topics and identify changes, deletions, and new additions.

    Parameters:
    - source_topics (list of dicts): List of dictionaries representing source topics.
    - feature_topics (list of dicts): List of dictionaries representing feature topics.

    Returns:
    list: A list of dictionaries, each containing information about changed topics. Each dictionary has the following format:
        {'topic_name': str, 'type': str, 'changes': dict}
        - 'topic_name': The name of the topic.
        - 'type': Type of change ('update', 'removed', 'new').
        - 'changes': Dictionary representing the changes (present if 'type' is 'update').
    """
    source_topics_dict = {}
    feature_topics_dict = {}

    for value in source_topics:
        source_topics_dict.update(value)

    for value in feature_topics:
        feature_topics_dict.update(value)

    changed_topic_names = []
    # Check for changes and deletions
    for topic_name, old_topic in source_topics_dict.items():
        new_topic = feature_topics_dict.get(topic_name)
        if new_topic:
            diff = DeepDiff(old_topic, new_topic, ignore_order=True)
            if diff:
                changed_topic_names.append({topic_name: diff, "type": "update"})
                logger.info(f"The following topic will be updated : {topic_name}")
        else:
            # Topic was removed
            changed_topic_names.append({topic_name: source_topics_dict.get(topic_name), "type": "removed"})
            logger.info(f"The following topic will be removed : {topic_name}")

    # Check for new additions
    for topic_name in feature_topics_dict:
        if topic_name not in source_topics_dict.keys():
            changed_topic_names.append({topic_name: feature_topics_dict.get(topic_name), "type": "new"})
            logger.info(f"The following topic will be added : {topic_name}")
    return changed_topic_names


def process_changed_topics(changed_topic_names):
    for topic in changed_topic_names:
        topic_name = list(topic.keys())[0]
        topic_configs = list(topic.values())[0]
        if topic['type'] == 'new':
            add_new_topic(topic_configs)
        elif topic['type'] == 'update':
            update_existing_topic(topic_name, topic_configs)
        else:
            delete_topic(topic_configs)


def build_topic_rest_url(base_url, cluster_id):
    """
    Build the REST API URL for Kafka topics based on the provided base URL and cluster ID.

    Parameters:
    - base_url (str): The base URL of the Kafka REST API.
    - cluster_id (str): The ID of the Kafka cluster.

    Returns:
    str: The constructed REST API URL for Kafka topics.
    """
    return f'{base_url}/v3/clusters/{cluster_id}/topics/'


def add_new_topic(topic):
    """
    Add a new Kafka topic using the provided topic configuration.

    Parameters:
    - topic (dict): Dictionary representing the configuration of the new Kafka topic.

    """
    rest_topic_url = build_topic_rest_url(REST_URL, CLUSTER_ID)
    topic_json = json.dumps(topic)

    response = requests.post(rest_topic_url, data=topic_json, headers=HEADERS)
    if response.status_code == 201:
        logger.info(f"The topic {topic['topic_name']} has been successfully created")
    else:
        logger.error(f"The topic {topic['topic_name']} returned {str(response.status_code)} due to the follwing reason: {response.reason}" )


def update_existing_topic(topic_name, topic_config):
    """
    Update an existing Kafka topic based on the provided topic configuration.

    Parameters:
    - topic_name (str): The name of the Kafka topic to be updated.
    - topic_config (dict): Dictionary containing the configuration changes for the Kafka topic.

    Raises:
    SystemExit: If any of the update steps fail, the program exits with status code 1.

    Notes:
    This function first retrieves the current definition of the topic by making a GET request to the Kafka REST API.
    It then updates the replication factor and partition count using helper functions.
    Finally, it alters the topic configurations using a POST request to the Kafka REST API.
    """
    rest_topic_url = build_topic_rest_url(REST_URL, CLUSTER_ID)
    try:
        response = requests.get(rest_topic_url + topic_name)
    except Exception as e:
        logger.error(e)

    current_topic_definition = response.json()
    print(f"existing topic definition :{current_topic_definition}")
    # Check if the requested update is a config change
    if not topic_config["values_changed"]["root[\'replication_factor\']"] and topic_config["values_changed"]["root[\'partitions_count\']"]:
        updateConfigs = "{\"data\":" + json.dumps(topic_config['configs']) + "}"
        logger.info("altering configs to " + updateConfigs)
        response = requests.post(f"{rest_topic_url}{topic_name}" + "/configs:alter", data=updateConfigs, headers=HEADERS)
        logger.info("this is the code " + str(response.status_code) + " this is the reason: " + response.reason)
    update_replication_factor(current_topic_definition, rest_topic_url, topic_config, topic_name)
    update_partition_count(current_topic_definition, rest_topic_url, topic_config, topic_name)


def update_replication_factor(current_topic_definition, rest_topic_url, topic_config, topic_name):
    """
    Update the replication factor for a Kafka topic based on the provided configuration.

    Parameters:
    - current_topic_definition (dict): Dictionary representing the current configuration of the Kafka topic.
    - rest_topic_url (str): The REST API URL for the Kafka topic.
    - topic_config (dict): Dictionary containing the configuration changes for the Kafka topic.
    - topic_name (str): The name of the Kafka topic.

    Returns:
    requests.Response: The response from the REST API call to update the replication factor.

    Raises:
    SystemExit: If the replication factor update fails, the program exits with status code 1.
    """
    current_replication_factor = current_topic_definition['replication_factor']
    # Check if the requested update is the replication factor
    try:
        if topic_config["values_changed"]["root[\'replication_factor\']"]:
            new_replication_factor = int(topic_config["values_changed"]["root[\'replication_factor\']"]["new_value"])
            if new_replication_factor != current_replication_factor:
                logger.info(f"A requested change for replication factor for topic  {topic_name} is from "
                            f"{str(current_replication_factor)} to {str(new_replication_factor)}")
                partition_response = requests.patch(f"{rest_topic_url}{topic_name}",
                                                    data="{\"replication_factor\":" + new_replication_factor + "}")
                if partition_response.status_code != 200:
                    logger.info(
                        f"The replication factor change failed for topic {topic_name} due to {str(partition_response.status_code)} -  {partition_response.reason}")
                    exit(1)
                logger.info(f"The replication factor change for topic {topic_name} was successful")
    except KeyError:
        logger.info("Replication Factor was not requested to be updated ")


def update_partition_count(current_topic_definition, rest_topic_url, topic_config, topic_name):
    """
    Update the partition count for a Kafka topic based on the provided configuration.

    Parameters:
    - current_topic_definition (dict): Dictionary representing the current configuration of the Kafka topic.
    - rest_topic_url (str): The REST API URL for the Kafka topic.
    - topic_config (dict): Dictionary containing the configuration changes for the Kafka topic.
    - topic_name (str): The name of the Kafka topic.

    Raises:
    SystemExit: If the partition count update fails, the program exits with status code 1.
    """
    current_partitions_count = current_topic_definition['partitions_count']

    # Check if the requested update is the partition count
    try:
        new_partition_count = int(topic_config["values_changed"]["root[\'partitions_count\']"]["new_value"])
        if new_partition_count > current_partitions_count:
            logger.info(f"A requested increase of partitions for topic  {topic_name} is from "
                        f"{str(current_partitions_count)} to {str(new_partition_count)}")
            partition_response = requests.patch(f"{rest_topic_url}{topic_name}",
                                                data="{\"partitions_count\":" + str(new_partition_count) + "}")
            if partition_response.status_code != 200:
                logger.info(
                    f"The partition increase failed for topic {topic_name} due to {str(partition_response.status_code)} -  {partition_response.reason}")
                exit(1)
            logger.info(f"The partition increase for topic {topic_name} was successful")
        elif new_partition_count < current_partitions_count:
            logger.error("Cannot reduce partition count for a given topic")
            exit(1)
    except Exception as e:
        logger.error("Failed due to " + e)


def delete_topic(topic):
    """
    Delete a Kafka topic based on the provided topic configuration.

    Parameters:
    - topic (dict): Dictionary representing the configuration of the Kafka topic, including the topic name.

    Raises:
    SystemExit: If the deletion fails, the program exits with status code 1.

    Notes:
    This method first checks if the topic exists by making a GET request to the Kafka REST API.
    If the topic exists, it proceeds to delete the topic using a DELETE request.
    """
    rest_topic_url = build_topic_rest_url(REST_URL, CLUSTER_ID)

    get_response = requests.get(rest_topic_url + topic['topic_name'])
    if get_response.status_code == 200:
        logger.info(f"Response code is {str(get_response.status_code)}")
    else:
        logger.error(f"Failed due to the following status code {str(get_response.status_code)} and reason {str(get_response.reason)}" )

    response = requests.delete(rest_topic_url + topic['topic_name'])
    if response.status_code == 204:
        logger.info(f"The topic {topic['topic_name']} has been successfully deleted")
    else:
        logger.error(f"The topic {topic['topic_name']} returned {str(response.status_code)} due to the following reason: {response.reason}" )


if __name__ == "__main__":
    old_topics, new_topics = get_topics_from_branches()
    changed_topics = find_changed_topics(old_topics, new_topics)
    print(changed_topics)
    process_changed_topics(changed_topics)