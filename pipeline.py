from github import Github
from deepdiff import DeepDiff
import logging
import os
import json
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
        latest_commit = repo.get_commits()[0]
        feature_topic_content = repo.get_contents("application1/topics/topics.json", ref="test")
        main_topic_content = repo.get_contents("application1/topics/topics.json", ref="main")
        source_topics = json.loads(main_topic_content.decoded_content)
        feature_topics = json.loads(feature_topic_content.decoded_content)

        return source_topics, feature_topics
    except Exception as e:
        logger.error(f"Error getting latest commit diff: {e}")
        raise


# def load_json_file(file_path):
#     with open(file_path, 'r') as file:
#         return json.load(file)


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
        topic_configs = list(topic.values())[0]
        if topic['type'] == 'new':
            add_new_topic(topic_configs)
        elif topic['type'] == 'update':
            update_existing_topic(topic_configs)
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

    Raises:
    Exception: If there is an error in creating the new topic, an exception is raised, and details are logged.
    """
    rest_topic_url = build_topic_rest_url(REST_URL, CLUSTER_ID)
    topic_json = json.dumps(topic)

    response = requests.post(rest_topic_url, data=topic_json, headers=HEADERS)
    if response.status_code == 201:
        logger.info(f"The topic {topic['topic_name']} has been successfully created")
    else:
        logger.error(f"The topic {topic['topic_name']} returned {str(response.status_code)} due to the follwing reason: {response.reason}" )


def update_existing_topic(topic):
    rest_topic_url = build_topic_rest_url(REST_URL, CLUSTER_ID)
    topic_json = json.dumps(topic)
    try:
        response = requests.get(rest_topic_url + topic['topic_name'])
    except Exception as e:
        logger.error(e)

    currentTopicDefinition = response.json()
    print("existing topic definition ")
    print(currentTopicDefinition)
    currentPartitionsCount = currentTopicDefinition['partitions_count']
    requestedChanges = topic_json
    print("requested topic definition ")
    print(requestedChanges)
    if (requestedChanges['partitions_count'] > currentPartitionsCount):
        print("requested increasing partitions from " + str(currentPartitionsCount) + " to " + str(
            requestedChanges['partitions_count']))
        r = requests.patch(f"{rest_topic_url}{topic['topic_name']}",
                           data="{\"partitions_count\":" + str(requestedChanges['partitions_count']) + "}")
        response_code = str(r.status_code)
        response_reason = r.reason
        print("this is the code " + response_code + " this is the reason: " + response_reason)
        if (response_code.startswith("2") == False):
            exit(1)
    elif (requestedChanges['partitions_count'] < currentPartitionsCount):
        print("Attempting to reduce partitions which is not allowed")
        exit(1)
    updateConfigs = "{\"data\":" + json.dumps(requestedChanges['configs']) + "}"
    print("altering configs to " + updateConfigs)
    r = requests.post(f"{rest_topic_url}{topic['topic_name']}" + "/configs:alter", data=updateConfigs, headers=HEADERS)
    response_code = str(r.status_code)
    response_reason = r.reason
    print("this is the code " + response_code + " this is the reason: " + response_reason)
    if (response_code.startswith("2") == False):
        exit(1)


def delete_topic(topic):
    rest_topic_url = build_topic_rest_url(REST_URL, CLUSTER_ID)
    topic_json = json.dumps(topic)
    try:
        response = requests.get(rest_topic_url + topic['topic_name'])
    except Exception as e:
        logger.error(e)

    try:
        response = requests.delete(rest_topic_url + {topic['topic_name']})
        if response.status_code == 204:
            logger.info(f"The topic {topic} has been successfully deleted")
    except Exception as e:
        logger.error(f"The topic {topic} returned {str(response.status_code)} due to the follwing reason: {response.reason}" )
        logger.error(e)


if __name__ == "__main__":
    old_topics, new_topics = get_topics_from_branches()
    changed_topics = find_changed_topics(old_topics, new_topics)
    process_changed_topics(changed_topics)