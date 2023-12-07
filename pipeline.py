from github import Github
from deepdiff import DeepDiff

# import click
import json
import logging
import os
import string
import subprocess
import re
import requests

# Constant variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}
REST_PROXY_URL = os.getenv('REST_URL')
CLUSTER_ID = os.getenv('KAFKA_CLUSTER_ID')
CONNECT_REST_URL = os.getenv('CONNECT_REST_URL')
BASIC_AUTH_USER = os.getenv('BASIC_AUTH_USER')
BASIC_AUTH_PASS = os.getenv('BASIC_AUTH_PASS')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_content_from_branches(source_file, source_branch, feature_file, feature_branch):
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
        feature_branch_content = repo.get_contents(feature_file, ref=feature_branch)
        source_branch_content = repo.get_contents(source_file, ref=source_branch)
        source_topics = json.loads(source_branch_content.decoded_content)
        feature_topics = json.loads(feature_branch_content.decoded_content)

        return source_topics, feature_topics
    except Exception as e:
        logger.error(f"Error getting latest commit diff: {e}")
        raise


def find_changed_topics(source_topics, new_topics):
    source_topics_dict = {}
    feature_topics_dict = {}

    for value in source_topics:
        source_topics_dict.update(value)

    for value in new_topics:
        feature_topics_dict.update(value)

    changed_topic_names = []
    # Check for changes and deletions
    for topic_name, source_topic in source_topics_dict.items():
        updated_topic = feature_topics_dict.get(topic_name)

        if updated_topic:
            diff = DeepDiff(source_topic, updated_topic, ignore_order=True)
            if diff:
                print(diff['values_changed'])
                # Check if this is a partition change
                try:
                    if diff['values_changed'] == { "root[\'partitions_count\']": diff['values_changed']["root[\'partitions_count\']"]}:
                        for change_type, details in diff.items():
                            change_dict = {
                                "topic_name": topic_name,
                                "changes": []
                            }
                            for change in details:
                                configs_changes = re.findall(r"\['(.*?)'\]", change)
                                change_dict["changes"].append({
                                    configs_changes[0]: details[change]['new_value']
                                })
                        changed_topic_names.append({"type": "update", "changes": change_dict})
                except KeyError as ke:
                    logger.error(ke)

                for change_type, details in diff.items():
                    change_dict = {
                        "topic_name": topic_name,
                        "changes": []
                    }

                    for change in details:
                        configs_changes = re.findall(r"configs'\]\[(.*)\]\[", change)
                        if configs_changes:
                            for index in configs_changes:
                                # config_index = int(configs_changes[int(index)])
                                prop_name = feature_topics_dict[topic_name]['configs'][int(index)]
                                change_dict["changes"].append({
                                    "name": prop_name["name"],
                                    "value": details[change]['new_value']
                                })

                            continue

                        property_name_list = re.findall(r"\['(.*?)'\]", change)
                        if property_name_list:
                            prop_name = property_name_list[0]
                            change_dict["changes"].append({
                                "name": prop_name["name"],
                                "value": details[change]['new_value']
                            })

                changed_topic_names.append({"type": "update", "changes": change_dict})
        else:
            # Topic was removed
            changed_topic_names.append({topic_name: source_topics_dict.get(topic_name), "type": "removed"})

    # Check for new additions
    for topic_name in feature_topics_dict:
        if topic_name not in source_topics_dict.keys():
            changed_topic_names.append({topic_name: feature_topics_dict.get(topic_name), "type": "new"})

    return changed_topic_names


def extract_data(changed_topics):
    results = []
    for topic in changed_topics:
        if topic["changes"]:
            for change in topic["changes"]["changes"]:
                property_name = change["prop_name"]
                property_value = change["changes"]["new_value"]
                results.append({"data": {
                    "name": property_name,
                    "value": property_value
                }})

    return results


def process_changed_topics(changed_topic_names):
    for i, topic in enumerate(changed_topic_names):
        topic_name = list(topic.keys())[i]
        topic_configs = list(topic.values())[i]
        if topic['type'] == 'new':
            add_new_topic(topic_configs)
        elif topic['type'] == 'update':
            update_existing_topic(topic['changes']['topic_name'], topic['changes']['changes'])
        else:
            delete_topic(topic_name)





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
    rest_topic_url = build_topic_rest_url(REST_PROXY_URL, CLUSTER_ID)
    topic_json = json.dumps(topic)

    response = requests.post(rest_topic_url, auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS), data=topic_json, headers=HEADERS)
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
    rest_topic_url = build_topic_rest_url(REST_PROXY_URL, CLUSTER_ID)
    try:
        response = requests.get(rest_topic_url + topic_name, auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS),)
    except Exception as e:
        logger.error(e)

    current_topic_definition = response.json()
    # Check if the requested update is a config change
    if 'partitions_count' in topic_config[0].keys():
        update_partition_count(current_topic_definition, rest_topic_url, topic_config[0]['partitions_count'], topic_name)
    else:
        updated_Configs = "{\"data\":" + json.dumps(topic_config) + "}"
        logger.info("altering configs to " + updated_Configs)
        response = requests.post(f"{rest_topic_url}{topic_name}" + "/configs:alter", auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS), data=updated_Configs, headers=HEADERS)
        logger.info("this is the code " + str(response.status_code) + " this is the reason: " + response.text)





def update_partition_count(current_topic_definition, rest_topic_url, partition_count, topic_name):
    """
    Update the partition count for a Kafka topic based on the provided configuration.

    Parameters:
    - current_topic_definition (dict): Dictionary representing the current configuration of the Kafka topic.
    - rest_topic_url (str): The REST API URL for the Kafka topic.
    - partition_count (str): Partition count.
    - topic_name (str): The name of the Kafka topic.

    Raises:
    SystemExit: If the partition count update fails, the program exits with status code 1.
    """
    current_partitions_count = current_topic_definition['partitions_count']

    # Check if the requested update is the partition count
    try:
        new_partition_count = int(partition_count)
        if new_partition_count > current_partitions_count:
            logger.info(f"A requested increase of partitions for topic  {topic_name} is from "
                        f"{str(current_partitions_count)} to {str(new_partition_count)}")
            partition_response = requests.patch(f"{rest_topic_url}{topic_name}",
                                                auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS),
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


def delete_topic(topic_name):
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
    rest_topic_url = build_topic_rest_url(REST_PROXY_URL, CLUSTER_ID)

    get_response = requests.get(rest_topic_url + topic_name, auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS))
    if get_response.status_code == 200:
        logger.info(f"Response code is {str(get_response.status_code)}")
    else:
        logger.error(f"Failed due to the following status code {str(get_response.status_code)} and reason {str(get_response.reason)}" )

    response = requests.delete(rest_topic_url + topic_name, auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS))
    if response.status_code == 204:
            logger.info(f"The topic {topic_name} has been successfully deleted")
    else:
        logger.error(f"The topic {topic_name} returned {str(response.status_code)} due to the following reason: {response.reason}" )



def find_changed_acls(source_acls, feature_acls):
    """
    Compare source topics with feature topics and identify changes, deletions, and new additions.

    Parameters:
    - source_topics (list of dicts): List of dictionaries representing source topics.
    - feature_topics (list of dicts): List of dictionaries representing feature topics.

    Returns:
    list: A list of dictionaries, each containing information about changed topics. Each dictionary has the following format:
        {'acl_id': str, 'type': str, 'changes': dict}
        - 'acl_id': The identifier of the acl.
        - 'type': Type of change ('update', 'removed', 'new').
        - 'changes': Dictionary representing the changes (present if 'type' is 'update').
    """
    source_acls_dict = {}
    feature_acls_dict = {}

    for value in source_acls:
        source_acls_dict.update(value)

    for value in feature_acls:
        feature_acls_dict.update(value)

    changed_acls = []
    # Check for changes and deletions
    for acl_name, source_acl in source_acls_dict.items():
        feature_acl = feature_acls_dict.get(acl_name)
        if not feature_acl:
            # ACL was removed
            changed_acls.append({acl_name: source_acls_dict.get(acl_name), "type": "removed"})
            logger.info(f"The following acl will be removed : {acl_name}")

    # Check for new additions
    for acl_name in feature_acls_dict:
        if acl_name not in source_acls_dict.keys():
            changed_acls.append({acl_name: feature_acls_dict.get(acl_name), "type": "new"})
            logger.info(f"The following acl will be added : {acl_name}")
    return changed_acls


def build_acl_rest_url(base_url, cluster_id):
    """
    Build the REST API URL for Kafka topics based on the provided base URL and cluster ID.

    Parameters:
    - base_url (str): The base URL of the Kafka REST API.
    - cluster_id (str): The ID of the Kafka cluster.

    Returns:
    str: The constructed REST API URL for Kafka topics.
    """
    return f'{base_url}/v3/clusters/{cluster_id}/acls/'


def add_new_acl(acl):
    """
    Add a new Kafka acl using the provided ACL configuration.

    Parameters:
    - acl (dict): Dictionary representing the configuration of the new Kafka ACL.

    """
    rest_acl_url = build_acl_rest_url(REST_PROXY_URL, CLUSTER_ID)
    acl_json = json.dumps(acl)
    print(acl_json)
    response = requests.post(rest_acl_url, auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS), data=acl_json, headers=HEADERS)
    if response.status_code == 201:
        logger.info(f"The acl {acl_json} has been successfully created")
    else:
        logger.error(f"The topic {acl.keys()[0]} returned {str(response.status_code)} due to the follwing reason: {response.reason}")


def delete_acl(acl):
    """
    Delete a Kafka acl based on the provided topic configuration.

    Parameters:
    - topic (dict): Dictionary representing the configuration of the Kafka acl, including the topic name.

    Raises:
    SystemExit: If the deletion fails, the program exits with status code 1.

    Notes:
    This method first checks if the topic exists by making a GET request to the Kafka REST API.
    If the topic exists, it proceeds to delete the topic using a DELETE request.
    """
    rest_acl_url = build_acl_rest_url(REST_PROXY_URL, CLUSTER_ID)
    response = requests.delete(rest_acl_url, auth=(BASIC_AUTH_USER, BASIC_AUTH_PASS), params=acl)
    if response.status_code == 200:
        logger.info(f"The acl {acl} has been successfully deleted")
    else:
        logger.error(f"The acl {acl} returned {str(response.status_code)} due to the following reason: {response.reason}")


def add_or_remove_acls(changed_acls):
    for i, topic in enumerate(changed_acls):
        acl_name = list(topic.keys())[i]
        acl_configs = list(topic.values())[i]
        if topic['type'] == 'new':
            add_new_acl(acl_configs)
        else:
            delete_acl(acl_configs)


def build_connect_rest_url(base_url, connector_name):
    """
    Build the REST API URL for Connect cluster on the provided base URL .

    Parameters:
    - base_url (str): The base URL of the Kafka REST API.

    Returns:
    str: The constructed REST API URL for connectors.
    """
    return f'{base_url}/connectors/{connector_name}/config'


def process_connector_changes(connector_file):
    # Add a new connector
    connector_name = connector_file.split("/connectors/")[1].replace(".json","")
    connect_rest_url = build_connect_rest_url(CONNECT_REST_URL, connector_name)
    json_file = open(connector_file)
    json_string_template = string.Template(json_file.read())
    json_string = json_string_template.substitute(**os.environ)

    response = requests.put(connect_rest_url, data=json_string, headers=HEADERS)
    if response.status_code == 201:
        logger.info(f"The connector {connector_name} has been successfully deleted")
    else:
        logger.error(f"The connector {connector_name} returned {str(response.status_code)} due to the following reason: {response.text}")


# @click.command()
if __name__ == "__main__":

    source_file = "application1/acls/acls.json"
    source_branch = "main"

    feature_file = "application1/acls/acls.json"
    feature_branch = "test"
    source_content, feature_content = get_content_from_branches(source_file, source_branch, feature_file, feature_branch)
    if "topic" in (source_file and feature_file):
        changed_topics = find_changed_topics(source_content, feature_content)
        process_changed_topics(changed_topics)

    if "acl" in (source_file and feature_file):
        changed_acls = find_changed_acls(source_content, feature_content)
        add_or_remove_acls(changed_acls)

    subprocess.run(['git', 'checkout', feature_branch]).stdout
    latest_sha = subprocess.run(['git', 'rev-parse', 'HEAD']).stdout
    if latest_sha is not None:
        output = subprocess.run(['git', 'diff', '--name-status', str(latest_sha)])
        print(output.stdout)

    if "connector" in feature_file:
        g = Github(GITHUB_TOKEN)
        repo = g.get_repo("NiyiOdumosu/kafkamanager")
        # process_connector_changes(feature_file)

