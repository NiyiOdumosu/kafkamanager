from github import Github
from deepdiff import DeepDiff
from datetime import datetime
from subprocess import PIPE

import click
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
REST_BASIC_AUTH_USER = os.getenv('REST_BASIC_AUTH_USER')
REST_BASIC_AUTH_PASS = os.getenv('REST_BASIC_AUTH_PASS')
CONNECT_BASIC_AUTH_USER = os.getenv('CONNECT_BASIC_AUTH_USER')
CONNECT_BASIC_AUTH_PASS = os.getenv('CONNECT_BASIC_AUTH_PASS')

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
                # Check if this is a partition change
                try:
                    if diff['values_changed']["root[\'partitions_count\']"]:
                        for change_type, details in diff.items():
                            change_dict = {
                                "topic_name": topic_name,
                                "changes": []
                            }
                            for change in details:
                                if change == "root[\'partitions_count\']":
                                    configs_changes = re.findall(r"\['(.*?)'\]", change)
                                    change_dict["changes"].append({
                                        configs_changes[0]: details[change]['new_value']
                                    })
                                else:
                                    configs_changes = re.findall(r"configs'\]\[(.*)\]\[", change)
                                    if configs_changes:
                                        for index in configs_changes:
                                            prop_name = feature_topics_dict[topic_name]['configs'][int(index)]
                                            change_dict["changes"].append({
                                                "name": prop_name["name"],
                                                "value": details[change]['new_value']
                                            })
                        changed_topic_names.append({"type": "update", "changes": change_dict})
                except KeyError as ke:
                    logger.error(ke)
                try:
                    if diff['values_changed']["root[\'configs\'][0][\'value\']"]:
                        for change_type, details in diff.items():
                            change_dict = {
                                "topic_name": topic_name,
                                "changes": []
                            }

                            for change in details:
                                configs_changes = re.findall(r"configs'\]\[(.*)\]\[", change)
                                if configs_changes:
                                    for index in configs_changes:
                                        prop_name = feature_topics_dict[topic_name]['configs'][int(index)]
                                        change_dict["changes"].append({
                                            "name": prop_name["name"],
                                            "value": details[change]['new_value']
                                        })

                        changed_topic_names.append({"type": "update", "changes": change_dict})
                except KeyError as ke:
                    logger.error(ke)
        else:
            # Topic was removed
            changed_topic_names.append({topic_name: source_topics_dict.get(topic_name), "type": "removed"})

    # Check for new additions
    for topic_name in feature_topics_dict:
        if topic_name not in source_topics_dict.keys():
            changed_topic_names.append({topic_name: feature_topics_dict.get(topic_name), "type": "new"})

    return changed_topic_names


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
    topic_name = topic["topic_name"]
    rest_topic_url = build_topic_rest_url(REST_PROXY_URL, CLUSTER_ID)

    get_response = requests.get(rest_topic_url + topic_name, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS))
    if get_response.status_code != 200:
        logger.info(f"Topic does not already exist. Please proceed with creating the topic")
    else:
        logger.error(f"Topic already exist. Will not create a the topic {topic_name}")
        exit(1)

    topic_json = json.dumps(topic)

    response = requests.post(rest_topic_url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=topic_json, headers=HEADERS)
    with open('CHANGELOG.md', 'a') as f:
        if response.status_code == 201:
            logger.info(f"The topic {topic['topic_name']} has been successfully created")
            f.writelines(f"{datetime.now()} - The topic {topic['topic_name']} has been successfully created\n")
        else:
            logger.error(f"The topic {topic['topic_name']} returned {str(response.status_code)} due to the follwing reason: {response.text}" )
            f.writelines(f"{datetime.now()} - The topic {topic['topic_name']} returned {str(response.status_code)} due to the follwing reason: {response.text}\n")


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
    response = requests.get(rest_topic_url + topic_name, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS))
    if response.status_code != 200:
        logger.error(f"The topic {topic_name} failed to be updated due to {response.status_code} - {response.text}")
        exit(1)

    current_topic_definition = response.json()
    # Check if the requested update is a config change
    if'name' in topic_config[0].keys():
        update_topic_configs(rest_topic_url, topic_config, topic_name)
    elif ('partitions_count' in topic_config[0].keys()) and ('name' in topic_config[1].keys()):
        update_partition_count(current_topic_definition, rest_topic_url, topic_config[0]['partitions_count'], topic_name)
        topic_config.pop(0)
        update_topic_configs(rest_topic_url, topic_config, topic_name)
    else:
        update_partition_count(current_topic_definition, rest_topic_url, topic_config[0]['partitions_count'], topic_name)


def update_topic_configs(rest_topic_url, topic_config, topic_name):
    updated_Configs = "{\"data\":" + json.dumps(topic_config) + "}"
    logger.info("altering configs to " + updated_Configs)
    with open('CHANGELOG.md', 'a') as f:
        response = requests.post(f"{rest_topic_url}{topic_name}" + "/configs:alter",
                                 auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=updated_Configs, headers=HEADERS)
        if response.status_code == 204:
            f.writelines(f"{datetime.now()} - The configs {updated_Configs} was successfully applied to {topic_name}\n")
            logger.info(f"The configs {updated_Configs} was successfully applied to {topic_name}\n")
        else:
            f.writelines(f"Topic configs failed to be applied to the topic due to {str(response.status_code)} this is the reason: {response.text}\n")
            logger.error(f"Topic configs failed to be applied to the topic due to {str(response.status_code)} this is the reason: {response.text}\n")


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
        if new_partition_count == current_partitions_count:
            logger.info(f"Requested partition count and current partition count is the same - {new_partition_count}")
        if new_partition_count > current_partitions_count:
            logger.info(f"A requested increase of partitions for topic  {topic_name} is from "
                        f"{str(current_partitions_count)} to {str(new_partition_count)}")
            partition_response = requests.patch(f"{rest_topic_url}{topic_name}",
                                                auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS),
                                                data="{\"partitions_count\":" + str(new_partition_count) + "}")
            with open('CHANGELOG.md', 'a') as f:
                if partition_response.status_code != 200:
                    logger.info(
                        f"The partition increase failed for topic {topic_name} due to {str(partition_response.status_code)} -  {partition_response.text}")
                    f.writelines(f"{datetime.now()} - The partition increase for topic {topic_name} was successful\n")
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

    get_response = requests.get(rest_topic_url + topic_name, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS))
    if get_response.status_code == 200:
        logger.info(f"Response code is {str(get_response.status_code)}")
    else:
        logger.error(f"Failed due to the following status code {str(get_response.status_code)} and reason {str(get_response.text)}" )

    response = requests.delete(rest_topic_url + topic_name, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS))
    with open('CHANGELOG.md', 'a') as f:
        if response.status_code == 204:
            logger.info(f"The topic {topic_name} has been successfully deleted")
            f.writelines(f"{datetime.now()} - {topic_name} has been successfully deleted\n")
        else:
            logger.error(f"The topic {topic_name} returned {str(response.status_code)} due to the following reason: {response.text}" )
            f.writelines(f"{datetime.now()} - {topic_name} attempted to be deleted but returned {str(response.status_code)} due to the following reason: {response.text}\n")


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
    response = requests.post(rest_acl_url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=acl_json, headers=HEADERS)
    with open('CHANGELOG.md', 'a') as f:
        if response.status_code == 201:
            logger.info(f"The acl {acl_json} has been successfully created")
            f.writelines(f"{datetime.now()} - {acl_json} has been successfully created\n")
        else:
            logger.error(f"The acl {acl_json} returned {str(response.status_code)} due to the following reason: {response.text}")
            f.writelines(f"{datetime.now()} - {acl_json} attempted to be created but was unsuccessful. REST API returned {str(response.status_code)} due to the following reason: {response.text}\n")


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
    response = requests.delete(rest_acl_url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), params=acl)
    with open('CHANGELOG.md', 'a') as f:
        if response.status_code == 200:
            logger.info(f"The acl {acl} has been successfully deleted")
            f.writelines(f"{datetime.now()} - {acl} has been successfully deleted\n")
        else:
            logger.error(f"The acl {acl} returned {str(response.status_code)} due to the following reason: {response.text}")
            f.writelines(f"{datetime.now()} - {acl} attempted to be deleted but was unsuccessful. REST API returned {str(response.status_code)} due to the following reason: {response.text}\n")


def add_or_remove_acls(changed_acls):
    for i, acls in enumerate(changed_acls):
        acl_configs = list(acls.values())
        if acls['type'] == 'new':
            add_new_acl(acl_configs[0])
        elif acls['type'] == 'removed':
            delete_acl(acl_configs[0])
        else:
            continue


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

    response = requests.put(connect_rest_url, data=json_string, auth=(CONNECT_BASIC_AUTH_USER, CONNECT_BASIC_AUTH_PASS), headers=HEADERS)
    with open('CHANGELOG.md', 'a') as f:
        if response.status_code == 201:
            logger.info(f"The connector {connector_name} has been successfully created")
            f.writelines(f"{datetime.now()} - The connector {connector_name} has been successfully created\n")
        else:
            logger.error(f"The connector {connector_name} returned {str(response.status_code)} due to the following reason: {response.text}")
            f.writelines(f"{datetime.now()} - The connector {connector_name} returned {str(response.status_code)} due to the following reason: {response.text}\n")


def delete_connector(connector_file):
    # Remove a connector
    connector_name = connector_file.split("/connectors/")[1].replace(".json","")
    connect_rest_url = build_connect_rest_url(CONNECT_REST_URL, connector_name)
    json_file = open(connector_file)
    json_string_template = string.Template(json_file.read())
    json_string = json_string_template.substitute(**os.environ)

    response = requests.delete(f"{connect_rest_url}/{connector_name}", auth=(CONNECT_BASIC_AUTH_USER, CONNECT_BASIC_AUTH_PASS), headers=HEADERS)
    with open('CHANGELOG.md', 'a') as f:
        if response.status_code == 204:
            logger.info(f"The connector {connector_name} has been successfully created")
            f.writelines(f"{datetime.now()} - The connector {connector_name} has been successfully created\n")
        else:
            logger.error(f"The connector {connector_name} returned {str(response.status_code)} due to the following reason: {response.text}")
            f.writelines(f"{datetime.now()} - The connector {connector_name} returned {str(response.status_code)} due to the following reason: {response.text}\n")


def main():
    latest_sha = subprocess.run(['git', 'rev-parse', 'HEAD', ], stdout=PIPE, stderr=PIPE).stdout
    previous_sha = subprocess.run(['git', 'rev-parse', 'HEAD~1',], stdout=PIPE, stderr=PIPE).stdout

    latest_commit = latest_sha.decode('utf-8').rstrip('\n')
    previous_commit = previous_sha.decode('utf-8').rstrip('\n')

    files = subprocess.run(['git', 'diff', '--name-status', previous_commit, latest_commit], stdout=PIPE, stderr=PIPE).stdout
    files_string = files.decode('utf-8')
    pattern = re.compile(r'([AMD])\s+(.+)')
    files_list = [match.group(1) + ' ' + match.group(2) for match in pattern.finditer(files_string)]

    current_topics = 'application1/topics/current-topics.json'
    previous_topics = 'application1/topics/previous-topics.json'

    current_acls = 'application1/acls/current-acls.json'
    previous_acls = 'application1/acls/previous-acls.json'

    for file in files_list:
        try:
            if "topics.json" in file:
                filename = file.split(" ")[1]
                current_topics_command = f"git show HEAD:{filename} > {current_topics}"
                previous_topics_command = f"git show HEAD~1:{filename} > {previous_topics}"

                subprocess.run(current_topics_command, stdout=PIPE, stderr=PIPE, shell=True)
                subprocess.run(previous_topics_command, stdout=PIPE, stderr=PIPE, shell=True)
                with open(previous_topics, 'r') as previous_topics_file:
                    source_topics = json.load(previous_topics_file)
                with open(current_topics, 'r') as current_topics_file:
                    feature_topics = json.load(current_topics_file)
                changed_topics = find_changed_topics(source_topics, feature_topics)
                process_changed_topics(changed_topics)
        except json.decoder.JSONDecodeError as error:
            logger.error(error)

        try:
            if "acls.json" in file:
                filename = file.split(" ")[1]
                current_acls_command = f"git show HEAD:{filename} > {current_acls}"
                previous_acls_command = f"git show HEAD~1:{filename} > {previous_acls}"
                subprocess.run(current_acls_command, stdout=PIPE, stderr=PIPE, shell=True)
                subprocess.run(previous_acls_command, stdout=PIPE, stderr=PIPE, shell=True)
                with open(previous_acls, 'r') as previous_acls_file:
                    source_acls = json.load(previous_acls_file)
                with open(current_acls, 'r') as current_acls_file:
                    feature_acls = json.load(current_acls_file)
                changed_acls = find_changed_acls(source_acls, feature_acls)
                add_or_remove_acls(changed_acls)
        except json.decoder.JSONDecodeError as error:
            logger.error(error)

        if ("connectors" in file) and ('D ' in file):
            filename = file.split(" ")[1]
            delete_connector(filename)
        elif (("connectors" in file) and ('M ' in file)) or (("connectors" in file) and ('A ' in file)):
            filename = file.split(" ")[1]
            process_connector_changes(filename)
        else:
            logger.info("No Kafka resource changes were detected")


if __name__ == '__main__':
    main()