from github import Github
from deepdiff import DeepDiff

import click
import json
import logging
import os
import pandas as pd
import requests
import re
import string

# Constant variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}
REST_PROXY_URL = os.getenv('REST_URL')
CLUSTER_ID = os.getenv('KAFKA_CLUSTER_ID')
CONNECT_REST_URL = os.getenv('CONNECT_REST_URL')
REPO = os.getenv('REPO')
REST_BASIC_AUTH_USER = os.getenv('REST_BASIC_AUTH_USER')
REST_BASIC_AUTH_PASS = os.getenv('REST_BASIC_AUTH_PASS')
ENV = os.getenv('env')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_files(pr_id):
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO)

    pull_request = repo.get_pull(int(pr_id))
    commits = pull_request.get_commits()

    head_branch = pull_request.head.ref
    base_branch = pull_request.base.ref
    files_list = []
    for commit in commits:
        files = commit.files
        for file in files:
            files_list.append(f"{file.filename}-{file.status}")
            files_set = set(files_list)

    return repo, files_set, head_branch, base_branch


def get_content_from_branches(repo, filename, head_branch, base_branch):
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

        head_file_content = repo.get_contents(filename, ref=head_branch)
        base_file_content = repo.get_contents(filename, ref=base_branch)
        head_file_content_json = json.loads(base_file_content.decoded_content)
        base_file_content_json = json.loads(head_file_content.decoded_content)

        return head_file_content_json, base_file_content_json
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

    logger.info(f"The topic {topic['topic_name']} will be created once the PR is merged")


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
        response = requests.get(rest_topic_url + topic_name)
    except Exception as e:
        logger.error(e)

    current_topic_definition = response.json()
    # Check if the requested update is a config change
    if 'partitions_count' in topic_config[0].keys():
        update_partition_count(current_topic_definition, rest_topic_url, topic_config[0]['partitions_count'], topic_name)
    else:
        updated_Configs = "{\"data\":" + json.dumps(topic_config) + "}"
        logger.info(f"The topic {topic_name} will be updated with the following topic configs {updated_Configs} once the PR is merged")


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
                        f"{str(current_partitions_count)} to {str(new_partition_count)}. This will be applied after the PR is merged.")
        elif new_partition_count < current_partitions_count:
            logger.error(f"Cannot reduce partition count from {str(current_partitions_count)} to {str(new_partition_count)} for a given topic")
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

    get_response = requests.get(rest_topic_url + topic_name)
    if get_response.status_code == 200:
        logger.info(f"Response code is {str(get_response.status_code)}")
        logger.info(f"The topic {topic_name} will be deleted once the PR is merged.")
    else:
        logger.error(f"Failed due to the following status code {str(get_response.status_code)} and reason {str(get_response.reason)}" )


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
    logger.info(f"The acl {acl[0]} will be created once the PR is merged")


def get_application_owner(filename):
    df = pd.read_csv(filename)
    for index, row in df.iterrows():
        ba_id = row['ba.id']

    ## service now logic

    ## response with app owner
    # with open(f'{filename}_application_owner', 'w') as json_file:
# json_file.write(json_output)

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

    get_response = requests.get(rest_acl_url + acl['topic_name']) # change this
    if get_response.status_code == 200:
        logger.info(f"Response code is {str(get_response.status_code)}")
        logger.info(f"The connector {acl[0].keys()} will be removed once the PR is merged.")
    else:
        logger.error(f"Failed due to the following status code {str(get_response.status_code)} and reason {str(get_response.reason)}" )


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
    json_file = open(connector_file)
    json_string_template = string.Template(json_file.read())
    json_string = json_string_template.substitute(**os.environ)

    connector_configs = json.loads(json_string)

    rest_topic_url = build_topic_rest_url(REST_PROXY_URL, CLUSTER_ID)

    topics = connector_configs['topics']
    if ',' in topics:
        topic_list = topics.split(',')
        for topic in topic_list:
            verify_topic_in_connector(connector_name, rest_topic_url, topic)
    else:
        verify_topic_in_connector((connector_name, rest_topic_url, topics))

    logger.info(f"The connector {connector_name} will be added once the PR is merged with the following configs {json_string}")


def verify_topic_in_connector(connector_name, rest_topic_url, topic):
    topic_response = requests.get(rest_topic_url + topic, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS))
    if topic_response.status_code == 200:
        logger.info(f"Topic {topic} for connector {connector_name} currently exists")
    else:
        logger.error(
            f"Topic {topic} for connector {connector_name} currently does not exist - {str(topic_response.status_code)}")
        exit(1)


def delete_connector(connector_file):
    # Remove a connector
    connector_name = connector_file.split("/connectors/")[1].replace(".json","")
    logger.info(f"The connector {connector_name} will be deleted once the PR is merged")


@click.command()
@click.argument('pr_id')
def main(pr_id):

    repo, files_set, head_branch, base_branch = get_files(pr_id)
    env = base_branch = string.split('-')[-1]
    print(files_set)
    for file in files_set:
        if f"topics_{env}.json" in file:
            filename = file.split("-")[0]
            head_content, base_content = get_content_from_branches(repo, filename, head_branch, base_branch)
            changed_topics = find_changed_topics(head_content, head_content)
            process_changed_topics(changed_topics)
        if f"acls_{env}.json" in file:
            filename = file.split("-")[0]
            head_content, base_content = get_content_from_branches(repo, filename, head_branch, base_branch)
            changed_acls = find_changed_acls(head_content, head_content)
            add_or_remove_acls(changed_acls)
        if ("connectors" in file) and (f"-{env}" in file) and ('D ' in file):
            filename = file.split(" ")[1]
            delete_connector(filename)
        elif (("connectors" in file) and (f"-{env}" in file) and ('M ' in file)) or (("connectors" in file) and (f"-{env}" in file) and ('A ' in file)):
            filename = file.split(" ")[1]
            process_connector_changes(filename)
        elif (("connectors" in file) and (f"-{env}" in file) and ('R ' in file)) or (("connectors" in file) and (f"-{env}" in file) and ('A ' in file)):
            filename = file.split(" ")[1]
            process_connector_changes(filename)


if __name__ == "__main__":
    main()