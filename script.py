import os
import requests
import json
import string
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_environment_variable(variable_name):
    try:
        return os.environ[variable_name]
    except KeyError:
        logger.error(f"Environment variable {variable_name} not set.")
        raise

def get_git_diff():
    git_diff = get_environment_variable('gitdiff')
    logger.info(f"Git diff:\n{git_diff}")
    return git_diff.split("\n")

def process_topic_changes(topic_names, rest_topic_url):
    for name in topic_names:
        try:
            logger.info(f"Processing topic change: {name}")
            file = name.split("\t")
            logger.info(f"File details: {file[0]} - {file[1]}")

            app_name_topic_name = file[1].split("/topics/")
            topic_name = app_name_topic_name[1].replace(".json", "")
            app_name = app_name_topic_name[0]

            if file[0] == 'D':
                logger.info(f"Deleting topic: {topic_name}")
                response = requests.delete(rest_topic_url + topic_name)
                logger.info(f"Delete response: {response}")

            elif file[0] == 'A' or file[0] == 'M':
                jsonFile=open(file[1])
            jsonstring=string.Template(jsonFile.read())
            jsonstring=jsonstring.substitute(**os.environ)

            print("final topic json "+jsonstring)

            headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
            response_code = 200
            response_reason =''
            if file[0]=='A':
                add_new_topic(headers, jsonstring, rest_topic_url, topic_name)
            else:
                print(f"updating topic {topic_name}")
                update_existing_topic(headers, jsonstring, rest_topic_url, topic_name)

        except Exception as error:
            logger.error(f"Error processing topic change: {error}")
            raise


def update_existing_topic(headers, jsonstring, rest_topic_url, topic_name):
    response = requests.get(f"{rest_topic_url}{topic_name}", headers=headers)
    currentTopicDefinition = response.json()
    print("existing topic definition ")
    print(currentTopicDefinition)
    currentPartitionsCount = currentTopicDefinition['partitions_count']
    requestedChanges = json.loads(jsonstring)
    print("requested topic definition ")
    print(requestedChanges)
    if (requestedChanges['partitions_count'] > currentPartitionsCount):
        print("requested increasing partitions from " + str(currentPartitionsCount) + " to " + str(
            requestedChanges['partitions_count']))
        r = requests.patch(f"{rest_topic_url}{topic_name}",
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
    r = requests.post(f"{rest_topic_url}{topic_name}" + "/configs:alter", data=updateConfigs, headers=headers)
    response_code = str(r.status_code)
    response_reason = r.reason
    print("this is the code " + response_code + " this is the reason: " + response_reason)
    if (response_code.startswith("2") == False):
        exit(1)


def add_new_topic(headers, jsonstring, rest_topic_url, topic_name):
    logger.info(f"Remaining logic for topic {topic_name}")
    r = requests.post(rest_topic_url, data=jsonstring, headers=headers)
    response_code = str(r.status_code)
    response_reason = r.reason
    print("this is the code " + response_code + " this is the reason: " + response_reason)
    if (response_code.startswith("2") == False):
        exit(1)


def process_connector_changes(connector_files, connector_url):
    for name in connector_files:
        try:
            logger.info(f"Processing connector change: {name}")
            file = name.split("\t")
            logger.info(f"File details: {file[0]} - {file[1]}")

            app_name_connector_name = file[1].split("/connector-definitions/")
            connector_name = app_name_connector_name[1].replace(".json", "")
            app_name = app_name_connector_name[0]

            if file[0] == 'D':
                logger.info(f"Deleting connector: {connector_name}")
                response = requests.delete(connector_url + connector_name)
                logger.info(f"Delete response: {response}")

            elif file[0] == 'A' or file[0] == 'M':
                # ... (remaining connector processing logic)
                logger.info(f"Remaining logic for connector {connector_name}")

        except Exception as error:
            logger.error(f"Error processing connector change: {error}")
            raise

def main():
    try:
        git_diff = get_git_diff()
        rest_topic_url = os.getenv('restURL') + "v3/clusters/" + os.getenv('kafkaClusterID') + "/topics/"
        connector_url = get_environment_variable('connectorURL')

        topic_names = [name for name in git_diff if "topics/" in name]
        connector_files = [name for name in git_diff if "connector-definitions/" in name]

        logger.info("Topic changes:")
        logger.info(topic_names)

        logger.info("Connector file changes:")
        logger.info(connector_files)

        process_topic_changes(topic_names, rest_topic_url)
        process_connector_changes(connector_files, connector_url)

    except Exception as error:
        logger.error(f"An error occurred: {error}")
        exit(1)

if __name__ == "__main__":
    main()
