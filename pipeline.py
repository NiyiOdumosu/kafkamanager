from github import Github
import logging
import os
import json
from deepdiff import DeepDiff

# Constant variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Authenticate with github and get the latest diff
def get_latest_commit_diff():
    try:
        g = Github(GITHUB_TOKEN)
        repo = g.get_repo("NiyiOdumosu/kafkamanager")
        latest_commit = repo.get_commits()[0]
        feature_topic_content = repo.get_contents("application1/topics/topics.json", ref="test")
        main_topic_content = repo.get_contents("application1/topics/topics.json", ref="main")
        old_topics = json.loads(main_topic_content.decoded_content)
        new_topics = json.loads(feature_topic_content.decoded_content)

        return   old_topics, new_topics
    except Exception as e:
        logger.error(f"Error getting latest commit diff: {e}")
        raise


def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)


def find_changed_topics(old_topics, new_topics):
    old_topics_dict = {}
    new_topics_dict = {}

    for value in old_topics:
        old_topics_dict.update(value)

    for value in new_topics:
        new_topics_dict.update(value)

    changed_topic_names = []
    # Check for changes and deletions
    for topic_name, old_topic in old_topics_dict.items():
        new_topic = new_topics_dict.get(topic_name)
        if new_topic:
            diff = DeepDiff(old_topic, new_topic, ignore_order=True)
            if diff:
                changed_topic_names.append({topic_name: diff, "type": "update"})
        else:
            # Topic was removed
            changed_topic_names.append({topic_name: old_topics_dict.get(topic_name), "type": "removed"})

    # Check for new additions
    for topic_name in new_topics_dict:
        if topic_name not in old_topics_dict.keys():
            changed_topic_names.append({topic_name: new_topics_dict.get(topic_name), "type": "new"})

    return changed_topic_names


if __name__ == "__main__":
    old_topics, new_topics = get_latest_commit_diff()
    print(find_changed_topics(old_topics, new_topics))