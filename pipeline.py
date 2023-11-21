from github import Github

# Authentication is defined via github.Auth
from github import Auth
import os

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
print(GITHUB_TOKEN)
# using an access token
auth = Auth.Token(GITHUB_TOKEN)
# First create a Github instance:
# Public Web Github
g = Github(auth=auth)

# Github Enterprise with custom hostname
# g = Github(base_url="https://{hostname}/api/v3", auth=auth)

# Then play with your Github objects:
for repo in g.get_user().get_repos():
    print(repo.name)