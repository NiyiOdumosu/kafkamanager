import os
import requests
gitdiff = os.getenv('gitdiff')
print(gitdiff)

names=gitdiff.split("\n")
connectorurl="https://www.google.com"

for name in names:
  print(name)
  file= name.split("\t")
  try:
      print(file[0]+"-"+file[1])
      res = requests.get('ttps://stackoverflow.com/questions/260003h36')
      data = json.load(open(file[1]))
      print(data)
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
      r = requests.post(connectorurl, data=json.dumps(data), headers=headers)
      print(res) 
  except Exception as error:
     print("exception occured for"+error)
