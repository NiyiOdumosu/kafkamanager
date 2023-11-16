import os
import requests
import json
gitdiff = os.getenv('gitdiff')
print(gitdiff)

names=gitdiff.split("\n")
connectorurl="http://localhost:8083/connectors/"

for name in names:
  print(name)
  file= name.split("\t")
  try:
      print(file[0]+"-"+file[1])
      if "connector-definitions" not in file[1]:
         print('continuing for'+file[1])
         continue
      else:
         connectorName = file[1].replace(".json","").replace("connector-definitions/","")
         if file[0]=='D': 
            
            print("deleting connector"+connectorName)
            r=requests.delete(connectorurl+connectorName)
            print(r)
          
         elif file[0]=='A' or file[0]=='M': 
            
            if file[0]=='A': 
               print("creating connector "+connectorName)
            else:
               print("updating connector "+connectorName)
               jsonFile=open(file[1])
               jsonstring=jsonFile.read()
               print("this is the json string before replace "+jsonstring)
               jsonstring=jsonstring.format(user=os.getenv('user')) 

               #data = json.load(jsonstring)  
               
               print("this is the json string after replace "+jsonstring)   
               headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
               r = requests.put(connectorurl+connectorName+"/config", data=jsonstring, headers=headers)
               print(r) 
       
      
  except Exception as error:
     print(error)
