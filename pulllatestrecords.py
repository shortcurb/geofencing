from support_connections.connections import hitadminprod
import json,datetime
from dotenv import load_dotenv
import requests,os


load_dotenv()


method = 'GET'
urlsuffix = 'telematics'
end = datetime.datetime.now()
start = end - datetime.timedelta(seconds = 60*10)
start = datetime.datetime.strftime(start,'%Y-%m-%dT%H:%M:%S.00Z')
end = datetime.datetime.strftime(end,'%Y-%m-%dT%H:%M:%SZ')
payload = {'start':start,'end':end,'offset':0,'limit':100}
print(payload)
headers = {'Content-Type':'application/json','Authorization':'Bearer '+os.environ['adminprodtoken']}
token = requests.request('POST','https://zoho.fluidfleet.io/api/login',headers = {'Content-Type':'application/json'},data = json.dumps({"email":"aevans@fluidtruck.com","password":"Volleybal7"})).json()['data']['token']
headers.update({'Authorization':'Bearer '+token,'Content-Type':'xxx-form-urlencoded'})
r = requests.request('GET','https://zoho.fluidfleet.io/api/telematics',params = payload,headers = headers)

print(r)
print(r.text)
print(json.dumps(r.json(),indent=2))

'https://zoho.fluidfleet.io/api/telematics?start=2024-06-25T18:06:50.00Z&end=2024-06-25T18:16:50.00Z&offset=0&limit=10000'

'''
I need to set up the zoho.fluidfleet API but I need to get the docs from Kevin first
Probably set up the key refresh and stuff in management directory
But it does work!!!

'''
