import requests
import json

url = "http://localhost:5000/getAll"

payload = json.dumps({
  "start_date": "2026-04-05",
  "end_date": "2026-04-05",
  "limit": 2
})
headers = {
  'accept': 'application/json',
  'Authorization': 'Basic YWRtaW46bWFuaXNo',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
