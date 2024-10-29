import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

gill_api = os.getenv('GILL_API_KEY')
url = "https://api.giinfotech.ae/api/Hotel/DestinationInfo"

payload = json.dumps({
  "destination": "A Lama"
})
headers = {
  'ApiKey': f'{gill_api}',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
