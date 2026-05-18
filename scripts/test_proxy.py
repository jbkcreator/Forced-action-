import requests
from config.settings import get_settings

s = get_settings()
user = s.oxylabs_username
pw = s.oxylabs_password.get_secret_value()
proxy = {"https": f"http://{user}:{pw}@pr.oxylabs.io:7777",
         "http":  f"http://{user}:{pw}@pr.oxylabs.io:7777"}

targets = [
    "https://ip.oxylabs.io/location",
    "https://officialrecords.mypinellasclerk.gov",
    "https://publicaccess.hillsclerk.com/oripublicaccess/",
]
for url in targets:
    try:
        r = requests.get(url, proxies=proxy, timeout=20)
        print(f"OK  {url}  →  {r.status_code}")
    except Exception as e:
        print(f"FAIL {url}  →  {e}")
