import requests
from requests.auth import HTTPBasicAuth
import os

class BulkDownloader:
    """Core engine for all bulk file downloads (HTTP/FTP/HTTPS)."""
    
    def __init__(self, username=None, password=None):
        self.auth = HTTPBasicAuth(username, password) if username else None

    def download(self, url, destination):
        """Standardized streaming download with basic progress logging."""
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        print(f"[*] Starting download: {url}")
        
        try:
            with requests.get(url, auth=self.auth, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(destination, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                        f.write(chunk)
            print(f"[+] Successfully saved to: {destination}")
            return True
        except Exception as e:
            print(f"[!] Download failed for {url}: {e}")
            return False