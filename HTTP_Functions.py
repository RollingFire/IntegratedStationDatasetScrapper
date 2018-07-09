'''
By Austin Dorsey
Started: 5/29/18
Last Modified: 6/9/18
Discription: Manages the reading of FTP sites.
'''


import requests
import os.path
from time import sleep

def downloadFile(url, filename, destination, timeout=120):
    '''Downloads a file that is stored on a HTTP protocall website.'''
    for _ in range(5):
        try:
            page = requests.get(url + "/" + filename, stream=True, timeout=timeout)
            if page.status_code != 200:
                continue
            destination = os.path.abspath(os.path.join(destination, filename))
            with open(destination, 'wb') as file:
                for chunk in page.iter_content(chunk_size=1024):
                    file.write(chunk)
                file.close()
            return
        except TimeoutError:
            sleep(10)
            continue
    raise Exception("Max number of retrys reached for:", url + filename)