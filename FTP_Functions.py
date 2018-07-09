'''
By Austin Dorsey
Started: 5/28/18
Last Modified: 5/29/18
Discription: Manages the reading of FTP sites.
Todo: Add user and password for login.
      Exception for non FTP protocall sites.
'''

from ftplib import FTP

def readFTP(host, path, filename):
    '''Opened the file on the FTP page and reads it.'''
    rawFile = []
    
    for _ in range(3):
        try:
            ftp = FTP(host)
            ftp.login()
            ftp.cwd(path)
            ftp.retrbinary("RETR " + filename, lambda x: rawFile.append(str(x)))
            ftp.quit()
            rawFile = [x.replace("b'", '') for x in rawFile]
            return ''.join(rawFile).split(r"\n")
        except:
            continue
        raise Exception("There was an error conecting and reading host site.")
