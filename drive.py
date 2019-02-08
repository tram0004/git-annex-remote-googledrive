import pickle

import os

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

class Remotefolder:
    def __init__(self, service, name, id_):
        self.service = service
        self.id = id_
        self.name = name
        
    def child(self, name):
        result = self.service.files().list(
                pageSize=1,
                fields="nextPageToken, files(id, name, mimeType)",
                q="'{this}' in parents and name='{name}'".format(this=self.id, name=name)
            ).execute()
        if "nextPageToken" in result:
            raise Exception("Two or more files {name}".format(name=name))
        if not result['files']:
            return None
        return self._reply_to_object(result["files"][0])
        
    def mkdir(self, name):
        file_ = self.child(name)
        if file_:
            if not hasattr(file_, "child"):
                raise Exception("Filename already exists ({name}) and it's not a folder.".format(name=name))
            return file_

        file_metadata = {
            'name': name, 
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.id]
        }
        result = self.service.files().create(body=file_metadata, fields='id, name').execute()
        result['mimeType'] = 'application/vnd.google-apps.folder'
        return self._reply_to_object(result)
        
    def upload_key(self, key, local_file):
        # this won't be the final solution
        # upload should be performed by Key.store("local_file")
        # upload should be resumable. id to resume should be stored in annex
        file_ = self.child(key)
        if file_:
            raise Exception("Filename already exists ({name}).".format(name=name))

        file_metadata = {
            'name': key, 
            'parents': [self.id]
        }
        media = MediaFileUpload(local_file)
        remote_file = self.service.files().create(body=file_metadata,
                                                    media_body=media,
                                                    fields='id').execute()
        return Key(self.service, key, remote_file['id'])
        
        
    def child_from_path(self, path):
        splitpath = path.strip('/').split('/', 1)
        child = self.child(splitpath[0])
        if len (splitpath) == 1:
            return child
        else:
            return child.child_from_path(splitpath[1])
        
    def remove(self):
        self.service.files().delete(fileId=self.id).execute()
        self.id = None
        
    def _reply_to_object(self, reply):
        if reply['mimeType'] == 'application/vnd.google-apps.folder':
            return Remotefolder(self.service, reply['name'], reply['id'])
        else:
            return Key(self.service, reply['name'], reply['id'])
            
class Key:
    def __init__(self, service, key, id_=None):
        self.key = key
        self.id = id_
        self.service = service
  
    def remove(self):
        self.service.files().delete(fileId=self.id).execute()
        self.id = None
        
    def receive(self, local_file, chunksize=10**7, progress_handler=None):
        try:
            local_file_size = os.path.getsize(local_file)
        except FileNotFoundError:
            local_file_size = 0
        
        remote_file_size = int(self.service.files().\
                            get(fileId=self.id, fields="size").\
                            execute()['size'])
        
        download_url = "https://www.googleapis.com/drive/v3/files/{fileid}?alt=media".\
                                format(fileid=self.id)
        
        with open(local_file, 'ab') as fh:
            while local_file_size < remote_file_size:
                download_range = "bytes={}-{}".\
                    format(local_file_size, local_file_size+chunksize-1)
                    
                # replace with googleapiclient.http.HttpRequest if possible
                # or patch MediaIoBaseDownload to support Range
                resp, content = self.service._http.request(
                                            download_url,
                                            headers={'Range': download_range})
                if resp.status == 206:
                        fh.write(content)
                        local_file_size+=int(resp['content-length'])
                        if progress_handler:
                            progress_handler(local_file_size)
                else:
                    raise HttpError(resp, content)

    def store(self, local_file, chunksize=10**7, progress_handler=None):
        raise NotImplementedError

class GoogleDrive(Remotefolder):
    def __init__(self):
        self.creds = None
        self.id = "root"
        
    def set_root(self, folder):
        self.id = folder.id
        self.name = folder.name
    
    def connect(self):
        self.auth()
        self.service = build('drive', 'v3', credentials=self.creds)
        
    def auth(self):
        SCOPES = ['https://www.googleapis.com/auth/drive']
        
        if not self.creds:
            try:
                with open('token.pickle', 'rb') as token:
                    self.creds = pickle.load(token)
            except FileNotFoundError:
                pass
            
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                try:
                    self.creds = flow.run_local_server()
                except OSError:
                    self.creds = flow.run_console()
            with open('token.pickle', 'wb') as token:
                pickle.dump(self.creds, token)
