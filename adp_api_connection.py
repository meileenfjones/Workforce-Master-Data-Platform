import tempfile
from pathlib import Path
import requests
import json


#############################################################################################################
class AdpApiConnection:
    def __init__(self, keepass_db, keepass_entry):
        self.__keepass_db = keepass_db
        self.__temp_directory = None
        self.temp_directory_created = False
        self.token_endpoint = 'https://accounts.adp.com/auth/oauth/v2/token'
        self.cert = None
        self.key = None
        self.grant_type = None
        self.access_token = None
        self.header = None
        self.keepass_entry = self.__keepass_db.find_entries(title=keepass_entry, first=True)
        if self.keepass_entry is None:
            raise Exception("FAILURE: Entry does not exist")

    def initialize_temp_directory(self):
        """
        creates and sets a temporary directory attribute to store cert files
        """
        self.__temp_directory = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.temp_directory_created = True

    def kill_connection(self):
        """
        deletes temporary directory if initialized along with cert files inside
        :return:
        """
        if self.temp_directory_created:
            self.__temp_directory.cleanup()
            self.temp_directory_created = False
        else:
            pass

    def generate_bearer_token(self):
        """
        generates the bearer token and saves cert paths and credentials necessary to access API
        :return: success or failure text
        """
        if self.temp_directory_created is True:
            self.kill_connection()
        self.initialize_temp_directory()
        client_id = self.keepass_entry.custom_properties['client_id']
        client_secret = self.keepass_entry.custom_properties['client_secret']
        self.grant_type = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}'
        # creates and populates 2 files in temp directory with decoded binary data and saves path to dictionary
        cert_path = {}
        for cert_file in ['public_cert.pem', 'private_key.txt']:
            binary = self.__keepass_db.find_attachments(element=self.keepass_entry, filename=cert_file, first=True)
            new_cert_path = Path(self.__temp_directory.name).joinpath(cert_file)
            new_cert_path.write_text(binary.data.decode())
            cert_path[cert_file] = str(new_cert_path)
        self.cert = cert_path['public_cert.pem']
        self.key = cert_path['private_key.txt']
        post_response = requests.post(self.token_endpoint, data=self.grant_type, cert=(self.cert, self.key))
        if post_response.status_code == 200:
            access_token = json.loads(post_response.text)["access_token"]
            self.access_token = access_token
            self.header = {'Accept': 'application/json;masked=false', 'Authorization': f'Bearer {access_token}'}
            return 'Successfully generated token'
        else:
            return 'Failure generating token'
