import json
import logging
import uuid
import os
import shutil
import dateparser
import pytz

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from client.es_client import ElasticsearchClient
from legacy_client.legacy_es_client import LegacyClient
from client.ssh_utils import SomeSSHException, get_private_key
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError

# configuration variables
KEY_GROUP_DB = 'db'
KEY_DB_HOSTNAME = 'hostname'
KEY_DB_PORT = 'port'
KEY_QUERY = 'request_body'  # this is named like this for backwards compatibility
KEY_INDEX_NAME = 'index_name'
KEY_STORAGE_TABLE = 'storage_table'
KEY_PRIMARY_KEYS = 'primary_keys'
KEY_INCREMENTAL = 'incremental'
KEY_GROUP_AUTH = 'authentication'
KEY_AUTH_TYPE = 'auth_type'
KEY_USERNAME = 'username'
KEY_PASSWORD = '#password'
KEY_API_KEY_ID = 'api_key_id'
KEY_API_KEY = '#api_key'
KEY_BEARER = '#bearer'
KEY_SCHEME = 'scheme'

KEY_GROUP_DATE = 'date'
KEY_DATE_APPEND = 'append_date'
KEY_DATE_FORMAT = 'format'
KEY_DATE_SHIFT = 'shift'
KEY_DATE_TZ = 'time_zone'
DATE_PLACEHOLDER = '{{date}}'
DEFAULT_DATE = 'yesterday'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_TZ = 'UTC'

KEY_SSH = "ssh_options"
KEY_USE_SSH = "enabled"
KEY_SSH_KEYS = "keys"
KEY_SSH_PRIVATE_KEY = "#private"
KEY_SSH_USERNAME = "user"
KEY_SSH_TUNNEL_HOST = "sshHost"
KEY_SSH_TUNNEL_PORT = "sshPort"

LOCAL_BIND_ADDRESS = "127.0.0.1"

KEY_LEGACY_SSH = 'ssh'

REQUIRED_PARAMETERS = [KEY_GROUP_DB]

RSA_HEADER = "-----BEGIN RSA PRIVATE KEY-----"


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        if params.get(KEY_LEGACY_SSH):
            self.run_legacy_client()
        else:
            out_table_name = params.get(KEY_STORAGE_TABLE, False)
            if not out_table_name:
                out_table_name = "ex-elasticsearch-result"
                logging.info(f"Using default output table name: {out_table_name}")

            user_defined_pk = params.get(KEY_PRIMARY_KEYS, [])
            incremental = params.get(KEY_INCREMENTAL, False)

            index_name, query = self.parse_index_parameters(params)
            statefile = self.get_state_file()

            ssh_options = params.get(KEY_SSH)
            # fix eternal KBC issue
            if not isinstance(ssh_options, list):
                if ssh_options.get(KEY_USE_SSH, False):
                    self._create_and_start_ssh_tunnel(params)

            client = self.get_client(params)

            temp_folder = os.path.join(self.data_folder_path, "temp")
            os.makedirs(temp_folder, exist_ok=True)

            columns = statefile.get(out_table_name, [])
            out_table = self.create_out_table_definition(out_table_name, primary_key=user_defined_pk,
                                                         incremental=incremental)

            try:
                with ElasticDictWriter(out_table.full_path, columns) as wr:
                    for result in client.extract_data(index_name, query):
                        wr.writerow(result)
                    wr.writeheader()
            except Exception as e:
                raise UserException(f"Error occured while extracting data from Elasticsearch: {e}")
            finally:
                if hasattr(self, 'ssh_tunnel') and self.ssh_tunnel.is_active:
                    self.ssh_tunnel.stop()

            self.write_manifest(out_table)
            statefile[out_table_name] = wr.fieldnames
            self.write_state_file(statefile)
            self.cleanup(temp_folder)

    @staticmethod
    def run_legacy_client() -> None:
        client = LegacyClient()
        client.run()

    @staticmethod
    def cleanup(temp_folder: str):
        shutil.rmtree(temp_folder)

    def get_client(self, params: dict) -> ElasticsearchClient:
        auth_params = params.get(KEY_GROUP_AUTH)
        if not auth_params:
            return self.get_client_legacy(params)

        db_params = params.get(KEY_GROUP_DB)
        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)
        scheme = params.get(KEY_SCHEME, "http")

        auth_type = auth_params.get(KEY_AUTH_TYPE, False)
        if auth_type not in ["basic", "api_key", "bearer", "no_auth"]:
            raise UserException(f"Invalid auth_type: {auth_type}")

        setup = {"host": db_hostname, "port": db_port, "scheme": scheme}

        logging.info(f"The component will use {auth_type} type authorization.")

        if auth_type == "basic":
            username = auth_params.get(KEY_USERNAME)
            password = auth_params.get(KEY_PASSWORD)

            if not (username and password):
                raise UserException("You must specify both username and password for basic type authorization")

            auth = (username, password)
            client = ElasticsearchClient([setup], scheme, http_auth=auth)

        elif auth_type == "api_key":
            api_key_id = auth_params.get(KEY_API_KEY_ID)
            api_key = auth_params.get(KEY_API_KEY)
            api_key = (api_key_id, api_key)
            client = ElasticsearchClient([setup], scheme, api_key=api_key)

        elif auth_type == "no_auth":
            client = ElasticsearchClient([setup], scheme)

        else:
            raise UserException(f"Unsupported auth_type: {auth_type}")

        try:
            p = client.ping(error_trace=True)
            if not p:
                raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed.")
        except Exception as e:
            raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed. {str(e)}")

        return client

    @staticmethod
    def get_client_legacy(params) -> ElasticsearchClient:
        db_params = params.get(KEY_GROUP_DB)
        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)

        setup = {"host": db_hostname, "port": db_port, "scheme": "http"}
        client = ElasticsearchClient([setup])

        return client

    def parse_index_parameters(self, params):
        index = params.get(KEY_INDEX_NAME, "")
        date_config = params.get(KEY_GROUP_DATE, {})
        query = self._parse_query(params)

        if DATE_PLACEHOLDER in index:
            index = self._replace_date_placeholder(index, date_config)

        return index, query

    @staticmethod
    def _parse_query(params):
        _query = params.get(KEY_QUERY, '{}').strip()
        query_string = _query if _query != '' else '{}'

        try:
            logging.info(f"Using query: {query_string}")
            return json.loads(query_string)
        except ValueError:
            raise UserException("Could not parse request body string to JSON.")

    def _replace_date_placeholder(self, index, date_config):
        _date = dateparser.parse(date_config.get(KEY_DATE_SHIFT, DEFAULT_DATE))
        if _date is None:
            raise UserException(f"Could not parse value {date_config[KEY_DATE_SHIFT]} to date.")

        _date = _date.replace(tzinfo=pytz.UTC)
        _tz = self._validate_timezone(date_config.get(KEY_DATE_TZ, DEFAULT_TZ))
        _date_tz = pytz.timezone(_tz).normalize(_date)
        _date_formatted = _date_tz.strftime(date_config.get(KEY_DATE_FORMAT, DEFAULT_DATE_FORMAT))

        logging.info(f"Replaced date placeholder with value {_date_formatted}. "
                     f"Downloading data from index {index.replace(DATE_PLACEHOLDER, _date_formatted)}.")
        return index.replace(DATE_PLACEHOLDER, _date_formatted)

    @staticmethod
    def _validate_timezone(tz):
        if tz not in pytz.all_timezones:
            raise UserException(f"Incorrect timezone {tz} provided. Timezone must be a valid DB timezone name. "
                                "See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List.")
        return tz

    @staticmethod
    def _save_results(results: list, destination: str) -> None:
        full_path = os.path.join(destination, f"{uuid.uuid4()}.json")
        with open(full_path, "w") as json_file:
            json.dump(results, json_file, indent=4)

    def _create_and_start_ssh_tunnel(self, params) -> None:
        ssh_params = params.get(KEY_SSH)
        ssh_username = ssh_params.get(KEY_SSH_USERNAME)
        keys = ssh_params.get(KEY_SSH_KEYS)
        private_key = keys.get(KEY_SSH_PRIVATE_KEY)
        ssh_tunnel_host = ssh_params.get(KEY_SSH_TUNNEL_HOST)
        ssh_tunnel_port = ssh_params.get(KEY_SSH_TUNNEL_PORT, 22)
        db_params = params.get(KEY_GROUP_DB)
        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)
        self._create_ssh_tunnel(ssh_username, private_key, ssh_tunnel_host, ssh_tunnel_port,
                                db_hostname, db_port)

        try:
            self.ssh_server.start()
        except BaseSSHTunnelForwarderError as e:
            raise UserException(
                "Failed to establish SSH connection. Recheck all SSH configuration parameters") from e

        logging.info("SSH tunnel is enabled.")

    @staticmethod
    def is_valid_rsa(rsa_key) -> (bool, str):
        if not rsa_key.startswith(RSA_HEADER):
            return False, f"The RSA key does not start with the correct header: {RSA_HEADER}"
        if "\n" not in rsa_key:
            return False, "The RSA key does not contain any newline characters."
        return True, ""

    def _create_ssh_tunnel(self, ssh_username, private_key, ssh_tunnel_host, ssh_tunnel_port,
                           db_hostname, db_port) -> None:

        is_valid, error_message = self.is_valid_rsa(private_key)
        if is_valid:
            logging.info("SSH tunnel is enabled.")
        else:
            raise UserException(f"Invalid RSA key provided: {error_message}")

        try:
            private_key = get_private_key(private_key, None)
        except SomeSSHException as e:
            raise UserException(e) from e

        try:
            db_port = int(db_port)
        except ValueError as e:
            raise UserException("Remote port must be a valid integer") from e

        self.ssh_server = SSHTunnelForwarder(ssh_address_or_host=ssh_tunnel_host,
                                             ssh_port=ssh_tunnel_port,
                                             ssh_pkey=private_key,
                                             ssh_username=ssh_username,
                                             remote_bind_address=(db_hostname, db_port),
                                             local_bind_address=(LOCAL_BIND_ADDRESS, db_port),
                                             ssh_config_file=None,
                                             allow_agent=False)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
