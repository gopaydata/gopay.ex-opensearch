import json
import logging
import uuid
import os
import shutil
import dateparser
import pytz
import csv

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

    def log_available_indices(self, params: dict, save_to_csv: str = None):
        """
        Loguje seznam dostupných indexů, ověřuje připojení k Elasticsearch
        a volitelně ukládá indexy do CSV souboru.

        :param params: Konfigurační parametry pro připojení k Elasticsearch.
        :param save_to_csv: Cesta k CSV souboru, kam se indexy uloží (volitelné).
        """
        try:
            # Inicializace klienta
            logging.info("Initializing Elasticsearch client...")
            client = self.get_client(params)

            # Test připojení k serveru
            logging.info("Testing connection to Elasticsearch...")
            if client.ping():
                logging.info("Connection to Elasticsearch successful. Server is reachable.")
            else:
                logging.error("Connection to Elasticsearch failed. Server might be unreachable.")
                raise UserException("Ping to Elasticsearch server failed.")

            # Získání seznamu indexů
            logging.info("Fetching available indices...")
            indices = client.indices.get_alias("*")  # Získání aliasů pro všechny indexy
            index_names = list(indices.keys())

            if not index_names:
                logging.warning("No indices found in Elasticsearch.")
            else:
                logging.info(f"Available indices: {index_names}")

            # Uložení seznamu indexů do CSV souboru (volitelné)
            if save_to_csv:
                logging.info(f"Saving indices to CSV file: {save_to_csv}")
                with open(save_to_csv, mode='w', newline='', encoding='utf-8') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(["Index Name"])
                    for index_name in index_names:
                        writer.writerow([index_name])
                logging.info(f"Index list successfully saved to {save_to_csv}")

        except UserException as ue:
            logging.error(f"UserException: {ue}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while fetching indices: {e}")
            raise UserException(f"Failed to retrieve indices due to an unexpected error: {str(e)}")

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Inicializace proměnných
        temp_folder = os.path.join(self.data_folder_path, "temp")
        out_table_name = params.get(KEY_STORAGE_TABLE, "ex-elasticsearch-result")
        ssh_tunnel_started = False

        # Legacy SSH klient
        if params.get(KEY_LEGACY_SSH):
            logging.info("Running legacy SSH client...")
            self.run_legacy_client()
            return

        try:
            # Nastavení SSH tunelu, pokud je povoleno
            ssh_options = params.get(KEY_SSH)
            if not isinstance(ssh_options, list) and ssh_options.get(KEY_USE_SSH, False):
                logging.info("Setting up SSH tunnel...")
                self._create_and_start_ssh_tunnel(params)
                ssh_tunnel_started = True
                logging.info("SSH tunnel started successfully.")

            # Ověření připojení k Elasticsearch
            self.log_available_indices(params, save_to_csv="available_indices.csv")
            logging.info("Elasticsearch connection and indices verification successful.")

            # Příprava výstupní tabulky
            logging.info(f"Preparing output table: {out_table_name}")
            user_defined_pk = params.get(KEY_PRIMARY_KEYS, [])
            incremental = params.get(KEY_INCREMENTAL, False)
            index_name, query = self.parse_index_parameters(params)
            statefile = self.get_state_file()

            os.makedirs(temp_folder, exist_ok=True)
            columns = statefile.get(out_table_name, [])
            out_table = self.create_out_table_definition(
                out_table_name, primary_key=user_defined_pk, incremental=incremental
            )

            # Extrakce dat z Elasticsearch
            logging.info(f"Starting data extraction from index: {index_name}")
            client = self.get_client(params)

            with ElasticDictWriter(out_table.full_path, columns) as wr:
                for result in client.extract_data(index_name, query):
                    wr.writerow(result)
                wr.writeheader()
            logging.info(f"Data successfully extracted to: {out_table.full_path}")

            # Zápis manifestu a stavového souboru
            self.write_manifest(out_table)
            statefile[out_table_name] = wr.fieldnames
            self.write_state_file(statefile)
            logging.info("Manifest and state file successfully written.")

        except UserException as ue:
            logging.error(f"User error: {ue}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during execution: {e}")
            raise
        finally:
            # Ukončení SSH tunelu, pokud byl spuštěn
            if ssh_tunnel_started and hasattr(self, 'ssh_tunnel') and self.ssh_tunnel.is_active:
                logging.info("Stopping SSH tunnel...")
                self.ssh_tunnel.stop()
                logging.info("SSH tunnel stopped successfully.")

            # Úklid dočasných souborů
            if os.path.exists(temp_folder):
                logging.info(f"Cleaning up temporary folder: {temp_folder}")
                self.cleanup(temp_folder)
                logging.info("Temporary folder cleaned up successfully.")

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

    def _create_and_start_ssh_tunnel(self, params):
        ssh_params = params.get(KEY_SSH)
        ssh_username = ssh_params.get(KEY_SSH_USERNAME)
        keys = ssh_params.get(KEY_SSH_KEYS)
        private_key = keys.get(KEY_SSH_PRIVATE_KEY)
        ssh_tunnel_host = ssh_params.get(KEY_SSH_TUNNEL_HOST)
        ssh_tunnel_port = ssh_params.get(KEY_SSH_TUNNEL_PORT, 22)
        db_params = params.get(KEY_GROUP_DB)
        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)

        # Vytvoření SSH tunelu
        self._create_ssh_tunnel(ssh_username, private_key, ssh_tunnel_host, ssh_tunnel_port,
                                db_hostname, db_port)

        try:
            self.ssh_server.start()
            logging.info(
                f"SSH tunnel is enabled: {self.ssh_server.local_bind_address} -> {self.ssh_server.remote_bind_address}")
        except BaseSSHTunnelForwarderError as e:
            logging.error("Failed to establish SSH tunnel. Recheck SSH configuration.")
            raise UserException(e) from e

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
