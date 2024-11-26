# import json
import logging
# import uuid
import os
import shutil
# import dateparser
# import pytz
import csv

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
# from keboola.csvwriter import ElasticDictWriter

from client.es_client import ElasticsearchClient
# from legacy_client.legacy_es_client import LegacyClient
from client.ssh_utils import get_private_key
from sshtunnel import SSHTunnelForwarder

# Configuration constants
KEY_GROUP_DB = 'db'
KEY_DB_HOSTNAME = 'hostname'
KEY_DB_PORT = 'port'
KEY_QUERY = 'request_body'
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

    def test_root_endpoint(self, params: dict):
        """
        Ověří, že komponenta dokáže získat stejnou odpověď jako prohlížeč
        z root endpointu OpenSearch serveru.
        """
        try:
            logging.info("Testing root endpoint of OpenSearch server...")
            client = self.get_client(params)
            response = client.perform_request('GET', '/')
            logging.info(f"Root endpoint response: {json.dumps(response, indent=2)}")
            return response
        except Exception as e:
            logging.error(f"Error testing root endpoint: {e}")
            raise UserException(f"Failed to fetch root endpoint response: {e}")

    def log_available_indices(self, params: dict, save_to_csv: str = None):
        """Logs the list of available indices."""
        try:
            logging.info("Initializing Elasticsearch client...")
            client = self.get_client(params)

            # Získání seznamu indexů přímo, bez ping()
            try:
                logging.info("Fetching list of available indices directly without ping...")
                indices = client.indices.get_alias("*")
                index_names = list(indices.keys())
                if not index_names:
                    logging.warning("No indices found in Elasticsearch.")
                else:
                    logging.info(f"Available indices: {index_names}")
            except Exception as e:
                logging.error(f"Error while fetching indices: {e}")
                raise UserException(f"Failed to fetch indices: {e}")

            # Uložení seznamu indexů do CSV souboru (volitelné)
            if save_to_csv:
                logging.info(f"Saving indices to CSV file: {save_to_csv}")
                with open(save_to_csv, mode='w', newline='', encoding='utf-8') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(["Index Name"])
                    for index_name in index_names:
                        writer.writerow([index_name])
                logging.info(f"Indices successfully saved to {save_to_csv}")

        except Exception as e:
            logging.error(f"Error while fetching indices: {e}")
            raise UserException(f"Failed to fetch indices: {e}")

    def get_client(self, params: dict) -> ElasticsearchClient:
        """Creates and returns an Elasticsearch client."""
        try:
            logging.info("Preparing to initialize Elasticsearch client...")
            auth_params = params.get(KEY_GROUP_AUTH, {})
            db_params = params.get(KEY_GROUP_DB, {})
            db_hostname = db_params.get(KEY_DB_HOSTNAME)
            db_port = db_params.get(KEY_DB_PORT)
            scheme = params.get(KEY_SCHEME, "http")

            auth_type = auth_params.get(KEY_AUTH_TYPE, "no_auth")
            setup = {"host": db_hostname, "port": db_port, "scheme": scheme}
            logging.info(f"Elasticsearch setup: {setup} with auth_type: {auth_type}")

            # Přidání logování endpointu
            logging.info(f"Using endpoint: {scheme}://{db_hostname}:{db_port}")

            if auth_type == "basic":
                username = auth_params.get(KEY_USERNAME)
                password = auth_params.get(KEY_PASSWORD)
                if not username or not password:
                    raise UserException("Both username and password must be provided for basic auth.")
                client = ElasticsearchClient([setup], scheme, http_auth=(username, password))
                logging.info("Using basic authentication for Elasticsearch.")

            elif auth_type == "api_key":
                api_key_id = auth_params.get(KEY_API_KEY_ID)
                api_key = auth_params.get(KEY_API_KEY)
                if not api_key_id or not api_key:
                    raise UserException("API Key ID and API Key must be provided for API Key authentication.")
                client = ElasticsearchClient([setup], scheme, api_key=(api_key_id, api_key))
                logging.info("Using API Key authentication for Elasticsearch.")

            elif auth_type == "no_auth":
                client = ElasticsearchClient([setup], scheme)
                logging.info("Using no authentication for Elasticsearch.")

            else:
                raise UserException(f"Unsupported auth_type: {auth_type}")

            # Test connection
            logging.info("Testing Elasticsearch client connection with ping...")
            if not client.ping():
                raise UserException(f"Connection to Elasticsearch at {db_hostname}:{db_port} failed.")
            logging.info("Elasticsearch client initialized successfully.")
            return client

        except Exception as e:
            logging.error(f"Error during Elasticsearch client initialization: {e}")
            raise UserException(f"Failed to initialize Elasticsearch client: {e}")

    def run(self):
        """Main execution logic for the component."""
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        ssh_tunnel_started = False
        try:
            logging.info("Starting component execution...")

            # Nastavení SSH tunelu, pokud je povoleno
            if params.get(KEY_SSH, {}).get(KEY_USE_SSH, False):
                logging.info("SSH tunneling is enabled. Setting up...")
                self._create_and_start_ssh_tunnel(params)
                ssh_tunnel_started = True

            # Test root endpoint
            root_response = self.test_root_endpoint(params)
            logging.info("Root endpoint test passed.")

            # Ověření indexů
            self.log_available_indices(params, save_to_csv="available_indices.csv")
            logging.info("Elasticsearch indices verification completed.")

            # Další operace (placeholder pro hlavní logiku)
            logging.info("Execution completed successfully.")

        except Exception as e:
            logging.error(f"Unexpected error during component execution: {e}")
            raise
        finally:
            if ssh_tunnel_started and hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
                logging.info("Stopping SSH tunnel...")
                self.ssh_tunnel.stop()
                logging.info("SSH tunnel stopped.")

    def _create_and_start_ssh_tunnel(self, params):
        """Sets up and starts the SSH tunnel."""
        try:
            logging.info("Validating SSH parameters...")
            ssh_params = params.get(KEY_SSH)
            ssh_username = ssh_params.get(KEY_SSH_USERNAME)
            private_key = ssh_params.get(KEY_SSH_KEYS, {}).get(KEY_SSH_PRIVATE_KEY)
            ssh_tunnel_host = ssh_params.get(KEY_SSH_TUNNEL_HOST)
            ssh_tunnel_port = ssh_params.get(KEY_SSH_TUNNEL_PORT, 22)
            db_params = params.get(KEY_GROUP_DB)
            db_hostname = db_params.get(KEY_DB_HOSTNAME)
            db_port = int(db_params.get(KEY_DB_PORT))

            # Validate private key
            is_valid, error_message = self.is_valid_rsa(private_key)
            if not is_valid:
                logging.error(f"Invalid RSA key: {error_message}")
                raise UserException(f"Invalid RSA key: {error_message}")

            logging.info(f"Setting up SSH tunnel to {ssh_tunnel_host}:{ssh_tunnel_port}...")
            self.ssh_tunnel = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_tunnel_host, ssh_tunnel_port),
                ssh_username=ssh_username,
                ssh_pkey=get_private_key(private_key, None),
                remote_bind_address=(db_hostname, db_port),
                local_bind_address=(LOCAL_BIND_ADDRESS, 0)  # Auto-assigned local port
            )
            self.ssh_tunnel.start()
            logging.info(f"SSH tunnel established: {self.ssh_tunnel.local_bind_address} -> {db_hostname}:{db_port}")

        except Exception as e:
            logging.error(f"Failed to start SSH tunnel: {e}")
            raise UserException(f"Failed to establish SSH tunnel: {e}")

    @staticmethod
    def is_valid_rsa(rsa_key):
        """Validates the RSA private key."""
        if not rsa_key.startswith(RSA_HEADER):
            return False, "RSA key does not start with the correct header."
        if "\n" not in rsa_key:
            return False, "RSA key does not contain newline characters."
        return True, ""


# Main entrypoint
if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
