# import json
import logging
import csv
# import time

import requests
from requests.auth import HTTPBasicAuth

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# from client.es_client import ElasticsearchClient
from client.ssh_utils import get_private_key
from sshtunnel import SSHTunnelForwarder

# from requests.auth import AuthBase
# from requests.models import Response

# import traceback

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

KEY_SSH = "ssh_options"
KEY_USE_SSH = "enabled"
KEY_SSH_KEYS = "keys"
KEY_SSH_PRIVATE_KEY = "#private"
KEY_SSH_USERNAME = "user"
KEY_SSH_TUNNEL_HOST = "sshHost"
KEY_SSH_TUNNEL_PORT = "sshPort"

LOCAL_BIND_ADDRESS = "127.0.0.1"
REQUIRED_PARAMETERS = [KEY_GROUP_DB]
RSA_HEADER = "-----BEGIN RSA PRIVATE KEY-----"


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    @staticmethod
    def validate_params(params):
        """Validates the configuration parameters."""
        logging.info("Validating configuration parameters...")
        if KEY_GROUP_AUTH not in params:
            raise UserException(f"Missing {KEY_GROUP_AUTH} in parameters.")
        auth_params = params[KEY_GROUP_AUTH]
        if not auth_params.get(KEY_API_KEY_ID) or not auth_params.get(KEY_API_KEY):
            raise UserException("Missing API Key ID or API Key in authentication parameters.")
        if KEY_GROUP_DB not in params:
            raise UserException(f"Missing {KEY_GROUP_DB} in parameters.")
        db_params = params[KEY_GROUP_DB]
        if not db_params.get(KEY_DB_HOSTNAME) or not db_params.get(KEY_DB_PORT):
            raise UserException("Missing database hostname or port.")
        logging.info("Validation successful.")

    def test_health(self, params):

        logging.info("OS health testing...")

        if hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
            logging.info("OK - Tunnel is active.")
            logging.info(self.ssh_tunnel.is_active)
            local_host, local_port = self.ssh_tunnel.local_bind_address
        else:
            local_host = 'os.gopay.com'
            local_port = '443'
            logging.warning("SSH tunnel is not active or not configured.")

        # Sestavení URL
        url = f"https://{local_host}:{local_port}/_cluster/health"

        auth_params = params.get(KEY_GROUP_AUTH, {})
        username = auth_params.get(KEY_API_KEY_ID)
        password = auth_params.get(KEY_API_KEY)

        logging.info("Connecting to " + url)
        logging.info("Username: " + username)

        response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=100)
        logging.info("Response code:" + str(response.status_code))

        if response.status_code == 200:
            logging.info("Connected successfully to the server.")
        elif response.status_code == 401:
            logging.info("Unauthorized: Check your username and password.")
        else:
            logging.info(f"Failed to connect: {response.status_code}")

        # Požadavek typu GET pro více informací
        logging.info("GET request, url: " + url)
        response = requests.get(url, auth=HTTPBasicAuth(username, password))

        if response.status_code == 200:
            logging.info("Response: " + str(response.json()))

            # Zpracování odpovědi jako CSV
            response_data = response.json()
            csv_file = self.create_out_table_definition("cluster_health.csv")
            out_table_path = csv_file.full_path

            logging.info(out_table_path)

            try:
                # Uložení JSON dat jako CSV
                with open(out_table_path, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    # Zápis hlaviček (klíčů JSON odpovědi)
                    writer.writerow(response_data.keys())
                    # Zápis hodnot (hodnot JSON odpovědi)
                    writer.writerow(response_data.values())

                print(f"Data byla uložena do souboru {out_table_path}.")
            finally:
                logging.info("Po pokusu o uložení souboru.")
        else:
            print(f"Failed to connect: {response.status_code}")

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

    def run(self):
        """Main execution logic for the component."""
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters
        self.validate_params(params)

        ssh_tunnel_started = False
        try:
            logging.info("Starting component execution...")

            # Set up SSH tunnel if enabled
            if params.get(KEY_SSH, {}).get(KEY_USE_SSH, False):
                logging.info("SSH tunneling is enabled. Setting up...")
                self._create_and_start_ssh_tunnel(params)
                ssh_tunnel_started = True

            # Test
            try:
                self.test_health(params)
            except Exception as e:
                logging.error(f"Test selhal: {e}")
            finally:
                logging.info("Konec testu.")

        except Exception as e:
            logging.error(f"Unexpected error during component execution: {type(e).__name__} - {str(e)}")
            raise
        finally:
            if ssh_tunnel_started and hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
                logging.info("Stopping SSH tunnel...")
                self.ssh_tunnel.stop()
                logging.info("SSH tunnel stopped.")


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
