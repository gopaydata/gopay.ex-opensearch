import json
import logging
# import csv
import time

import requests
from requests.auth import HTTPBasicAuth

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# from client.es_client import ElasticsearchClient
from client.ssh_utils import get_private_key
from sshtunnel import SSHTunnelForwarder

from requests.auth import AuthBase
from requests.models import Response

import traceback

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

    @staticmethod
    def retry_request(
            url: str,
            auth: AuthBase = None,
            retries: int = 5,
            initial_delay: float = 1,
            timeout: float = 10
    ) -> Response:
        """
        Retries an HTTP GET request with exponential backoff.

        Args:
            url (str): The target URL for the HTTP request.
            auth (AuthBase): Authentication details (default: None).
            retries (int): Maximum number of retry attempts (default: 5).
            initial_delay (float): Initial delay in seconds for backoff (default: 1).
            timeout (float): Timeout for the request in seconds (default: 10).

        Returns:
            Response: Successful HTTP response object.

        Raises:
            UserException: If all retries fail.
        """
        for attempt in range(1, retries + 1):
            try:
                logging.info(f"Attempt {attempt} of {retries}: Sending request to {url}")
                # Send the request
                response = requests.get(url, auth=auth, timeout=timeout)
                # Check if the response is valid
                if response is not None:
                    response.raise_for_status()
                    logging.info(f"Request successful on attempt {attempt}")
                    return response
                else:
                    logging.warning("Received no response (response is None)")
                    raise requests.exceptions.RequestException("Response object is None.")

            except requests.exceptions.Timeout:
                logging.error(f"Attempt {attempt}: Request timed out after {timeout} seconds.")

            except requests.exceptions.ConnectionError:
                logging.error(f"Attempt {attempt}: Failed to connect to the server.")

            except requests.exceptions.HTTPError as http_err:
                logging.error(
                    f"Attempt {attempt}: HTTP error occurred: {http_err} - Status code: "
                    f"{getattr(response, 'status_code', 'unknown')}")

            except requests.exceptions.RequestException as req_err:
                logging.error(f"Attempt {attempt}: General request exception occurred: {req_err}")

            except Exception as e:
                logging.error(f"Attempt {attempt}: An unexpected error occurred: {e}")

            # Exponential backoff
            if attempt < retries:
                delay = initial_delay * (2 ** (attempt - 1))
                logging.info(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logging.error("Max retries reached, aborting.")
                raise UserException("All retry attempts failed.")

    def test_opensearch(self, params):
        logging.info("Test_OpenSearch...")
        url = "https://os.gopay.com:443"

        auth_params = params.get(KEY_GROUP_AUTH, {})
        username = auth_params.get(KEY_API_KEY_ID)
        password = auth_params.get(KEY_API_KEY)

        logging.info(F"Username: {username}, Password: {password}")

        # Ověření SSH tunelu
        if hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
            logging.info("SSH tunnel is active.")
            local_host, local_port = self.ssh_tunnel.local_bind_address
        else:
            logging.exception("SSH tunnel is not active or not configured.")
            # raise UserException("SSH tunnel is not active or not configured.")


        # Sestavení URL
        url = f"https://{local_host}:{local_port}/app-logs-prod/_search"

        logging.info(F"Connecting to {url}")

        # Požadavek typu HEAD
        response = requests.head(url, auth=HTTPBasicAuth(username, password))
        logging.info(F"Response: {response.headers}")
        if response.status_code == 200:
            print("Connected successfully to the server.")
        elif response.status_code == 401:
            print("Unauthorized: Check your username and password.")
        else:
            print(f"Failed to connect: {response.status_code}")

        # Požadavek typu GET pro více informací
        response = requests.get(url, auth=HTTPBasicAuth(username, password))

        if response.status_code == 200:
            print("Response:", response.json())
        else:
            print(f"Failed to connect: {response.status_code}")

    def test_connection_directly(self, params):
        """Tests the connection directly via requests."""
        logging.info("Testing connection directly via requests.")
        try:
            auth_params = params.get(KEY_GROUP_AUTH, {})
            username = auth_params.get(KEY_API_KEY_ID)
            password = auth_params.get(KEY_API_KEY)

            # Ověření SSH tunelu
            if hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
                local_host, local_port = self.ssh_tunnel.local_bind_address
            else:
                raise UserException("SSH tunnel is not active or not configured.")

            # Sestavení URL
            url = f"https://{local_host}:{local_port}/app-logs-prod/_search"
            logging.info(f"Testing direct connection to {url} with username {username}.")

            # Autentizace
            auth = HTTPBasicAuth(username, password)

            # Volání retry_request
            response = self.retry_request(url, auth)
            logging.info(f"Response code: {response.status_code}")
            logging.info(f"Response body: {response.json()}")

        except Exception as e:
            logging.error(f"Direct connection test failed: {e}")
            logging.error(f"Exception details: {traceback.format_exc()}")
            raise UserException("Direct connection test failed.")

    def test_ssh_tunnel(self):
        """Tests the SSH tunnel by sending a request through it."""
        if not self.ssh_tunnel.is_active:
            raise UserException("SSH tunnel is not active.")
        local_host, local_port = self.ssh_tunnel.local_bind_address
        logging.info(f"Testing SSH tunnel: Local bind address is {local_host}:{local_port}")
        try:
            url = f"https://{local_host}:{local_port}/app-logs-prod/_search"
            response = requests.post(url, timeout=5)
            logging.info(f"SSH tunnel test response: {response.status_code} - {response.text}")
        except Exception as e:
            logging.error(f"SSH tunnel test failed: {e}")
            raise UserException("Failed to communicate through SSH tunnel.")

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

    @staticmethod
    def export_debug_info(params, response=None):
        """Exports debug information to a file."""
        with open("debug_info.json", "w") as debug_file:
            debug_data = {
                "params": params,
                "response": response.text if response else None,
                "status_code": response.status_code if response else None,
            }
            json.dump(debug_data, debug_file, indent=2)
        logging.info("Debug info exported to debug_info.json.")

    def run(self):
        """Main execution logic for the component."""
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters
        self.validate_params(params)

        # self.test_opensearch(params)

        ssh_tunnel_started = False
        try:
            logging.info("Starting component execution...")

            # Set up SSH tunnel if enabled
            if params.get(KEY_SSH, {}).get(KEY_USE_SSH, False):
                logging.info("SSH tunneling is enabled. Setting up...")
                self._create_and_start_ssh_tunnel(params)
                ssh_tunnel_started = True

            # Test connection directly
            self.test_opensearch(params)
            # self.test_connection_directly(params)

            # Optional: Test root endpoint only if explicitly enabled
            # if params.get("test_root_endpoint", False):
            #     self.test_root_endpoint(params)

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
