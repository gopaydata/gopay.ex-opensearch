import logging
import pandas as pd
from opensearchpy import OpenSearch

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client.ssh_utils import get_private_key
from sshtunnel import SSHTunnelForwarder

# configuration variables
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

    def get_data_client(self, params):

        # Data input
        input_csv = "../data/in/tables/input.csv"
        payment_data = pd.read_csv(input_csv)

        payment_ids = payment_data['payment_session_id'].dropna().astype(str).tolist()

        auth_params = params.get(KEY_GROUP_AUTH, {})
        username = auth_params.get(KEY_API_KEY_ID)
        password = auth_params.get(KEY_API_KEY)

        local_host = 'os.gopay.com'
        local_port = '443'

        # SSH tunel test
        if hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
            logging.info("OK - Tunnel is active.")
            logging.info(self.ssh_tunnel.is_active)
            local_host, local_port = self.ssh_tunnel.local_bind_address
        else:
            logging.warning("SSH tunnel is not active or not configured.")

        client = OpenSearch(
            hosts=[{"host": local_host, "port": local_port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )

        all_results = []

        for pid in payment_ids:
            query = {
                "query": {
                    "query_string": {
                        "query": pid
                    }
                },
                "size": 1000
            }

            try:
                response = client.search(
                    body=query,
                    index="app-logs-prod"
                )
                hits = response.get("hits", {}).get("hits", [])
                all_results.extend(hits)

            except Exception as e:
                print(f"Data extraction failed for ID {pid}: {e}")

        if all_results:
            df = pd.DataFrame(all_results)

            def parse_json(source):
                return source if isinstance(source, dict) else {}

            df['_source'] = df['_source'].apply(parse_json)

            def assign_payment_id(row):
                for payment_id in payment_ids:
                    if payment_id in str(row['_source']):
                        return payment_id
                return None

            df['payment_session_id'] = df.apply(assign_payment_id, axis=1)

            source_expanded = pd.json_normalize(df['_source'])

            expanded_data = pd.concat([df[['_id', '_index', 'payment_session_id']], source_expanded], axis=1)

            # CSV Output file
            csv_file = self.create_out_table_definition("payment_logs.csv")
            out_table_path = csv_file.full_path
            expanded_data.to_csv(out_table_path, index=False)
            print(f"Data saved to file: {out_table_path}")
        else:
            print("No data extracted.")

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
        logging.getLogger("opensearch").setLevel(logging.WARNING)

        ssh_tunnel_started = False
        try:
            logging.info("Starting component execution...")

            # Set up SSH tunnel if enabled
            if params.get(KEY_SSH, {}).get(KEY_USE_SSH, False):
                logging.info("SSH tunneling is enabled. Setting up...")
                self._create_and_start_ssh_tunnel(params)
                ssh_tunnel_started = True

            try:
                self.get_data_client(params)
            except Exception as e:
                logging.error(f"Data extraction fail: {e}")
            finally:
                logging.info("Data extraction finished.")

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
