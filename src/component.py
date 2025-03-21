import logging
import gc
import os
import psutil
import pandas as pd
from opensearchpy import OpenSearch
import pytz
import time
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from datetime import datetime, timedelta
from client.ssh_utils import get_private_key
from sshtunnel import SSHTunnelForwarder

import re

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

KEY_DATE = "date"
KEY_HOUR = 'hours'

LOCAL_BIND_ADDRESS = "127.0.0.1"

KEY_LEGACY_SSH = 'ssh'

REQUIRED_PARAMETERS = [KEY_GROUP_DB]

RSA_HEADER = "-----BEGIN RSA PRIVATE KEY-----"

# Required columns
REQUIRED_COLUMNS = [
    "event.action",  "@timestamp", "labels.system_log_severity", "source.ip", "user_agent.original",
    "user_id", "labels.relevant_domain", "labels.relevant_domain_id", "is_processed",  "result", "problem_detail",
    "beat.hostname", "es_index",  "host.env", "host.name", "labels.source_class_name", "log.file.path", "log.level",
    "log.logger", "message", "process.thread.name", "service.environment", "service.name", "service.node.name",
    "service.type",  "user_agent.os.full", "user_agent.os.name",
]


class Component(ComponentBase):

    @staticmethod
    def log_memory_usage(stage=""):
        process = psutil.Process()
        mem_info = process.memory_info()
        logging.info(
            f"[MEMORY] {stage} - RSS: {mem_info.rss / (1024 * 1024):.2f} MB, "
            f"VMS: {mem_info.vms / (1024 * 1024):.2f} MB")

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
    def extract_user_id(message):
        if not isinstance(message, str):
            return None
        match = re.search(r"U:(\d+)", message)
        return match.group(1) if match else None

    @staticmethod
    def extract_problem_detail(message):
        if not isinstance(message, str):
            return None

        message = message.replace("\n", " ")

        match = re.search(r"R\[.*?\]\[(.*?)\](?=,?\s*IP\[)", message, re.DOTALL)

        if match:
            problem_detail = match.group(1)

            if "-eshop" in problem_detail:
                parts = problem_detail.split("-", 2)
                if len(parts) > 2:
                    problem_detail = f"{parts[0]}-{parts[1]}{{{parts[2]}}}"
            else:
                problem_detail = problem_detail.replace("-", "{", 1)

            # problem_detail = problem_detail.replace("[", "{").replace("]", "}")

            return f"{problem_detail[:-1]}}}"

        return None

    @staticmethod
    def extract_result(message):
        if not isinstance(message, str):
            return None
        match = re.search(r"R\[(true|false)", message, re.IGNORECASE)
        return "1" if match and match.group(1).lower() == "true" else None

    def get_data_client(self, params):
        """ Fetches data from OpenSearch based on the given parameters. """
        start_time = time.time()
        logging.info("Fetching data from OpenSearch...")

        # Memory check
        def get_memory_usage():
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            return memory_info.rss / 1024 / 1024

        max_memory_usage = 0
        memory_measurements = []
        memory_points = []

        # Memory refresh and report
        def log_memory_usage(step_name=None):
            nonlocal max_memory_usage
            current_memory = get_memory_usage()
            memory_measurements.append(current_memory)
            if step_name:
                memory_points.append((step_name, current_memory))
            max_memory_usage = max(max_memory_usage, current_memory)
            return current_memory

        log_memory_usage("initialization")

        # Authentication parameters
        auth_params = params.get(KEY_GROUP_AUTH, {})
        username = auth_params.get(KEY_API_KEY_ID)
        password = auth_params.get(KEY_API_KEY)

        # Extract parameters
        param_date = params.get(KEY_DATE, '2025-01-01')  # Default date if not provided
        param_hours = int(params.get(KEY_HOUR, 1))  # Default to 1 hour increment
        index_name = params.get(KEY_INDEX_NAME)
        batch_size = int(params.get("batch_size", 500))  # Default batch size

        print(f"Increment {param_hours} hour(s)")
        print(f"Index name: {index_name}")
        print(f"Batch size: {batch_size}")

        # OpenSearch connection settings
        local_host = 'os.gopay.com'
        local_port = '443'

        # Check if an SSH tunnel is active
        if hasattr(self, "ssh_tunnel") and self.ssh_tunnel.is_active:
            logging.info("OK - Tunnel is active.")
            local_host, local_port = self.ssh_tunnel.local_bind_address
        else:
            logging.warning("SSH tunnel is not active or not configured.")

        # OpenSearch client initialization
        client = OpenSearch(
            hosts=[{"host": local_host, "port": local_port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=30
        )
        log_memory_usage("after_client_init")

        # File paths for output and last processed record tracking
        csv_file = self.create_out_table_definition("os_output.csv")
        out_table_path = csv_file.full_path
        last_item_path = os.path.join(os.path.dirname(out_table_path), "last_item.csv")
        in_table_path = "../data/in/tables/last_item.csv"

        logging.info(f"{in_table_path} exists: {os.path.exists(in_table_path)}")

        last_id = None
        last_timestamp = None

        # Load last processed timestamp and ID
        if os.path.exists(in_table_path):
            try:
                last_item_df = pd.read_csv(in_table_path, dtype=str)
                if not last_item_df.empty:
                    last_id = last_item_df.iloc[0]["id"]
                    last_timestamp = last_item_df.iloc[0]["timestamp_cz"]
                    print(f"Continuing from last timestamp: {last_timestamp} (ID: {last_id})")
            except Exception as e:
                print(f"Error reading last_item.csv: {e}")

        # If no timestamp is stored, start from the provided date
        if not last_timestamp:
            last_timestamp = f"{param_date}T00:00:00"

        # Convert timestamp from Prague timezone to UTC
        prague_tz = pytz.timezone("Europe/Prague")
        last_timestamp_dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S")
        last_timestamp_dt = prague_tz.localize(last_timestamp_dt)
        last_timestamp_utc = last_timestamp_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")

        # Set upper time limit based on incremented hours
        upper_timestamp_dt = (last_timestamp_dt + timedelta(hours=param_hours)).replace(second=0, microsecond=0)
        upper_timestamp_utc = upper_timestamp_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")
        log_memory_usage("after_time_setup")

        # OpenSearch query to fetch data within the given time range
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gt": last_timestamp_utc,  # Use UTC timestamp
                        "lte": upper_timestamp_utc,  # Upper time limit in UTC
                        "format": "yyyy-MM-dd'T'HH:mm:ss"
                    }
                }
            },
            "size": batch_size,
            "sort": [
                {"@timestamp": "asc"}
            ]
        }

        scroll_time = "10m"
        file_exists = os.path.exists(out_table_path)
        batch_counter = 0
        total_saved = 0

        try:
            # Initial OpenSearch request
            response = client.search(
                body=query,
                index=index_name,
                scroll=scroll_time
            )
            scroll_id = response.get("_scroll_id")
            hits = response.get("hits", {}).get("hits", [])
            log_memory_usage("after_initial_search")

            while hits:
                batch_counter += 1
                df = pd.DataFrame(hits)
                log_memory_usage(f"batch_{batch_counter}_after_creating_df")

                if not df.empty:

                    df['_source'] = df['_source'].apply(lambda x: x if isinstance(x, dict) else {})
                    source_expanded = pd.json_normalize(df['_source'])
                    log_memory_usage(f"batch_{batch_counter}_after_json_normalize")

                    source_expanded['_index'] = df['_index']

                    # Extract additional fields
                    if 'message' in source_expanded.columns:
                        source_expanded['user_id'] = source_expanded['message'].apply(self.extract_user_id)
                        source_expanded['problem_detail'] = (source_expanded['message'].
                                                             apply(self.extract_problem_detail))
                        source_expanded['result'] = source_expanded['message'].apply(self.extract_result)
                        source_expanded['is_processed'] = ''

                    if 'REQUIRED_COLUMNS' in globals():
                        selected_columns = [col for col in REQUIRED_COLUMNS if col in source_expanded.columns]
                        filtered_data = source_expanded[selected_columns] if selected_columns else source_expanded
                    else:
                        filtered_data = source_expanded

                    log_memory_usage(f"batch_{batch_counter}_after_filtering_columns")

                    filtered_data.insert(0, '_id', df['_id'])

                    if "@timestamp" in filtered_data.columns:
                        filtered_data.loc[:, "@timestamp"] = (
                            pd.to_datetime(filtered_data["@timestamp"], utc=True)
                            .dt.tz_convert(prague_tz)
                            .dt.floor("ms")
                            .dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                            .str[:-3]
                        )

                    if 'labels.relevant_domain_id' in filtered_data.columns:
                        filtered_data = filtered_data.astype({'labels.relevant_domain_id': 'string'})
                        filtered_data.loc[:, 'labels.relevant_domain_id'] = (
                            filtered_data['labels.relevant_domain_id']
                            .fillna('')
                            .astype(str)
                            .str.replace(r'\.0$', '', regex=True)
                        )
                    log_memory_usage(f"batch_{batch_counter}_after_data_processing")

                    filtered_data = filtered_data.rename(columns=lambda x: x.lstrip("@_").replace(".", "_"))
                    filtered_data = filtered_data.rename(columns={
                                                             "id": "system_log_id",
                                                             "event_action": "system_log_type",
                                                             "timestamp": "date_performed",
                                                             "labels_system_log_severity": "severity",
                                                             "user_agent_original": "user_agent",
                                                             "labels_relevant_domain": "relevant_domain",
                                                             "labels_relevant_domain_id": "relevant_domain_id"
                                                         }
                                                         )

                    log_memory_usage(f"batch_{batch_counter}_after_column_rename")

                    filtered_data.to_csv(out_table_path, index=False, mode='a', header=not file_exists, chunksize=1000)
                    file_exists = True
                    total_saved += len(filtered_data)
                    log_memory_usage(f"batch_{batch_counter}_after_csv_save")

                    last_id = str(filtered_data['system_log_id'].iloc[-1])
                    last_timestamp = df["_source"].iloc[-1]["@timestamp"] if "@timestamp" in df["_source"].iloc[
                        -1] else None

                    del df, filtered_data, source_expanded
                    gc.collect()
                    log_memory_usage(f"batch_{batch_counter}_after_garbage_collection")

                if batch_counter % 100 == 0 or batch_counter == 1:
                    print(f"Saved {total_saved:,} rows to file {out_table_path}".replace(",", " "))

                # Fetch next batch of data
                response = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                hits = response.get("hits", {}).get("hits", [])
                if hits:
                    log_memory_usage(f"batch_{batch_counter}_after_fetching_next_batch")

                # Last item detail save to last_item.csv
                if last_id and last_timestamp:
                    last_timestamp_dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                        tzinfo=pytz.utc)
                    last_timestamp_cz = last_timestamp_dt.astimezone(prague_tz).strftime("%Y-%m-%dT%H:%M:%S")

                    last_item_df = pd.DataFrame(
                        [{"id": last_id, "timestamp": last_timestamp, "timestamp_cz": last_timestamp_cz}]
                    )
                    last_item_df.to_csv(last_item_path, index=False)

            print(f"Finished processing. Total records saved: {total_saved:,}".replace(",", " "))

        except Exception as e:
            print(f"Data extraction failed: {e}")

        finally:
            end_time = time.time()
            elapsed_time = end_time - start_time
            log_memory_usage("final")

            # Memory statistics
            if memory_measurements:
                avg_memory = sum(memory_measurements) / len(memory_measurements)
                print("\nOVERALL MEMORY STATISTICS:")
                print(f"Data extraction completed in {elapsed_time:.2f} seconds")
                print(f"Initial memory: {memory_measurements[0]:.2f} MB")
                print(f"Final memory: {memory_measurements[-1]:.2f} MB")
                print(f"Maximum memory: {max_memory_usage:.2f} MB")
                print(f"Average memory: {avg_memory:.2f} MB")

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
