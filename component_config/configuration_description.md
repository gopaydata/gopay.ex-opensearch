
## Configuration Overview

### Connection Settings

#### Database (`db`)
- `hostname` — OpenSearch host.
- `port` — OpenSearch port.

#### Authentication (`authentication`)
- `api_key_id`
- `api_key`

#### SSH (`ssh_options`) *(optional)*
- `enabled`
- `sshHost`
- `sshPort`
- `user`
- `keys`

### Extraction Parameters

- `index_name` — OpenSearch index to extract from.
- `batch_size` — Number of logs per batch.
- `date` — Starting date if `last_item.csv` is missing.
- `hours` — Defines the extraction window in hours.

## Required Input

Upload an initial `/data/in/tables/last_item.csv`:

```csv
id,timestamp,timestamp_cz
,,
```

This file is used to track the extraction state.

## Output

### os_output.csv
Extracted logs with transformed columns.

### last_item.csv
State file used for incremental extraction.
