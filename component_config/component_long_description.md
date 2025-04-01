
# OpenSearch Extractor

The OpenSearch Extractor is a Keboola component that allows you to extract logs from OpenSearch indexes into Keboola Storage with minimal setup.

## Key Features
- Supports both full and incremental extraction.
- Automatically tracks the last processed record using `last_item.csv`.
- Transforms, renames, and cleans selected columns.
- Supports OpenSearch connections with optional SSH tunneling.
- Outputs two files: extracted data (`os_output.csv`) and state file (`last_item.csv`).

## Incremental Extraction
The component automatically continues from where it left off using the `last_item.csv` file. It tracks both the timestamp and the ID of the last successfully processed log.

If you want to start fresh, simply delete or reset the `last_item.csv`.

## Output Files
1. `os_output.csv` — Main dataset with transformed log data.
2. `last_item.csv` — Stores the ID and timestamp of the last processed record for incremental loads.
