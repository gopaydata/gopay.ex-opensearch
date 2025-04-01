
# OpenSearch Extractor for Keboola

This component extracts logs from OpenSearch indexes and loads them into Keboola Storage. It supports incremental extraction using the `last_item.csv` state file and provides full control over connection and extraction parameters.

---

## ✨ Features

- ✅ Incremental extraction based on last processed timestamp and ID
- ✅ SSH tunneling support
- ✅ Automatic timezone handling (UTC → Europe/Prague)
- ✅ Selected columns transformation and renaming
- ✅ Generates two output tables (`os_output.csv` and `last_item.csv`)
- ✅ Designed for Keboola Connection (KBC)

---

## 📊 Data Flow

```text
┌────────────────────────┐
│  last_item.csv (input) │
└─────────┬──────────────┘
          │ (read)
┌─────────▼──────────────┐
│    OpenSearch Query    │
│  (incremental window)  │
└─────────┬──────────────┘
          │ (extract)
┌─────────▼──────────────┐
│   Data Transformation  │
│ (select + rename cols) │
└──────┬──────┬──────────┘
       │      │
┌──────▼──┐ ┌─▼──────────┐
│os_output│ │last_item   │
│ .csv    │ │.csv (state)│
└─────────┘ └────────────┘
```

---

## 📦 Output Files

### 1️⃣ os_output.csv

Main extracted dataset.

| Column | Description |
|--------|-------------|
| system_log_id | Unique log ID |
| system_log_type | Log type |
| date_performed | Timestamp (Europe/Prague timezone) |
| severity | Log severity |
| user_agent | User agent string |
| relevant_domain | Domain |
| relevant_domain_id | Domain ID |
| ... | Other selected columns |

Includes extracted and transformed columns, timestamp normalization, and additional fields like `problem_detail`, `result`, `is_processed`.

### 2️⃣ last_item.csv

Stores the last successfully processed log to continue incrementally in the next run.

| Column | Description |
|--------|-------------|
| id | ID of the last processed log |
| timestamp | UTC timestamp |
| timestamp_cz | Local Prague timestamp |

---

## ⚙️ Configuration Overview

### Required Parameters
- `index_name` — OpenSearch index name.
- `batch_size` — Extraction batch size (default: 500).
- `date` — Starting date if no `last_item.csv` exists.
- `hours` — Extraction window in hours (default: 1).

### Authentication
- API Key ID + API Key

### SSH (Optional)
- Supports SSH tunneling via private key.

### Input Table
You must provide `/data/in/tables/last_item.csv` before each run.

Minimal example:

```csv
id,timestamp,timestamp_cz
,,
```

---

## 🟣 Incremental Extraction Explained

The extractor uses the `last_item.csv` file to continue from the last successfully processed log.

- ✅ If `last_item.csv` exists → extraction continues from saved timestamp and ID.
- ✅ If missing or empty → extraction starts from the configured `date` parameter.
- ✅ After successful extraction, the file is automatically updated.

### Reset incremental extraction
If you want to restart the extraction from scratch:
1. Delete `last_item.csv`
2. Or set a custom `date` parameter

---

## 💡 Best Practices

- Keep batch size reasonable (`500 - 5000`) depending on data volume.
- Use SSH when accessing secured internal OpenSearch clusters.
- Always version-control your configuration files.
- Regularly check the content of `last_item.csv` to avoid unexpected duplicates.

---

## 🐳 Development

```bash
git clone https://github.com/gopaydata/gopay.ex-opensearch.git
cd gopay.ex-opensearch
docker-compose build
docker-compose run --rm dev
```

## 🧪 Testing

```bash
docker-compose run --rm test
```

---

## 📚 Useful Links

- [OpenSearch Search API Docs](https://opensearch.org/docs/latest/api-reference/search/)
- [Keboola Component Development](https://components.keboola.com/~/components/gopay.ex-opensearch)
- [Official Repository](https://github.com/gopaydata/gopay.ex-opensearch)
