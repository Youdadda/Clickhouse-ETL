# Migration from OpenSearch to ClickHouse
## Overview

This project provides an ETL pipeline to migrate data from OpenSearch to ClickHouse. The pipeline extracts data from OpenSearch, transforms it as needed, and loads it into a ClickHouse database.

## Prerequisites

- Python 3.8+
- Access to an OpenSearch cluster
- Access to a ClickHouse server
- [pip](https://pip.pypa.io/en/stable/)

## Setup

1. **Clone the repository:**
    ```bash
    git clone https://github.com/Youdadda/Clickhouse-ETL.git
    cd Clickhouse-ETL
    ```

2. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3. **Configure environment variables:**

    Create a `.env` file in the project root with the following content:
    ```
    OPENSEARCH_HOST=<your-opensearch-host>
    OPENSEARCH_PORT=<your-opensearch-port>
    OPENSEARCH_USER=<your-opensearch-username>
    OPENSEARCH_PASSWORD=<your-opensearch-password>
    CLICKHOUSE_HOST=<your-clickhouse-host>
    CLICKHOUSE_PORT=<your-clickhouse-port>
    CLICKHOUSE_USER=<your-clickhouse-username>
    CLICKHOUSE_PASSWORD=<your-clickhouse-password>
    CLICKHOUSE_DATABASE=<your-clickhouse-database>
    ```

## Usage

Run the migration pipeline with:

```bash
cd Pipeline
python migration.py
```

This will connect to the configured OpenSearch and ClickHouse instances, extract data, perform any necessary transformations, and load it into ClickHouse.

## Customization

- **Transformation Logic:**  
  Modify the transformation step in `pipeline.py` to suit your data requirements.

- **Index/Table Names:**  
  Update the source OpenSearch index and target ClickHouse table names in the configuration section of `pipeline.py`.

## Troubleshooting

- Ensure network connectivity to both OpenSearch and ClickHouse.
- Check credentials and permissions for both databases.
- Review logs for error messages.

## License

See [LICENSE](LICENSE) for details.