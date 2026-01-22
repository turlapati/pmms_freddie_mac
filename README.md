# PMMS Freddie Mac Data Ingestion

Streams Freddie Mac Primary Mortgage Market Survey (PMMS) data into SQLite with incremental updates and comprehensive logging.

## Quick Start

```bash
# Run ingestion
python3 ingest_pmms.py

# Run continuously (every 60 seconds)
for i in {1..600}; do python3 ingest_pmms.py; sleep 60; done
```

## Configuration

Edit `ingest_pmms.config` to customize database, logging, proxy, and HTTP settings.

### Proxy Configuration
```python
# Basic proxy
HTTP_PROXY = "http://proxy.company.com:8080"
HTTPS_PROXY = "http://proxy.company.com:8080"

# With authentication
PROXY_USERNAME = "your_username"
PROXY_PASSWORD = "your_password"
```

## Database Schema

SQLite table `pmms_rates`:
- `date` (TEXT PRIMARY KEY): YYYY-MM-DD format
- `pmms30` (REAL): 30-year fixed mortgage rate  
- `pmms15` (REAL): 15-year fixed mortgage rate

## Features

- **Streaming ingestion**: Memory-efficient CSV processing
- **Incremental updates**: Only fetches new records
- **Proxy support**: HTTP/HTTPS with authentication
- **Log rotation**: Automatic rotation with compression
- **Error handling**: Network, parsing, and database errors

## Dependencies

- Python 3.6+
- `requests` library (install with: `pip install -r requirements.txt`)

## Data Source

Freddie Mac Primary Mortgage Market Survey:
https://www.freddiemac.com/pmms/docs/PMMS_history.csv
