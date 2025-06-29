# Pipeline Workflow Documentation

This project implements a data pipeline that integrates with both a database and Azure Data Lake Storage (ADLS).

## Prerequisites

Before running the pipeline, ensure you have:

1. Python 3.8+ installed
2. Azure Data Lake Storage account access
3. Database access credentials
4. Required Python packages (see `requirements.txt`)

## Configuration

The pipeline requires the following environment variables to be set:

```bash
# Database connection string
DB_CONN_STR="your_database_connection_string"

# Azure Data Lake Storage connection string
ADLS_CONN_STRING="your_adls_connection_string"

# ADLS container name
ADLS_CONTAINER_NAME="your_container_name"
```

## Setup Instructions

1. Clone this repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set the environment variables as shown above

## Running the Pipeline

To execute the pipeline:

```bash
python main.py
```

## Troubleshooting

If you encounter any issues:

1. Verify all environment variables are set correctly
2. Check database connection string format
3. Ensure ADLS connection string is valid
4. Confirm ADLS container exists and is accessible

## Support

For any questions or issues, please contact the project maintainer.