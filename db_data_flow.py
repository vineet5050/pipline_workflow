from prefect import flow, task, get_run_logger
from prefect.artifacts import create_link_artifact
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

@flow(name="Product Data Pipeline")
def product_pipeline(customer_count: int):
    logger = get_run_logger()
    if customer_count > 600:
        logger.info(f"Starting product copy (Customer count: {customer_count})")
        logger.info("Product data copied to ADLS")
        return True
    else:
        logger.warning("Skipped product copy: Customer count <= 600")
        return False

@task
def get_customer_count():
    engine = create_engine(os.getenv("DB_CONN_STR"))
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM customers"))
        count = result.scalar()
    return count

@task
def copy_customer_data_to_adls():
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("ADLS_CONN_STRING")
    )
    container_client = blob_service_client.get_container_client(os.getenv("ADLS_CONTAINER_NAME"))
    
    blob_name = f"customers/customer_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    
    blob_client = container_client.get_blob_client(blob_name)
    
    return blob_name

@flow(name="Customer Data Pipeline")
def customer_pipeline():
    logger = get_run_logger()
    customer_count = get_customer_count()
    
    if customer_count > 500:
        logger.info(f"Copying customer data (Count: {customer_count})")
        adls_path = copy_customer_data_to_adls()
        create_link_artifact(
            key="adls-customer-data",
            link=adls_path,
            description="Customer data in ADLS"
        )
        
        product_pipeline(customer_count)
    else:
        logger.warning("Skipped customer copy: Count <= 500")

if __name__ == "__main__":
    customer_pipeline()