from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests
import json
from prefect.schedules.clocks import CronClock

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=12))
def fetch_country_data(country: str):
    logger = get_run_logger()
    url = f"https://restcountries.com/v3.1/name/{country}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    logger.info(f"Fetched data for {country}")
    return data

@task
def save_to_json(data: dict, country: str):
    with open(f"{country}.json", "w") as f:
        json.dump(data, f, indent=2)
    return f"{country}.json"

@flow(name="Country Data Pipeline")
def country_pipeline(countries: list = ["india", "us", "uk", "china", "russia"]):
    for country in countries:
        data = fetch_country_data(country)
        save_to_json(data, country)

# Configure schedule (runs at 00:00 and 12:00 IST)
deployment = country_pipeline.to_deployment(
    name="scheduled",
    schedule=CronClock(cron="0 0,12 * * *", timezone="Asia/Kolkata")
)
deployment.apply()