import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import asyncio 

# Initialize logger
log = LoggingMixin().log

def _run_data_ingestion_and_sentiment():
    """
    Task function to fetch data, perform sentiment analysis, and store it.
    This function will call the main logic residing in src/main.py.
    """
    log.info("Starting data ingestion and sentiment analysis process...")
    try:
        from main import run_data_ingestion_pipeline
        # We don't want to return anything from this function to avoid XCom serialization issues.
        _ = asyncio.run(run_data_ingestion_pipeline())
        log.info("Data ingestion and sentiment analysis completed successfully.")
    except Exception as e:
        log.error(f"Error during data ingestion and sentiment analysis: {e}")
        raise # Re-raise to fail the Airflow task
    return None

def _run_data_validation_with_ge():
    """
    Task function to execute Great Expectations checkpoints for data quality.
    This function will call the GE validation logic.
    """
    log.info("Starting Great Expectations data validation...")
    try:
        from main import run_ge_validation_checkpoint
        # asyncio.run returns the result of the coroutine.
        # We don't want to return anything from this function to avoid XCom serialization issues.
        result = asyncio.run(run_ge_validation_checkpoint(checkpoint_name="my_checkpoint"))
        if not result["success"]:
            log.error(f"Data Validation FAILED for checkpoint 'my_checkpoint'.")
            # Optionally, you might want to retrieve and log the validation report URL
            # Forcing DAG failure if validation fails
            raise ValueError(f"Data validation failed for checkpoint 'my_checkpoint'. Check GE reports.")
        else:
            log.info(f"Data Validation PASSED for checkpoint 'my_checkpoint'.")
    except Exception as e:
        log.error(f"Error during Great Expectations validation: {e}")
        raise # Re-raise to fail the Airflow task
    return None

def _run_trend_analysis():
    """
    Task function to perform bullish/bearish trend analysis and store results.
    This function will call the trend analysis logic residing in src/main.py.
    """
    log.info("Starting trend analysis process...")
    try:
        from main import run_trend_analysis_pipeline
        # We don't want to return anything from this function to avoid XCom serialization issues.
        _ = asyncio.run(run_trend_analysis_pipeline())
        log.info("Trend analysis completed successfully.")
    except Exception as e:
        log.error(f"Error during trend analysis: {e}")
        raise # Re-raise to fail the Airflow task
    return None

with DAG(
    dag_id="reddit_sentiment_etl_pipeline",
    # Use pendulum for start_date as recommended
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 0 * * *", # Runs daily at midnight UTC
    catchup=False, # Set to False to prevent backfilling on first run
    tags=["reddit", "etl", "sentiment_analysis", "trends", "great_expectations"],
    # Add optional description for the DAG UI
    doc_md="""
    ### Reddit Sentiment ETL and Trend Analysis Pipeline

    This DAG orchestrates the process of fetching Reddit data, performing sentiment analysis
    with Pandas, validating data quality using Great Expectations, identifying bullish/bearish
    trends, and storing all processed information in a PostgreSQL database.

    **Flow:**
    1.  `fetch_sentiment_data`: Fetches raw data from the Tradestie API,
        performs sentiment analysis, and persists it.
    2.  `validate_sentiment_data`: Runs Great Expectations checkpoints on the newly
        ingested and processed sentiment data to ensure quality.
    3.  `analyze_bullish_bearish_trends`: Aggregates the sentiment data over 7, 14,
        and 21-day periods to identify and store bullish/bearish trends.
    """
) as dag:
    # Task 1: Fetch data, perform sentiment analysis, and store
    fetch_sentiment_data = PythonOperator(
        task_id="fetch_sentiment_data",
        python_callable=_run_data_ingestion_and_sentiment,
        # Potentially add retries, e.g., retries=3, retry_delay=pendulum.duration(minutes=5)
    )

    # Task 2: Validate the newly ingested and processed data using Great Expectations
    validate_sentiment_data = PythonOperator(
        task_id="validate_sentiment_data",
        python_callable=_run_data_validation_with_ge,
    )

    # Task 3: Perform trend analysis on the processed data
    analyze_bullish_bearish_trends = PythonOperator(
        task_id="analyze_bullish_bearish_trends",
        python_callable=_run_trend_analysis,
    )

    # Define task dependencies
    fetch_sentiment_data >> validate_sentiment_data >> analyze_bullish_bearish_trends