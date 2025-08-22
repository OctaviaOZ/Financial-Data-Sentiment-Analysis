import os
import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import logging
import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, JSON
import json
from typing import Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configuration from environment variables
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
API_URL = os.getenv("API_URL", "https://tradestie.com/api/v1/apps/reddit")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 3600)) # Cache for 1 hour
DATABASE_URL = os.getenv("DATABASE_URL")

# Add new variables for API mocking
USE_MOCK_API = os.getenv("USE_MOCK_API", "false").lower() == "true"
MOCK_DATA_PATH = os.getenv("MOCK_DATA_PATH", "/app/data/mocked_reddit_data.json")

# Initialize Redis client
redis_client = redis.from_url(REDIS_URL)

# HTTPX client with retry and backoff
async_client = httpx.AsyncClient(
    timeout=httpx.Timeout(10.0, connect=5.0),
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
)

# SQLAlchemy setup
Base = declarative_base()
engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)

class TickerSentimentDB(Base):
    __tablename__ = "ticker_sentiments"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    title = Column(String)
    body = Column(String)
    created_utc = Column(Integer)
    no_of_comments = Column(Integer)
    sentiment = Column(String)
    sentiment_score = Column(Float)
    retrieval_date = Column(DateTime, default=datetime.utcnow)
    raw_data = Column(JSON) # Store the entire raw JSON for flexibility

class TrendDataDB(Base):
    __tablename__ = "trend_data"
    id = Column(Integer, primary_key=True, index=True)
    period_days = Column(Integer, index=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    avg_sentiment_score = Column(Float)
    total_tickers = Column(Integer)
    bullish_count = Column(Integer)
    bearish_count = Column(Integer)
    neutral_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

class TickerSentiment(BaseModel):
    id: str
    no_of_comments: int
    sentiment: str
    sentiment_score: float
    ticker: str
    title: str
    body: str
    created_utc: int

class APIResponse(BaseModel):
    data: list[TickerSentiment]
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)

class ProcessedTickerSentiment(TickerSentiment):
    retrieval_date: datetime

    class Config:
        orm_mode = True

class TrendData(BaseModel):
    period_days: int
    start_date: datetime
    end_date: datetime
    avg_sentiment_score: float
    total_tickers: int
    bullish_count: int
    bearish_count: int
    neutral_count: int

    class Config:
        orm_mode = True

async def fetch_data_from_api(url: str, date: str = None) -> list[dict]:
    """
    Fetches data from the external API with retry and backoff, optionally by date.
    Supports loading from mock data based on USE_MOCK_API flag.
    """
    if USE_MOCK_API:
        logger.info(f"USE_MOCK_API is TRUE. Loading data from mock file: {MOCK_DATA_PATH}")
        try:
            with open(MOCK_DATA_PATH, "r", encoding="utf-8") as f:
                all_mock_data = json.load(f)
            logger.info(f"Successfully loaded {len(all_mock_data)} records from mock data.")

            if date:
                # Filter mock data by the specified date
                target_date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                filtered_data = [
                    item for item in all_mock_data
                    if datetime.fromtimestamp(item["created_utc"]).date() == target_date_obj
                ]
                logger.info(f"Filtered mock data for date {date}: {len(filtered_data)} records.")
                return filtered_data
            else:
                # If no date is specified, return all mock data (or a recent subset).
                # Here, we return all, as the size of 100 records is small.
                return all_mock_data
        except FileNotFoundError:
            logger.error(f"Mock data file not found at {MOCK_DATA_PATH}. Please ensure it exists.")
            raise HTTPException(status_code=500, detail="Mock data file not found.")
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from mock data file: {MOCK_DATA_PATH}.")
            raise HTTPException(status_code=500, detail="Error parsing mock data file.")
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing mock data: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing mock data: {e}")
    else:
        # Original logic for fetching from the real API
        full_url = url
        if date:
            full_url = f"{url}?date={date}"

        retries = 3
        backoff_factor = 0.5
        for i in range(retries):
            try:
                logger.info(f"Fetching data from {full_url}, attempt {i + 1}")
                response = await async_client.get(full_url)
                response.raise_for_status() # Raise an exception for 4xx or 5xx status codes
                return response.json()
            except httpx.RequestError as exc:
                logger.error(f"An error occurred while requesting {exc.request.url!r}: {exc}")
                if i < retries - 1:
                    await asyncio.sleep(backoff_factor * (2 ** i)) # Exponential backoff
                else:
                    raise HTTPException(status_code=503, detail=f"Failed to connect to external API after {retries} retries.")
            except httpx.HTTPStatusError as exc:
                logger.error(f"Error response {exc.response.status_code} while requesting {exc.request.url!r}: {exc.response.text}")
                if exc.response.status_code in [429, 500, 502, 503, 504] and i < retries - 1:
                    await asyncio.sleep(backoff_factor * (2 ** i)) # Retry for specific server errors
                else:
                    raise HTTPException(status_code=exc.response.status_code, detail=f"API returned an error: {exc.response.text}")
        return [] # Should not reach here

async def process_and_store_data(data: list[Union[dict, TickerSentiment]], session: AsyncSession):
    """Processes data, performs sentiment analysis, and stores in DB."""
    processed_tickers = []
    for item in data:
        # Check if item is a dictionary (from API fetch) or a TickerSentiment object (from cache)
        if isinstance(item, dict):
            ticker_data = item
        else: # Assume it's a TickerSentiment object
            ticker_data = item.dict() # Convert Pydantic model to dictionary
            
        db_ticker = TickerSentimentDB(
            ticker=item.get("ticker"),
            title=item.get("title"),
            body=item.get("body"),
            created_utc=item.get("created_utc"),
            no_of_comments=item.get("no_of_comments"),
            sentiment=item.get("sentiment"),
            sentiment_score=item.get("sentiment_score"),
            retrieval_date=datetime.utcnow(),
            raw_data=item
        )
        session.add(db_ticker)
        processed_tickers.append(db_ticker)
    await session.commit()
    for ticker in processed_tickers:
        await session.refresh(ticker)
    logger.info(f"Stored {len(processed_tickers)} processed tickers to database.")
    return processed_tickers

async def aggregate_trend_data(period_days: int):
    """Aggregates sentiment trend data for a given period and stores in DB."""
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=period_days)

    async with AsyncSessionLocal() as session:
        # Fetch tickers within the period
        # Using f-string directly in execute for simplicity, but parameterized queries are safer.
        # Ensure retrieval_date in DB matches UTC timezone as datetime.utcnow()
        result = await session.execute(
            f"""SELECT sentiment_score, sentiment FROM ticker_sentiments WHERE retrieval_date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"""
        )
        tickers = result.all() # Fetch all results before processing

        if not tickers:
            logger.warning(f"No tickers found for trend aggregation in the last {period_days} days.")
            return

        df = pd.DataFrame([{"sentiment_score": t.sentiment_score, "sentiment": t.sentiment} for t in tickers])
        
        avg_sentiment = df["sentiment_score"].mean()
        
        bullish_count = df[df["sentiment"] == "Bullish"].shape[0]
        bearish_count = df[df["sentiment"] == "Bearish"].shape[0]
        neutral_count = df[df["sentiment"] == "Neutral"].shape[0]

        trend_data = TrendDataDB(
            period_days=period_days,
            start_date=start_date,
            end_date=end_date,
            avg_sentiment_score=avg_sentiment,
            total_tickers=len(tickers),
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            neutral_count=neutral_count
        )
        session.add(trend_data)
        await session.commit()
        logger.info(f"Aggregated trend data for {period_days} days and stored in DB.")

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up application...")
    try:
        await redis_client.ping()
        logger.info("Connected to Redis.")
    except Exception as e:
        logger.error(f"Could not connect to Redis: {e}")

    # Create database tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables checked/created.")
    
    # Log if mock API is enabled
    if USE_MOCK_API:
        logger.warning(f"Application is running with MOCK API enabled. Data will be loaded from {MOCK_DATA_PATH}.")
    else:
        logger.info("Application is running with REAL API enabled.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application...")
    await async_client.aclose()
    await redis_client.close()

@app.get("/health")
def health_check():
    return {"status": "ok"}

async def run_data_ingestion_pipeline(date: str = None):
    """
    Fetches Reddit data, performs sentiment analysis, and stores in DB.
    Optionally fetches data for a specific date (YYYY-MM-DD).
    This is a standalone function for Airflow.
    """
    cache_key = f"reddit_data_cache_{date or 'latest'}"
    
     # Try to get data from cache
    try:
        cached_data = await redis_client.get(cache_key)
    except Exception as e:
        logger.warning(f"Failed to retrieve from cache or parse cached data: {e}. Fetching from API instead.")
        cached_data = None # Proceed to fetch from API

    if cached_data:
        logger.info("Returning data from cache.")
        api_response = APIResponse.parse_raw(cached_data)
        data = [item.dict() for item in api_response.data] if isinstance(api_response.data[0], TickerSentiment) else api_response.data
    else:
        # If not in cache, fetch from API (or mock)
        logger.info(f"Fetching data from external API (cache miss) for date: {date or 'latest'}.")
        data = await fetch_data_from_api(API_URL, date=date)
        
        if not data:
            logger.error("Failed to retrieve data from external API.")
            raise ValueError("Failed to retrieve data from external API.")

        data_for_api_response = [
            TickerSentiment(
                id=item.get("id", ""),
                no_of_comments=item.get("no_of_comments", 0),
                sentiment=item.get("sentiment", "Neutral"),
                sentiment_score=item.get("sentiment_score", 0.0),
                ticker=item.get("ticker", "UNKNOWN"),
                title=item.get("title", ""),
                body=item.get("body", ""),
                created_utc=item.get("created_utc", 0)
            ) for item in data
        ]
        api_response = APIResponse(data=data_for_api_response)
        
        # Store in cache
        await redis_client.setex(cache_key, CACHE_TTL_SECONDS, api_response.json())
        logger.info(f"Data stored in cache with TTL: {CACHE_TTL_SECONDS} seconds.")

    async with AsyncSessionLocal() as session:
        processed_tickers = await process_and_store_data(data, session)
        return [ProcessedTickerSentiment.from_orm(t) for t in processed_tickers]

async def run_trend_analysis_pipeline():
    """
    Triggers aggregation of sentiment trend data for 7, 14, and 21 days.
    This is a standalone function for Airflow.
    """
    periods = [7, 14, 21]
    for period in periods:
        await aggregate_trend_data(period)
    logger.info("Trend aggregation completed successfully for all periods.")

# Great Expectations validation function (already standalone)
async def run_ge_validation_checkpoint(checkpoint_name: str):
    """
    Executes a Great Expectations checkpoint.
    This is a standalone function for Airflow.
    """
    logger.info(f"Starting Great Expectations data validation for checkpoint: {checkpoint_name}...")
    # Placeholder for actual Great Expectations logic
    # In a real scenario, you would import and use Great Expectations classes here.
    # For example:
    # from great_expectations.checkpoint import Checkpoint
    # context = gx.get_context()
    # checkpoint = context.get_checkpoint(checkpoint_name)
    # result = checkpoint.run()
    # if not result["success"]:
    #     raise ValueError(f"Great Expectations validation failed for checkpoint {checkpoint_name}")
    logger.info(f"Great Expectations validation for checkpoint {checkpoint_name} completed (placeholder).")
    return {"success": True} # Mock success for now

@app.get("/fetch-and-process-reddit-data", response_model=list[ProcessedTickerSentiment])
async def fetch_and_process_reddit_data_endpoint(date: str = None):
    """
    FastAPI endpoint to trigger data ingestion and sentiment analysis.
    """
    return await run_data_ingestion_pipeline(date)

@app.post("/aggregate-trends")
async def trigger_trend_aggregation_endpoint():
    """
    FastAPI endpoint to trigger aggregation of sentiment trend data.
    """
    await run_trend_analysis_pipeline()
    return {"message": "Trend aggregation triggered successfully for all periods."}

from sqlalchemy import select

# ... (rest of the file)

@app.get("/trends", response_model=list[TrendData])
async def get_trends(days: int = None):
    """
    Retrieves aggregated trend data. Optionally filter by number of days.
    """
    async with AsyncSessionLocal() as session:
        if days:
            stmt = select(TrendDataDB).where(TrendDataDB.period_days == days).order_by(TrendDataDB.created_at.desc()).limit(1)
            result = await session.execute(stmt)
        else:
            stmt = select(TrendDataDB).order_by(TrendDataDB.created_at.desc())
            result = await session.execute(stmt)
        trends = result.scalars().all()
        return [TrendData.from_orm(t) for t in trends]

@app.get("/data", response_model=list[ProcessedTickerSentiment])
async def get_processed_data(date: str = None):
    """
    Retrieves daily processed data. Optionally filter by date (YYYY-MM-DD).
    """
    async with AsyncSessionLocal() as session:
        if date:
            start_of_day = datetime.strptime(date, "%Y-%m-%d")
            end_of_day = start_of_day + timedelta(days=1)
            result = await session.execute(
                f"""SELECT * FROM ticker_sentiments WHERE retrieval_date BETWEEN '{start_of_day.isoformat()}' AND '{end_of_day.isoformat()}' ORDER BY retrieval_date DESC"""
            )
        else:
            result = await session.execute("""SELECT * FROM ticker_sentiments ORDER BY retrieval_date DESC LIMIT 100""") # Limit to 100 for general query
        tickers = result.scalars().all()
        return [ProcessedTickerSentiment.from_orm(t) for t in tickers]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)