import datetime
import random
import time
import json
import os # Import the os module

def generate_mock_data(num_records=100, start_date_str="2025-06-15"):
    """
    Generates mock data for Reddit API, including text for sentiment analysis.

    Args:
        num_records (int): The total number of records to generate.
        start_date_str (str): The start date in YYYY-MM-DD format.

    Returns:
        list: A list of dictionaries, each representing a mock Reddit post.
    """
    mock_data = []
    tickers = ["GME", "AMC", "PLTR", "TSLA", "AAPL", "MSFT", "GOOG", "AMZN"]
    sentiment_types = {
        "Bullish": ["Stock market soaring, incredible gains!", "Massive rally, buy now!", "Optimistic outlook, strong fundamentals.", "Breaking resistance, clear skies ahead!", "Earnings beat, shares up!"],
        "Bearish": ["Concerns growing about inflation, market is volatile.", "Recession fears loom, cautious outlook ahead.", "Market taking a dive, prepare for correction.", "Weak earnings, stock plunging.", "Economic slowdown, sell-off in sight."],
        "Neutral": ["Minor dip, but long-term outlook remains positive.", "Market stability returning, positive signs emerging.", "Consolidation phase, waiting for next move.", "Mixed signals, sideways trading expected.", "Analysts divided on future direction."]
    }

    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    # Generate dates for 21 days, spanning two months
    date_range = [start_date + datetime.timedelta(days=i) for i in range(21)]
    # To ensure around 100 records and balanced distribution,
    # generate roughly num_records / 21 records per day.
    records_per_day = num_records // len(date_range)
    remaining_records = num_records % len(date_range)

    record_id_counter = 1

    for day_offset, current_date in enumerate(date_range):
        num_records_today = records_per_day
        if remaining_records > 0:
            num_records_today += 1
            remaining_records -= 1

        for _ in range(num_records_today):
            selected_sentiment_type = random.choice(list(sentiment_types.keys()))
            title_text = random.choice(sentiment_types[selected_sentiment_type])
            body_text = f"Further details on {selected_sentiment_type.lower()} trend. {title_text.replace('!', '.')} Expect some market movements soon."

            # Assign sentiment score based on sentiment type
            if selected_sentiment_type == "Bullish":
                sentiment_score = round(random.uniform(0.1, 0.5), 3) # Positive score
            elif selected_sentiment_type == "Bearish":
                sentiment_score = round(random.uniform(-0.5, -0.1), 3) # Negative score
            else: # Neutral
                sentiment_score = round(random.uniform(-0.05, 0.05), 3) # Near zero

            mock_data.append({
                "id": f"post_{record_id_counter}", # Added for unique identification in your system
                "no_of_comments": random.randint(10, 500),
                "sentiment": selected_sentiment_type,
                "sentiment_score": sentiment_score,
                "ticker": random.choice(tickers),
                "title": title_text,
                "body": body_text,
                "created_utc": int(current_date.timestamp()) # Unix timestamp
            })
            record_id_counter += 1

    # Shuffle the data to mix dates and sentiments, then truncate to num_records if needed
    random.shuffle(mock_data)
    return mock_data[:num_records]

# Generate 100 records starting from June 15, 2025
mocked_data_for_sentiment_analysis = generate_mock_data(num_records=100, start_date_str="2025-06-15")

# You can print the first few records to inspect the structure
print(f"Generated {len(mocked_data_for_sentiment_analysis)} mock records.")
# For example, print the first 5 records
for i, record in enumerate(mocked_data_for_sentiment_analysis[:5]):
    print(f"Record {i+1}: {record}")
    print("-" * 30)

# Define the directory path
output_directory = "data"
# Define the full file path
output_filepath = os.path.join(output_directory, "mocked_reddit_data.json")

# Create the directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Save the data to the specified file path
with open(output_filepath, "w", encoding="utf-8") as f:
    json.dump(mocked_data_for_sentiment_analysis, f, ensure_ascii=False, indent=4)

print(f"\nMocked data saved to '{output_filepath}'")