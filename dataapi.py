import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pandas as pd
import findspark

# Load .env file
load_dotenv()
findspark.init()

# Read API key from environment
API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


def get_symbols():
    url = f"{BASE_URL}/stock/symbol"
    params = {
        "exchange": "US",
        "token": API_KEY
    }
    response = requests.get(url, params=params)
    return response.json()


def get_stock_quote(symbol):
    url = f"{BASE_URL}/quote"
    params = {"symbol": symbol, "token": API_KEY}
    response = requests.get(url, params=params)
    return response.json()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Stock_ETL") \
        .master("local[*]") \
        .getOrCreate()

    data = get_symbols()
    dataframe = pd.DataFrame(data)

    # Optional: show head using pandas
    print(dataframe.head())

    df = spark.createDataFrame(dataframe)
    df.show(5)

    # Example: Get quote for Apple
    quote = get_stock_quote("AAPL")
    print("Apple Quote:", quote)
