import requests
import datetime
from elasticsearch import Elasticsearch, helpers

# ------------------ CONFIG ------------------ #
FINNHUB_API_KEY = ""  # Replace with your Finnhub API key
ELASTIC_CLOUD_ID = ""  # Replace with your Elastic Cloud ID
ELASTIC_API_KEY = ""  # Replace with your Elastic API key
INDEX_NAME = "real_stock_sentiment"

# Stocks to track
TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN"]

# ------------------ CONNECT TO ELASTICSEARCH ------------------ #
es = Elasticsearch(
    cloud_id=ELASTIC_CLOUD_ID,
    api_key=ELASTIC_API_KEY
)
print("Connected to Elasticsearch")

# ------------------ FINNHUB NEWS FETCH ------------------ #
def get_sentiment(text):
    """Very simple sentiment scoring (placeholder)"""
    positive_words = ["up", "gain", "positive", "growth", "beat", "strong", "bullish", "record", "buy"]
    negative_words = ["down", "loss", "negative", "drop", "miss", "weak", "bearish", "sell"]

    score = 0
    lower_text = text.lower()
    for word in positive_words:
        if word in lower_text:
            score += 1
    for word in negative_words:
        if word in lower_text:
            score -= 1
    return score / max(len(positive_words) + len(negative_words), 1)

def fetch_news_for_ticker(ticker):
    """Fetches news for a stock ticker from Finnhub"""
    url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from=2020-01-01&to=2030-01-01&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f" Error fetching news for {ticker}: {response.status_code}")
        return []

# ------------------ INDEXING INTO ELASTICSEARCH ------------------ #
def index_stock_sentiment():
    actions = []
    now = datetime.datetime.utcnow()

    for ticker in TICKERS:
        news_items = fetch_news_for_ticker(ticker)
        print(f" Indexing {len(news_items)} news items for {ticker}")

        for news in news_items:
            sentiment_score = get_sentiment(news.get("summary", ""))
            actions.append({
                "_index": INDEX_NAME,
                "_source": {
                    "ticker": ticker,
                    "headline": news.get("headline", ""),
                    "summary": news.get("summary", ""),
                    "source": news.get("source", ""),
                    "sentiment": sentiment_score,
                    "timestamp": datetime.datetime.utcfromtimestamp(news.get("datetime", now.timestamp()))
                }
            })

    if actions:
        helpers.bulk(es, actions)
        print(" Indexed all stock sentiment data for Kibana.")

# ------------------ RUN ------------------ #
if __name__ == "__main__":
    index_stock_sentiment()
