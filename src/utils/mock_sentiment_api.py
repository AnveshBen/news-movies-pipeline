import random

def get_sentiment_score(text: str) -> float:
    """
    Mock API to get sentiment score for a given text.
    Returns a random float between 0 and 1.
    """
    return random.uniform(0, 1) 