class ClassifyService:
    HIGH_URGENCY_KEYWORDS = {
        "lawsuit",
        "legal",
        "gdpr",
        "chargeback",
        "fraud",
        "security",
        "breach",
        "emergency",
        "dangerous",
    }
    MEDIUM_URGENCY_KEYWORDS = {
        "refund",
        "invoice",
        "payment",
        "broken",
        "error",
        "bug",
        "glitch",
        "outage",
        "fail",
        "not working",
    }
    NEGATIVE_SENTIMENT_KEYWORDS = {
        "angry",
        "terrible",
        "scam",
        "worst",
        "useless",
        "incompetent",
        "frustrating",
        "disappointed",
        "slow",
    }
    POSITIVE_SENTIMENT_KEYWORDS = {
        "great",
        "awesome",
        "amazing",
        "thanks",
        "helpful",
        "best",
        "love",
    }

    @staticmethod
    def classify(message: str, subject: str) -> dict:
        """
        Very simple starter implementation of rule-based classification.
        You are encouraged to review and refine the rules to make them more
        realistic and internally consistent.
        """
        text = f"{subject} {message}".lower()

        if any(keyword in text for keyword in ClassifyService.HIGH_URGENCY_KEYWORDS):
            urgency = "high"
        elif any(keyword in text for keyword in ClassifyService.MEDIUM_URGENCY_KEYWORDS):
            urgency = "medium"
        else:
            urgency = "low"

        if any(keyword in text for keyword in ClassifyService.NEGATIVE_SENTIMENT_KEYWORDS):
            sentiment = "negative"
        elif any(keyword in text for keyword in ClassifyService.POSITIVE_SENTIMENT_KEYWORDS):
            sentiment = "positive"
        else:
            sentiment = "neutral"

        requires_action = (
            urgency in {"high", "medium"} or sentiment == "negative"
        )

        return {
            "urgency": urgency,
            "sentiment": sentiment,
            "requires_action": requires_action,
        }
