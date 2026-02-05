class ClassifyService:
    @staticmethod
    def classify(message: str, subject: str) -> dict:
        """
        Very simple starter implementation of rule-based classification.
        You are encouraged to review and refine the rules to make them more
        realistic and internally consistent.
        """
        text = f"{subject} {message}".lower()

        if "lawsuit" in text or "gdpr" in text or "urgent" in text:
            urgency = "high"
        elif "refund" in text or "broken" in text:
            urgency = "medium"
        else:
            urgency = "low"

        if "angry" in text or "broken" in text or "lawsuit" in text:
            sentiment = "negative"
        elif "great" in text or "thanks" in text:
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
