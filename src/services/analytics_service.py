from datetime import datetime, timedelta, timezone
from src.db.mongo import get_db

class AnalyticsService:
    async def get_tenant_stats(self, tenant_id: str, from_date: datetime, to_date: datetime) -> dict:
        """
        Compute analytics for a tenant within a date range.
        Focus on letting the database do the heavy lifting rather than
        bringing large result sets into application memory.
        """
        db = await get_db()
        match: dict = {"tenant_id": tenant_id, "deleted_at": None}
        if from_date or to_date:
            match["created_at"] = {}
            if from_date:
                match["created_at"]["$gte"] = from_date
            if to_date:
                match["created_at"]["$lte"] = to_date

        last_24_hours = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)

        pipeline = [
            {"$match": match},
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "by_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                        {"$project": {"_id": 0, "k": "$_id", "v": "$count"}}
                    ],
                    "urgency_high": [
                        {"$match": {"urgency": "high"}},
                        {"$count": "count"}
                    ],
                    "negative_sentiment": [
                        {"$match": {"sentiment": "negative"}},
                        {"$count": "count"}
                    ],
                    "hourly_trend": [
                        {"$match": {"created_at": {"$gte": last_24_hours}}},
                        {"$group": {"_id": {"$dateTrunc": {"date": "$created_at", "unit": "hour"}}, "count": {"$sum": 1}}},
                        {"$sort": {"_id": 1}},
                        {"$project": {"_id": 0, "hour": "$_id", "count": 1}}
                    ],
                    "top_keywords": [
                        {"$project": {"words": {"$split": [{"$toLower": "$message"}, " "]}}},
                        {"$unwind": "$words"},
                        {"$match": {"words": {"$regex": "^[a-z]{4,}$"}}},
                        {"$group": {"_id": "$words", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10},
                        {"$project": {"_id": 0, "word": "$_id"}}
                    ],
                    "at_risk_customers": [
                        {"$match": {"urgency": "high"}},
                        {"$group": {"_id": "$customer_id", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 5},
                        {"$project": {"_id": 0, "customer_id": "$_id", "count": 1}}
                    ]
                }
            },
            {
                "$project": {
                    "total_tickets": {"$ifNull": [{"$arrayElemAt": ["$total.count", 0]}, 0]},
                    "by_status": {
                        "$cond": [
                            {"$gt": [{"$size": "$by_status"}, 0]},
                            {"$arrayToObject": "$by_status"},
                            {}
                        ]
                    },
                    "urgency_high_ratio": {
                        "$let": {
                            "vars": {
                                "total": {"$ifNull": [{"$arrayElemAt": ["$total.count", 0]}, 0]},
                                "high": {"$ifNull": [{"$arrayElemAt": ["$urgency_high.count", 0]}, 0]}
                            },
                            "in": {
                                "$cond": [
                                    {"$gt": ["$$total", 0]},
                                    {"$divide": ["$$high", "$$total"]},
                                    0
                                ]
                            }
                        }
                    },
                    "negative_sentiment_ratio": {
                        "$let": {
                            "vars": {
                                "total": {"$ifNull": [{"$arrayElemAt": ["$total.count", 0]}, 0]},
                                "negative": {"$ifNull": [{"$arrayElemAt": ["$negative_sentiment.count", 0]}, 0]}
                            },
                            "in": {
                                "$cond": [
                                    {"$gt": ["$$total", 0]},
                                    {"$divide": ["$$negative", "$$total"]},
                                    0
                                ]
                            }
                        }
                    },
                    "hourly_trend": "$hourly_trend",
                    "top_keywords": {
                        "$map": {
                            "input": "$top_keywords",
                            "as": "kw",
                            "in": "$$kw.word"
                        }
                    },
                    "at_risk_customers": "$at_risk_customers"
                }
            }
        ]

        results = await db.tickets.aggregate(pipeline).to_list(length=1)
        if results:
            return results[0]

        return {
            "total_tickets": 0,
            "by_status": {},
            "urgency_high_ratio": 0,
            "negative_sentiment_ratio": 0,
            "hourly_trend": [],
            "top_keywords": [],
            "at_risk_customers": []
        }
