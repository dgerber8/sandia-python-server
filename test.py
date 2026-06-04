import boto3
from collections import defaultdict

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table    = dynamodb.Table("simulation-ingest")

# Scan just date + timestamp key attributes
date_latest = defaultdict(str)
last_key    = None

while True:
    kwargs = {
        "ProjectionExpression":     "#d, #ts",
        "ExpressionAttributeNames": {"#d": "date", "#ts": "timestamp"},
        "FilterExpression":         boto3.dynamodb.conditions.Attr("date").ne("__meta__"),
    }
    if last_key:
        kwargs["ExclusiveStartKey"] = last_key
    resp     = table.scan(**kwargs)
    for item in resp.get("Items", []):
        d, ts = item.get("date", ""), item.get("timestamp", "")
        if d and ts and ts > date_latest[d]:
            date_latest[d] = ts
    last_key = resp.get("LastEvaluatedKey")
    if not last_key:
        break

# Write the meta item in one shot
date_info = {d: ts for d, ts in date_latest.items() if d and ts}
table.put_item(Item={"date": "__meta__", "timestamp": "DATES", "dateInfo": date_info})
print(f"Wrote {len(date_info)} dates: {sorted(date_info)}")