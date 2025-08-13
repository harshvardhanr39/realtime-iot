import os, boto3, logging
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

cfg = Config(
    connect_timeout=3,  # fast fail if not reachable
    read_timeout=5,
    retries={"max_attempts": 2},
)

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
    region_name=os.getenv("AWS_REGION", "us-east-1"),
)

endpoint = os.getenv("DDB_ENDPOINT", "http://127.0.0.1:8000")
print(f"Using endpoint: {endpoint}")

try:
    ddb = session.client("dynamodb", endpoint_url=endpoint, config=cfg)
    out = ddb.list_tables()
    print("OK list_tables ->", out)
except EndpointConnectionError as e:
    print("Endpoint connection error:", e)
except (BotoCoreError, ClientError) as e:
    print("Boto/Dynamo error:", e)
