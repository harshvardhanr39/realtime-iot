# scripts/create_ddb_table.py (replace the ddb creation lines)
import os, boto3, botocore

endpoint = os.getenv("DDB_ENDPOINT", "http://localhost:8000")
region = os.getenv("AWS_REGION", "us-east-1")
table_name = os.getenv("DDB_TABLE", "openaq_timeseries")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
    region_name=region,
)
ddb = session.resource("dynamodb", endpoint_url=endpoint)
def ensure_table():
    try:
        table = ddb.Table(table_name)
        table.load()
        print(f"Table '{table_name}' already exists.")
        return
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            raise

    print(f"Creating table '{table_name}' ...")
    table = ddb.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    print("Table ACTIVE.")

if __name__ == "__main__":
    ensure_table()
