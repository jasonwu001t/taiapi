from chalice import Chalice, Response
import boto3
import json
from botocore.exceptions import ClientError

app = Chalice(app_name='taiapi')

# Initialize boto3 client for S3
s3_client = boto3.client('s3')

BUCKET_NAME = 'jtrade1-dir'  # S3 bucket name

# Helper function to fetch JSON data from S3
def fetch_json_from_s3(key: str):
    """Fetch JSON data from S3 bucket."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        return {"error": f"Unable to fetch {key} from S3: {str(e)}"}
    except json.JSONDecodeError:
        return {"error": f"Error decoding JSON in {key}"}

# Helper function to create JSON responses
def create_json_response(body, status_code=200):
    return Response(
        body=body,
        status_code=status_code,
        headers={'Content-Type': 'application/json'}
    )

categories = fetch_json_from_s3('api/treasury_yield_all.json')
print (categories)