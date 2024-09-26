from chalice import Chalice, Response
import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime

app = Chalice(app_name='taiapi')

# Initialize boto3 client for S3
s3_client = boto3.client('s3')

BUCKET_NAME = 'jtrade1-dir'  # S3 bucket name for the app
SUBSCRIPTION_KEY = 'subscriptions.json'  # Key for storing subscription data

# Define FRED and BLS S3 keys outside functions to avoid redundancy
FRED_S3_KEYS = {
    "us_30yr_fix_mortgage_rate": "api/fred/us_30yr_fix_mortgage_rate.json",
    "consumer_price_index": "api/fred/consumer_price_index.json",
    "federal_funds_rate": "api/fred/federal_funds_rate.json",
    "gdp": "api/fred/gdp.json",
    "core_cpi": "api/fred/core_cpi.json",
    "fed_total_assets": "api/fred/fed_total_assets.json",
    "m2": "api/fred/m2.json",
    "sp500": "api/fred/sp500.json",
    "commercial_banks_deposits": "api/fred/commercial_banks_deposits.json",
    "total_money_market_fund": "api/fred/total_money_market_fund.json",
    "us_producer_price_index": "api/fred/us_producer_price_index.json"
}

BLS_S3_KEYS = {
    "unemployment_rate": "api/bls/unemployment_rate.json",
    "nonfarm_payroll": "api/bls/nonfarm_payroll.json",
    "us_avg_weekly_hours": "api/bls/us_avg_weekly_hours.json",
    "us_job_opening": "api/bls/us_job_opening.json"
}

# Other categories (non-FRED, non-BLS) data path mapping
GENERIC_S3_KEYS = {
    "us_treasury_yield": "api/treasury_yield_all.json",
    "articles": "articles.json",
    "chart": "chart_data.json",
    "categories": "categories.json",
    "indicators": "indicators.json",
}

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

# Helper function to upload JSON data to S3
def upload_json_to_s3(key: str, data: dict):
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data))
    except ClientError as e:
        return {"error": f"Unable to upload {key} to S3: {str(e)}"}

# Date filtering function for FRED and BLS data
def filter_last_10_years(data, date_field="date"):
    """Filter the data to only include entries from the last 10 years."""
    ten_years_ago = datetime.now() - timedelta(days=10*365)
    return [entry for entry in data if datetime.strptime(entry[date_field], "%Y-%m-%d") > ten_years_ago]


# Generic handler for any endpoint fetching data from S3
def handle_s3_request(s3_key: str):
    data = fetch_json_from_s3(s3_key)
    if "error" in data:
        return create_json_response(
            {'message': 'Internal server error', 'details': data["error"]}, 
            status_code=500
        )
    return create_json_response(data)

# Combine data from multiple S3 keys
def combine_data_from_s3(keys: list):
    combined_data = []
    for key in keys:
        data = fetch_json_from_s3(key)
        if "error" not in data:
            combined_data.extend(data)  # Assuming the data is a list of dictionaries
    return combined_data

#########################
# FRED specific endpoints
#########################

@app.route('/fred/{indicator}', methods=['GET'], cors=True)
def get_fred_data(indicator):
    s3_key = FRED_S3_KEYS.get(indicator)

    if s3_key:
        return handle_s3_request(s3_key)
    else:
        return create_json_response({'message': f'Indicator {indicator} not found'}, status_code=404)

# Combine all FRED data into one response
@app.route('/fred', methods=['GET'], cors=True)
def get_combined_fred_data():
    combined_fred_data = combine_data_from_s3(FRED_S3_KEYS.values())
    return create_json_response(combined_fred_data)

#########################
# BLS specific endpoints
#########################

@app.route('/bls/{indicator}', methods=['GET'], cors=True)
def get_bls_data(indicator):
    s3_key = BLS_S3_KEYS.get(indicator)

    if s3_key:
        return handle_s3_request(s3_key)
    else:
        return create_json_response({'message': f'Indicator {indicator} not found'}, status_code=404)

# Combine all BLS data into one response
@app.route('/bls', methods=['GET'], cors=True)
def get_combined_bls_data():
    combined_bls_data = combine_data_from_s3(BLS_S3_KEYS.values())
    return create_json_response(combined_bls_data)

#########################
# Generic category-based endpoint (Non-FRED, Non-BLS)
#########################

@app.route('/{category}/{key}', methods=['GET'], cors=True)
@app.route('/{category}', methods=['GET'], cors=True)
def get_generic_data(category, key=None):
    if category == 'articles':
        articles = fetch_json_from_s3('articles.json')
        if "error" in articles:
            return create_json_response({'message': 'Internal server error', 'details': articles["error"]}, status_code=500)

        # If key is provided, it's an article ID, so filter by ID
        if key:
            article = next((a for a in articles if a['id'] == key), None)
            if article:
                return create_json_response(article)
            else:
                return create_json_response({'message': f'Article with id {key} not found'}, status_code=404)

        # If no key, return all articles
        return create_json_response(articles)
    else:
        # Look for the category in GENERIC_S3_KEYS when key is None
        if key is None:
            s3_key = GENERIC_S3_KEYS.get(category)
        else:
            return create_json_response({'message': f'Invalid request'}, status_code=400)

    if s3_key:
        return handle_s3_request(s3_key)
    else:
        return create_json_response({'message': f'Category {category} not found'}, status_code=404)

@app.route('/stocks/daily_ohlc/{symbol}', methods=['GET'], cors=True)
def get_daily_ohlc(symbol):
    key = f'api/stock_daily_bar/{symbol.upper()}.json'
    data = fetch_json_from_s3(key)
    if "error" in data:
        return create_json_response(
            {'message': f'Stock data for {symbol.upper()} not found', 'details': data["error"]},
            status_code=404
        )
    
    # Get query parameters
    request = app.current_request
    query_params = request.query_params

    # Initialize date filters
    start_date = None
    end_date = None

    # Parse 'from' and 'to' query parameters
    if query_params:
        from_str = query_params.get('from')
        to_str = query_params.get('to')

        date_format = "%Y-%m-%d"

        if from_str:
            try:
                start_date = datetime.strptime(from_str, date_format).date()
            except ValueError:
                return create_json_response(
                    {'message': "Invalid 'from' date format. Expected YYYY-MM-DD."},
                    status_code=400
                )

        if to_str:
            try:
                end_date = datetime.strptime(to_str, date_format).date()
            except ValueError:
                return create_json_response(
                    {'message': "Invalid 'to' date format. Expected YYYY-MM-DD."},
                    status_code=400
                )

    # Filter data by date if filters are provided
    if start_date or end_date:
        filtered_data = []
        for record in data:
            # Parse the date in each record
            try:
                record_date = datetime.strptime(record['date'], "%Y-%m-%d").date()
            except ValueError:
                continue  # Skip records with invalid date format

            # Apply the date filters
            if start_date and record_date < start_date:
                continue
            if end_date and record_date > end_date:
                continue
            filtered_data.append(record)
        data = filtered_data

    return create_json_response(data)


#########################
# POST subscription endpoint
#########################

@app.route('/subscribe', methods=['POST'], cors=True)
def subscribe_user():
    request = app.current_request
    body = request.json_body
    
    # Validate the input data
    name = body.get('name')
    email = body.get('email')
    subscribe_to_email_list = body.get('subscribe', False)  # Default to False if not provided
    
    if not name or not email:
        return create_json_response({'message': 'Name and email are required.'}, status_code=400)
    
    # Fetch existing subscription data from S3
    subscriptions = fetch_json_from_s3(SUBSCRIPTION_KEY)
    if "error" in subscriptions:
        return create_json_response({'message': 'Internal server error', 'details': subscriptions['error']}, status_code=500)
    
    # Add the new subscription
    new_subscription = {
        'name': name,
        'email': email,
        'subscribe': subscribe_to_email_list
    }
    
    if isinstance(subscriptions, list):
        subscriptions.append(new_subscription)
    else:
        subscriptions = [new_subscription]

    # Upload updated subscription list to S3
    upload_error = upload_json_to_s3(SUBSCRIPTION_KEY, subscriptions)
    if upload_error:
        return create_json_response({'message': 'Internal server error', 'details': upload_error['error']}, status_code=500)

    return create_json_response({'message': 'Subscription successful.'}, status_code=200)