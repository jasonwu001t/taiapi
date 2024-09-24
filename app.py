from chalice import Chalice, Response
import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime

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

@app.route('/us_treasury_yield', methods=['GET'], cors=True)
def get_categories():
    categories = fetch_json_from_s3('api/treasury_yield_all.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

#################  BLS ############################
@app.route('/unemployment_rate', methods=['GET'], cors=True)  #data from BLS
def get_categories():
    categories = fetch_json_from_s3('api/bls/unemployment_rate.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

@app.route('/nonfarm_payroll', methods=['GET'], cors=True)  #data from BLS
def get_categories():
    categories = fetch_json_from_s3('api/bls/nonfarm_payroll.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

@app.route('/us_avg_weekly_hours', methods=['GET'], cors=True)  #data from BLS
def get_categories():
    categories = fetch_json_from_s3('api/bls/us_avg_weekly_hours.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

@app.route('/us_job_opening', methods=['GET'], cors=True)  #data from BLS
def get_categories():
    categories = fetch_json_from_s3('api/bls/us_job_opening.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)
###################################################


# Endpoint to get all articles or filter by id
@app.route('/articles', methods=['GET'], cors=True)
@app.route('/articles/{id}', methods=['GET'], cors=True)
def get_articles(id=None):
    articles = fetch_json_from_s3('articles.json')
    if "error" in articles:
        return create_json_response({'message': 'Internal server error', 'details': articles["error"]}, status_code=500)

    if id:
        # Filter articles by id
        article = next((a for a in articles if a['id'] == id), None)
        if article:
            return create_json_response(article)
        else:
            return create_json_response({'message': f'Article with id {id} not found'}, status_code=404)

    return create_json_response(articles)

# Endpoint to get all chart data or filter by id
@app.route('/chart', methods=['GET'], cors=True)
@app.route('/chart/{id}', methods=['GET'], cors=True)
def get_chart_data(id=None):
    chart_data = fetch_json_from_s3('chart_data.json')
    if "error" in chart_data:
        return create_json_response({'message': 'Internal server error', 'details': chart_data["error"]}, status_code=500)

    if id:
        # Filter chart data by id
        chart = next((c for c in chart_data if c['id'] == id), None)
        if chart:
            return create_json_response(chart)
        else:
            return create_json_response({'message': f'Chart with id {id} not found'}, status_code=404)

    return create_json_response(chart_data)

# Endpoint to get all categories from S3
@app.route('/categories', methods=['GET'], cors=True)
def get_categories():
    categories = fetch_json_from_s3('categories.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

# Endpoint to get all indicators from S3
@app.route('/indicators', methods=['GET'], cors=True)
def get_indicators():
    indicators = fetch_json_from_s3('indicators.json')
    if "error" in indicators:
        return create_json_response({'message': 'Internal server error', 'details': indicators["error"]}, status_code=500)
    return create_json_response(indicators)

# Updated endpoint to get daily OHLC data with date filters
@app.route('/stocks/daily_ohlc/{symbol}', methods=['GET'], cors=True)
def get_daily_ohlc(symbol):
    if symbol:
        key = f'api/stock_daily_bar/{symbol.upper()}.json'
        data = fetch_json_from_s3(key)
        if "error" in data:
            return create_json_response(
                {'message': f'Stock data for {symbol.upper()} not found', 'details': data["error"]},
                status_code=404
            )
        else:
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
    else:
        return create_json_response({'message': 'Symbol is required'}, status_code=400)