import requests

# Define the API URL (replace with your actual URL)
api_url = 'https://bmohn85r9a.execute-api.us-west-2.amazonaws.com/api/subscribe'

# Define the data to be sent in the POST request
data = {
    'name': 'John Doe',
    'email': 'john.doe@example.com',
    'subscribe': True  # or False if the user does not want to subscribe to the email list
}

# Send the POST request
response = requests.post(api_url, json=data)

# Check if the request was successful
if response.status_code == 200:
    print('Subscription successful:', response.json())
else:
    print('Failed to subscribe:', response.status_code, response.text)
