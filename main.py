import os
import requests
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
ITEM_ID = os.getenv('ITEM_ID')
DATABASE_URL = os.getenv('DATABASE_URL')
TARGET_ENDPOINT = os.getenv('TARGET_ENDPOINT')

def get_api_key():
    try:
        print("Requesting API Key with CLIENT_ID and CLIENT_SECRET")
        response = requests.post('https://api.pluggy.ai/auth', json={
            'clientId': CLIENT_ID,
            'clientSecret': CLIENT_SECRET
        })
        print("Response Status Code:", response.status_code)
        print("Response Body:", response.text)
        response.raise_for_status()
        token_data = response.json()
        if 'apiKey' in token_data:
            return token_data['apiKey']
        else:
            print("Error: 'apiKey' not found in the response.")
            print(token_data)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error getting API Key: {e}")
        return None

def get_accounts(api_key, item_id):
    try:
        print(f"Getting accounts for item: {item_id}")
        response = requests.get(f'https://api.pluggy.ai/accounts?itemId={item_id}', headers={
            'X-API-KEY': api_key
        })
        print("Response Status Code:", response.status_code)
        print("Response Body:", response.text)
        response.raise_for_status()
        return response.json()['results']
    except requests.exceptions.RequestException as e:
        print(f"Error getting accounts: {e}")
        return None

def get_transactions(api_key, account_id):
    try:
        print(f"Getting transactions for account: {account_id}")
        response = requests.get(f'https://api.pluggy.ai/transactions?accountId={account_id}', headers={
            'X-API-KEY': api_key
        })
        print("Response Status Code:", response.status_code)
        print("Response Body:", response.text)
        response.raise_for_status()
        return response.json()['results']
    except requests.exceptions.RequestException as e:
        print(f"Error getting transactions: {e}")
        return None

def get_item_data(api_key, item_id):
    accounts = get_accounts(api_key, item_id)
    if not accounts:
        return None

    for account in accounts:
        transactions = get_transactions(api_key, account['id'])
        account['transactions'] = transactions if transactions else []

    return {'accounts': accounts}

def save_data_to_database(data):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        for account in data['accounts']:
            for transaction in account['transactions']:
                cur.execute(sql.SQL("""
                    INSERT INTO transactions (account_id, transaction_id, amount, description)
                    VALUES (%s, %s, %s, %s)
                """), (account['id'], transaction['id'], transaction['amount'], transaction['description']))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error saving data to database: {e}")

def send_data_to_endpoint(data):
    try:
        response = requests.post(TARGET_ENDPOINT, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error sending data to endpoint: {e}")

def main():
    api_key = get_api_key()
    if not api_key:
        return
    
    item_data = get_item_data(api_key, ITEM_ID)
    if not item_data:
        return

    save_data_to_database(item_data)
    send_data_to_endpoint(item_data)

if __name__ == "__main__":
    main()