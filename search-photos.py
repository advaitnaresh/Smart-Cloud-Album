import json
import boto3
import urllib3
import os

# --- CONFIGURATION (UPDATED) ---
OS_ENDPOINT = 'search-photos-rlkgaxoxafiswtgnt7sqyvwuay.us-east-1.es.amazonaws.com' 
OS_USER = 'admin'
OS_PASS = 'Admin123!'
BOT_ID = 'S1WEY6V3PK'
BOT_ALIAS_ID = 'TSTALIASID'

# CRITICAL CHANGE: Define the specific unique bucket name to filter by
TARGET_BUCKET_NAME = 'photos-bucket-aj4700-a3' 
# ----------------------------------------------------------------------

def lambda_handler(event, context):
    http = urllib3.PoolManager()
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,GET'
    }

    try:
        clean_endpoint = OS_ENDPOINT.replace("https://", "").replace("/", "")
        client = boto3.client('lexv2-runtime')

        q = event.get('queryStringParameters', {}).get('q', 'dogs')
        
        # 1. Lex Disambiguation
        lex_resp = client.recognize_text(botId=BOT_ID, botAliasId=BOT_ALIAS_ID, localeId='en_US', sessionId='test', text=q)
        slots = lex_resp.get('sessionState', {}).get('intent', {}).get('slots', {})
        keyword = q
        if slots and slots.get('keywords') and slots['keywords'].get('value'):
            keyword = slots['keywords']['value']['originalValue']
            
        # 2. Search OpenSearch
        url = f"https://{clean_endpoint}/photos/_search?q=labels:{keyword}"
        auth_header = urllib3.make_headers(basic_auth=f"{OS_USER}:{OS_PASS}")
        
        response = http.request('GET', url, headers=auth_header)
        raw_response = response.data.decode('utf-8')
        data = json.loads(raw_response)
        
        # 3. Crash-Proof & Filtered Parsing
        results = []
        unique_urls = set()

        for hit in data.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            bucket = source.get('bucket')
            key = source.get('objectKey')
            labels = source.get('labels', [])
            
            # --- FILTER LOGIC (Include ONLY the old manual bucket) ---
            if bucket != TARGET_BUCKET_NAME:
                continue
            # --- END FILTER LOGIC ---
            
            img_url = f"https://{bucket}.s3.amazonaws.com/{key}"
            
            if img_url in unique_urls:
                continue

            unique_urls.add(img_url)
            
            if bucket and key:
                results.append({
                    "url": img_url,
                    "labels": labels
                })
        
        # 4. Return Success
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(results)
        }

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return {
            'statusCode': 500, 
            'headers': headers,
            'body': json.dumps({"error": "Final Processing Failed", "details": str(e)})
        }