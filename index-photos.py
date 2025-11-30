import json
import boto3
import time
import urllib3
import urllib.parse

# --- CONFIGURATION ---
# Your specific OpenSearch Endpoint
OS_ENDPOINT = 'search-photos-rlkgaxoxafiswtgnt7sqyvwuay.us-east-1.es.amazonaws.com' 

# Authentication (For Fine-Grained Access Control)
OS_USER = 'admin'
OS_PASS = 'Admin123!' 
# ---------------------

def lambda_handler(event, context):
    http = urllib3.PoolManager()
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition')
    
    print("Event received:", json.dumps(event))
    
    try:
        # 1. Get Bucket and Key from Event
        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        # Decode filename (e.g. "My%20Photo.jpg" -> "My Photo.jpg")
        key = urllib.parse.unquote_plus(s3_record['object']['key'])
        
        print(f"Processing image: {key} from bucket: {bucket_name}")

        # 2. Call Rekognition to detect labels
        rekog_response = rekognition.detect_labels(
            Image={'S3Object': {'Bucket': bucket_name, 'Name': key}},
            MaxLabels=10, MinConfidence=75
        )
        labels = [l['Name'] for l in rekog_response['Labels']]
        print(f"AI Labels detected: {labels}")
        
        # 3. Retrieve Custom Labels from S3 Metadata
        meta = s3.head_object(Bucket=bucket_name, Key=key)
        # S3 metadata keys are returned in lowercase
        custom_labels = meta.get('Metadata', {}).get('customlabels', '')
        
        if custom_labels:
            custom_list = [x.strip() for x in custom_labels.split(',')]
            labels.extend(custom_list)
            print(f"Custom Labels added: {custom_list}")

        # 4. Prepare JSON Object
        doc = {
            "objectKey": key,
            "bucket": bucket_name,
            "createdTimestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
            "labels": labels
        }
        
        # 5. Send to OpenSearch (with Authentication)
        url = f"https://{OS_ENDPOINT}/photos/_doc"
        
        # Create Basic Auth Header
        auth_header = urllib3.make_headers(basic_auth=f"{OS_USER}:{OS_PASS}")
        headers = {'Content-Type': 'application/json'}
        headers.update(auth_header)
        
        response = http.request(
            'POST',
            url,
            body=json.dumps(doc).encode('utf-8'),
            headers=headers
        )
        
        response_data = response.data.decode('utf-8')
        print("OpenSearch Index Response:", response_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Image Indexed Successfully')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }