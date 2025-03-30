from flask import Flask, request, jsonify
import boto3
import json
import io
import re
import uuid
from datetime import datetime
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

app = Flask(__name__)

@app.route('/convert', methods=['POST'])
def convert_hudi_to_iceberg():
    try:
        data = request.get_json()
        table_path = data.get('table_path')
        aws_access_key = data.get('aws_access_key')
        aws_secret_access_key = data.get('aws_secret_access_key')

        if not table_path:
            return jsonify({'error': 'Missing table_path'}), 400

        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key
        ) if aws_access_key and aws_secret_access_key else boto3.client('s3')

        # Extract bucket and prefix
        s3_path = table_path.replace('s3://', '').replace('\\', '/')
        bucket_name = s3_path.split('/')[0]
        table_prefix = '/'.join(s3_path.split('/')[1:])
        hoodie_dir_prefix = f"{table_prefix}/.hoodie" if table_prefix else ".hoodie"

        # Validate S3 path
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            return jsonify({'error': str(e)}), 400

        return jsonify({'message': 'Connection successful', 'bucket': bucket_name, 'prefix': table_prefix}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
