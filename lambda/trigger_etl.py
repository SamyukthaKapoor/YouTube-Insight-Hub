import json
import boto3
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize boto3 clients
s3 = boto3.client('s3')
glue = boto3.client('glue')

def lambda_handler(event, context):
    """
    Lambda function triggered by S3 event when files are uploaded to landing zone.
    This function checks if both CSV and JSON files are present and triggers the Glue ETL job.
    """
    try:
        # Extract bucket name and file key from the S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"File uploaded: s3://{bucket}/{key}")
        
        # Check if required files exist in the landing zone
        required_files = {
            'csv_file': False,
            'json_file': False
        }
        
        # List objects in the landing zone
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix='landing-zone/'
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                if file_key.endswith('.csv'):
                    required_files['csv_file'] = True
                    csv_file_key = file_key
                elif file_key.endswith('.json'):
                    required_files['json_file'] = True
                    json_file_key = file_key
        
        # If both files exist, trigger Glue job
        if all(required_files.values()):
            logger.info("Both CSV and JSON files found. Triggering Glue ETL job.")
            
            # Create a copy of the files to the data-zone-2 for backup
            s3.copy_object(
                Bucket=bucket,
                CopySource=f"{bucket}/{csv_file_key}",
                Key=f"data-zone-2/{os.path.basename(csv_file_key)}"
            )
            
            s3.copy_object(
                Bucket=bucket,
                CopySource=f"{bucket}/{json_file_key}",
                Key=f"data-zone-2/{os.path.basename(json_file_key)}"
            )
            
            # Trigger Glue job
            glue_response = glue.start_job_run(
                JobName='yt-transform-job',
                Arguments={
                    '--landing_zone_bucket': bucket,
                    '--landing_zone_path': 'landing-zone/',
                    '--csv_file': os.path.basename(csv_file_key),
                    '--json_file': os.path.basename(json_file_key),
                    '--transformed_zone_bucket': bucket,
                    '--transformed_zone_path': 'transformed-zone/'
                }
            )
            
            logger.info(f"Glue job triggered successfully: {glue_response['JobRunId']}")
            
            return {
                'statusCode': 200,
                'body': json.dumps(f"Glue job triggered: {glue_response['JobRunId']}")
            }
        else:
            logger.info("Waiting for both CSV and JSON files to be uploaded.")
            return {
                'statusCode': 200,
                'body': json.dumps("Waiting for both CSV and JSON files.")
            }
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
