from datetime import datetime
import json
import logging
import requests
import boto3

from airflow import DAG
# Updated to modern Airflow 3 SDK imports to eliminate deprecation warnings
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

api_base_url = 'https://jsonplaceholder.typicode.com'
kinesis_client = boto3.client('kinesis')   

## Setting up incremental user id for next call
def _set_api_user_id(**context):
    try:
        current_id = int(Variable.get("api_user_id", default=-1))
        logger.info(f'Current api_user_id:: {current_id}')
        
        if current_id == -1 or current_id >= 10:
            next_id = 1
        else:
            next_id = current_id + 1
            
        # Airflow 3 / Pydantic requires value to be a string
        Variable.set(key="api_user_id", value=str(next_id)) 
        return f"Latest api user id {next_id} set successfully"
    except Exception as e:
        logger.error(f'ERROR WHILE SETTING UP userId param value:: {e}')
        raise

def _extract_userposts(**context):
    try:
        new_api_user_id = int(Variable.get("api_user_id", default=1))
        logger.info(f'Fetching posts for new_api_user_id:: {new_api_user_id}')
        
        response = requests.get(f'{api_base_url}/posts?userId={new_api_user_id}')
        response.raise_for_status()
        user_posts = response.json()
        
        logger.info(f'api data||user_posts count:: {len(user_posts)}')
        context['task_instance'].xcom_push(key='user_posts', value=user_posts)
        return user_posts
    except Exception as e:
        logger.error(f'ERROR WHILE FETCHING USER POSTS API DATA:: {e}')
        raise

def _process_user_posts(**context):
    try:
        stream_name = "dea11-dev-west1-kinesis-user-post-data-stream-01"    
        user_posts = context['task_instance'].xcom_pull(task_ids='extract_userposts', key='user_posts')
        new_api_user_id = int(Variable.get("api_user_id", default=1))
        logger.info(f'Retrieved {len(user_posts) if user_posts else 0} posts from XCom')

        if not user_posts:
            logger.warning("No posts found to push to Kinesis.")
            return "No posts to write."

        # Writing data one by one to kinesis data stream
        for user_post in user_posts:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(user_post) + '\n', 
                PartitionKey=str(user_post['userId'])
            )
            logger.info(
                f"Produced record {response['SequenceNumber']} to Shard {response['ShardId']}, "
                f"status code {response['ResponseMetadata']['HTTPStatusCode']}"
            )
    
        return f'Total {len(user_posts)} posts with user id {new_api_user_id} written into kinesis stream `{stream_name}`'
    except Exception as e:
        logger.error(f'ERROR WHILE WRITING USER POSTS TO KINESIS STREAM:: {e}')
        raise


with DAG(
    dag_id='load_api_aws_kinesis', 
    default_args={'owner': 'Sovan'}, 
    tags=["api data load to s3"], 
    start_date=datetime(2023, 9, 24), 
    schedule='@daily', 
    catchup=False
) as dag:

    get_api_userId_params = PythonOperator(
        task_id='get_api_userId_params',
        python_callable=_set_api_user_id
    ) 
    
    extract_userposts = PythonOperator(
        task_id='extract_userposts',
        python_callable=_extract_userposts
    )

    write_userposts_to_stream = PythonOperator(
        task_id='write_userposts_to_stream',
        python_callable=_process_user_posts
    )

    get_api_userId_params >> extract_userposts >> write_userposts_to_stream