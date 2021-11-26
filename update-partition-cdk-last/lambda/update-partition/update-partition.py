"""
This lambda function can is triggered by an event source mapping from SQS.
The payload should be delivered to the queue via a subscription to an SNS
topic from the Data Ingestion Hub (DIH), however it retains the ability to
be triggered directly with a payload into the SQS queue for testing.

The json payload structure required for processing should be:

{
  "notify_type": "dih_file_create_success",
  "feed": "NYK_SDW_2#WH_SOURCE_SYSTEM_DIM",
  "publish_ts": "20211118 11:06:40",
  "partition_value_list": ["2021-09-13"],
  "rec_count": "1956678",
  "success_flg": "Y",
  "partition_key": ["dw_bus_dt"],
  "partition_prefix": "s3://dev1-tes-cross1-225166083024-eu-west-1/rainstorworm/sdw/NYK_SDW_2/WH_SOURCE_SYSTEM_DIM/dw_bus_dt=2021-09-13"
} 

In this example, the value for "feed" is a composite of the database and table 
(<DATABASE_NAME>#<TABLE_NAME>).

The function is also mandated to run as VPC attached and communicate with other 
AWS services through VPC endpoints.  Communication to the glue service requires
the boto3 client to be created with the DNS name of the endpoint.

On successful execution the new partition with be created and a success 
response is submitted back to DIH.  If the partition is already present in
the table (i.e. the trigger payload was a duplicate), a success response
is sent to DIH.  If the lambda fails to execute successfully, the built in
retry logic for lambda will retry 3 times and then push the message to the
dead letter queue.  The dead letter queue message then triggers the dead 
letter lambda function to send a failure response to DIH.

"""

import boto3
import os
import copy
import logging
import json
import traceback

log = logging.getLogger()
log.setLevel('INFO')

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

GLUE_ENDPOINT_DNS = os.environ.get('GLUE_ENDPOINT_DNS')
glue_url = f'https://{GLUE_ENDPOINT_DNS}'
glue_client = boto3.client('glue', endpoint_url=glue_url)

def sns_publish(topic_arn: str, subject: str, message: str, region: str):
    """Function to publish SNS message with the DIH required attributes."""
    try:

        log.info(f"Publish to SNS {json.dumps({ 'topic_arn': topic_arn, 'subject': subject, 'message': message, 'region': region})}")

        sns_client= boto3.client('sns', region_name=region)
        
        message_attrib = {}
        for key in message.keys():
            if isinstance(message.get(key), list):
                string_value = ','.join(message.get(key))
            else:
                string_value = message.get(key)
            message_attrib[key] = { 'DataType': 'String','StringValue': json.dumps(string_value)}

        sns_client.publish(
            TargetArn=topic_arn,
            Subject=subject,
            Message=json.dumps(message),
            MessageAttributes=message_attrib
        )

    except Exception as e:
        log.info('Publish to SNS Failed.')
        log.error(e)
        log.error(traceback.format_exc())

def handler(event, context):

    log.info(f'event: {event}')

    for record in event.get('Records'):
    
        body = json.loads(record.get('body'))
        log.info(f'body: {body}')

        if body.get('Type') == 'Notification':
            body = json.loads(body.get('Message'))

        if body.get('notify_type') != "dih_file_create_success":
            return

        try:
            region = record.get('awsRegion')            
            source_key = body.get('partition_prefix')
            database_name, table_name = body.get('feed').split('#')

            log.info(f'Database: {database_name} Table: {table_name}')

            log.info('Getting Table Details.')
            get_table_response = glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )

            storage_descriptor = get_table_response['Table']['StorageDescriptor']

            custom_storage_descriptor = copy.deepcopy(storage_descriptor)
            custom_storage_descriptor['Location'] = source_key

            log.info(f'custom_storage_descriptor {custom_storage_descriptor}') 

            log.info(f"Create Partition API Call {json.dumps({ 'DatabaseName': database_name, 'TableName': table_name, 'PartitionKeys': body.get('partition_key'), 'PartitionInput': {'Values': body.get('partition_value_list'),'StorageDescriptor': custom_storage_descriptor}})}")
            create_partition_response = glue_client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput={
                    'Values': body.get('partition_value_list'),
                    'StorageDescriptor': custom_storage_descriptor
                }
            )
            log.info(create_partition_response)
            log.info('Glue partition created successfully.') 
            sns_body = body
            sns_body['notify_type'] = 'dih_glue_add_ptn_success'
            
            sns_publish(SNS_TOPIC_ARN, 'DIH Glue Catalog Partition Refresh message', sns_body, region)

        except Exception as e:

            if type(e).__name__ == 'AlreadyExistsException':
                log.info('Glue partition already present.')
                sns_body = body
                sns_body['notify_type'] = 'dih_glue_add_ptn_success'
            
                sns_publish(SNS_TOPIC_ARN, 'DIH Glue Catalog Partition Refresh message', sns_body, region)

            else:
                log.info('Glue partition creation failed.')
                log.error(e)
                log.error(traceback.format_exc())
                raise e
