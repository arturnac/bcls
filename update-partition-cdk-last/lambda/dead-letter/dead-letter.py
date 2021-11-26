import boto3
import os
import logging
import json
import traceback

log = logging.getLogger()
log.setLevel('INFO')

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def sns_publish(topic_arn: str, subject: str, message: str, region: str):
    """Function to publish SNS message with the DIH required attributes."""
    try:
        log.info(f"Publishing to SNS {json.dumps({ 'topic_arn': topic_arn, 'subject': subject, 'message': message, 'region': region})}")

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
    log.info(event)
    for record in event.get('Records'):
        try:                    
            region = record.get('awsRegion')
            log.info(region)
            body = json.loads(record.get('body')) 

            sns_body = body
            sns_body['notify_type'] = 'dih_glue_add_ptn_failure'
            sns_publish(SNS_TOPIC_ARN, 'DIH Glue Catalog Partition Refresh message', sns_body, region )
            
        except Exception as e:
            log.error(e)
            log.error(traceback.format_exc())