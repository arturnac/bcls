from __future__ import print_function
import json
import boto3
import botocore
import sys
import uuid
import logging
import urllib3

http = urllib3.PoolManager()
SUCCESS = "SUCCESS"
FAILED = "FAILED"


"""
This lambda can be invoked directly or through cloudformation as custom resource. 
It looks for the 'RequestType' key in the input event. If the lambda is triggered 
by the CF stack as custom resource then this RequestType will have the values as 
Create/Delete/Update. While invoking this lambda directly, it will not have the 
RequestType paramter and the input will be as mentioned below for the direct lamda 
invocation scenario:

Scenario 1: Following are the parameters for invoking this lambda directly through 
event:
This lambda function takes the following parameter as input and then grants
the lakeformation read access on the database and table list which is passed
as the input parameters as mentioned below. 
1. database_name       : (Mandatory) Glue Database name, which needs to be shared with
                         the consumer AWS account. This lambda will grant the DESCRIBE
                         access through lakeformation to the consumer AWS account.
2. table_name_list     : (Mandatory) List of the tables which needs to be shared with
                         the consumer AWS account. This lambda will grant the ALL
                         access through lakeformation to the consumer AWS account.
3. external_account  : (Mandatory) AWS account number for the producer or consumer.

Based on the input event parameters, This lambda will grant the DESCRIBE access 
on the Glue database to consumer AWS account. Then it will grant the ALL access
to the tables passed as list to the consumer AWS account. 

Below example shows a sample test event input for this lambda function:
{
  "external_account": "12345678900",
  "database_name": "testdb",
  "table_name_list": [
    "temp",
    "temp2"
  ]
}

Scenario 2: Following are the parameters for invoking this lambda through cloudformation
as custom resource:
When invoking this lambda as a custom resource from the CF stack, CF will send the 
input event with RequestType as 'Create' or 'Update' for creating/updating exsting
stack. While deleting the stack it will send RequestType as 'Delete' in the event. 
The input to the lambda should be passed as Input parameter while invoking this lambda
as custom resource and this shuold have the same as input event for scenario 1 mentioned
above:

Below is an example for inovking this lambda from CF:

Resources:
  primerinvoke1:
    Type: AWS::CloudFormation::CustomResource
    Version: "1.0"
    Properties:
      ServiceToken: arn:aws:lambda:us-east-1:123456789012:function:CustomRamShareResource
      Input:  {"external_account": "123456789011","database_name": "testdb","table_name_list": ["temp","temp2"]}


Once the lakeformation access grants are provided for another AWS producer/consumer account,
then the lakeformation admin on the producer/consumer account needs to accept the RAM share
invite. After that a local database needs to be created by the appropriate user role
and resource link needs to be created for the shared tables.



"""


log = logging.getLogger('deploy.cf.create_or_update')

def lambda_handler(event, context):
    
    batch_grant_permissions=""
    Table_Bulk_Entries=[]
    
    try:
        if "RequestType" in event:
            if event["RequestType"] == "Delete":
                send(event, context, SUCCESS, {"Status":"Skipping for Delete request"});
                return;
        
            input_event=event['ResourceProperties']['Input']
            log.info("Cloudformation custom resource inovcation")
        else:
            input_event=event
            log.info("Custom resource inovcation")
    
        log.info(input_event)

        if "external_account" in input_event and input_event["external_account"]:
            external_account=input_event['external_account']
        
        if "database_name" in input_event and input_event['database_name']:
            database_name=input_event['database_name']
    
        if "table_name_list" in input_event and input_event['table_name_list']:
            table_name_list=input_event['table_name_list']
    
        log.info("Starting custom lakeformation access grants to account:" + external_account
        + " for database:" + database_name + " and the tables:" + str(table_name_list))
    
        database_details={
            'Name' : database_name
        }
    
        client = boto3.client('lakeformation')
    
    
        log.info("Granting access on database:"+ database_name + " to account:" + external_account)
        database_grant_response = client.grant_permissions(Principal={
            'DataLakePrincipalIdentifier': external_account
            },
            Resource={
                'Database': database_details
            },
            Permissions=['DESCRIBE'],
            PermissionsWithGrantOption=['DESCRIBE',]
            )
        log.info(database_grant_response)
        log.info("Database grant completed successfully.")
    
        
        for table_name in table_name_list:
            table_entry={
                'Id': str(uuid.uuid4()),
                'Principal': {
                        'DataLakePrincipalIdentifier': external_account
                    },
                'Resource': {
                    'Table': {
                        'Name' : table_name,
                        'DatabaseName' : database_name
                        }
                },
                'Permissions': ['ALL'],
                'PermissionsWithGrantOption': ['ALL']
            }
            Table_Bulk_Entries.append(table_entry)

        log.info("Granting access on Tables:"+ str(table_name) + " to account:" + external_account)
        batch_grant_permissions = client.batch_grant_permissions(
            Entries=Table_Bulk_Entries
            )
    
        log.info(batch_grant_permissions)
    except Exception as ex:
        log.error("Glue database and table lakeformation grants failed.")
        send(event, context, FAILED, ex)
        log.error(ex)
    
    if "RequestType" in event:
        send(event, context, SUCCESS, batch_grant_permissions)
        return;
    else:
        return {
            'statusCode': 200,
            'body': json.dumps(batch_grant_permissions)
        }

def send(event, context, responseStatus, responseData, physicalResourceId=None, noEcho=False, reason=None):
    responseUrl = event['ResponseURL']

    print(responseUrl)

    responseBody = {
        'Status' : responseStatus,
        'Reason' : reason or "See the details in CloudWatch Log Stream: {}".format(context.log_stream_name),
        'PhysicalResourceId' : physicalResourceId or context.log_stream_name,
        'StackId' : event['StackId'],
        'RequestId' : event['RequestId'],
        'LogicalResourceId' : event['LogicalResourceId'],
        'NoEcho' : noEcho,
        'Data' : responseData
    }

    json_responseBody = json.dumps(responseBody)

    print("Response body:")
    print(json_responseBody)

    headers = {
        'content-type' : '',
        'content-length' : str(len(json_responseBody))
    }

    try:
        response = http.request('PUT', responseUrl, headers=headers, body=json_responseBody)
        print("Status code:", response.status)


    except Exception as e:

        print("send(..) failed executing http.request(..):", e)