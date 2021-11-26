from aws_cdk import (
    core as cdk,
    aws_sqs as sqs,
    aws_lambda as aws_lambda,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kms as kms,
    aws_lambda_event_sources as aws_lambda_event_sources,
)

#from aws_cdk import core


class UpdatePartitionCdkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)       

        #~~~~~~~~~~~~~~
        # PARAMETERS
        #~~~~~~~~~~~~~~

        VpcId = cdk.CfnParameter(self, 'VpcId', type='String', 
            description='AWS VPC in which to place the Update-Partition Module infrastructure'
        )

        glue_endpoint_sg_id = cdk.CfnParameter(self, 'glue_endpoint_sg_id', type='String', 
            description='(Optional) Security group of the existing glue vpc endpoint, leave default if non-existent and one will be created',
        ) 
        glue_endpoint_dns = cdk.CfnParameter(self, 'glue_endpoint_dns', type='String', 
            description='(Optional) DNS Name of the existing glue vpc endpoint, leave default if non-existent and one will be created e.g. vpce-04b373001e2c062af-urtn0l38.glue.eu-west-2.vpce.amazonaws.com',
        ) 

        sns_endpoint_sg_id = cdk.CfnParameter(self, 'sns_endpoint_sg_id', type='String', 
            description='(Optional) Security group of the existing sns vpc endpoint, leave default if non-existent and one will be created',
        ) 

        sns_notification_topic = cdk.CfnParameter(self, 'sns_notification_topic', type='String',
            description='(Mandatory) SNS Topic which to return success or failure messages'
        )
 
        default_azs = 'eu-west-2a'
        availability_zones = cdk.CfnParameter(self, 'availability_zones', type='String', 
            description='(Optional) Availability Zones',
            default=default_azs
        )

        default_private_subnet_ids = 'subnet-0a8c1b94c026e6837'
        private_subnet_ids = cdk.CfnParameter(self, 'private_subnet_ids',
            description='(Optional) Availability Zones',
            default=default_private_subnet_ids
        )

        update_partition_lambda_name = cdk.CfnParameter(self, 'update_partition_lambda_name', type='String', 
            description='(Optional) Update Partition Lambda Name',
            default='MVP-Glue-Partition-Update-Lambda'
        )

        dead_letter_lambda_name = cdk.CfnParameter(self, 'dead_letter_lambda_name', type='String', 
            description='(Optional) Dead Letter Lambda Name',
            default='MVP-Dead-Letter-Lambda'
        )

        #~~~~~~~~~~~~~~
        # IMPORTS
        #~~~~~~~~~~~~~~

        VpcFromString=ec2.Vpc.from_vpc_attributes(self, 'Vpc', 
            availability_zones=[availability_zones.value_as_string], 
            vpc_id=VpcId.value_as_string,
            private_subnet_ids=[private_subnet_ids.value_as_string],
        )

        #~~~~~~~~~~~~~~
        # LAMBDA KMS
        #~~~~~~~~~~~~~~

        lambda_kms_key = kms.Key(self, 'LambdaKmsKey',
            enabled=True,
            description='KMS Key for use with the Update Partition Module Lambda Functions',
        )
        lambda_kms_key.grant_admin(iam.AccountRootPrincipal())

        #~~~~~~~~~~~~~~
        # LAMBDA
        #~~~~~~~~~~~~~~

        update_partition_lambda_security_group = ec2.SecurityGroup(self, 'UpdatePartitionSG',
            vpc=VpcFromString,
            description='Security Group for the Update Partition Lambda Function',
            security_group_name='UpdatePartitionLambdaSecurityGroup',
            allow_all_outbound=False
        )

        update_partition_lambda_execution_role = iam.Role(self, 'UpdatePartitionRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description='Execution Role for Update Partition Lambda',
            role_name='svc-UpdatePartitionLambda',
            permissions_boundary=f'arn:aws:iam::{cdk.Stack.region}:policy/core-ServiceRolePermissionsBoundary',
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaVPCAccessExecutionRole')]
        )

        update_partition_lambda = aws_lambda.Function(
            self, 'UpdatePartitionLambda',
            function_name=update_partition_lambda_name.value_as_string,
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.from_asset('lambda/update-partition'),
            memory_size=1024,
            timeout=cdk.Duration.seconds(60),
            handler='update-partition.handler',
            security_groups=[update_partition_lambda_security_group],
            role=update_partition_lambda_execution_role,
            environment={
                'SNS_TOPIC_ARN': sns_notification_topic.value_as_string
            },
            environment_encryption=lambda_kms_key,
            vpc=VpcFromString,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)
        )
        update_partition_lambda.add_to_role_policy(iam.PolicyStatement(
             actions=[
                'glue:GetTable',
                'glue:GetPartition',
                'glue:GetPartitions',
                'glue:UpdatePartition',
                'glue:CreatePartition',
                'sns:Publish'
            ],
            resources=['*']
        ))        

        dead_letter_lambda_security_group = ec2.SecurityGroup(self, 'DeadLetterSG',
            vpc=VpcFromString,
            description='Security Group for the Dead Letter Lambda Function',
            security_group_name='DeadLetterLambdaSecurityGroup',
            allow_all_outbound=False
        )

        dead_letter_lambda_execution_role = iam.Role(self, 'DeadLetternRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description='Execution Role for Dead Letter Lambda',
            role_name='svc-DeadLetterLambda',
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaVPCAccessExecutionRole')],
            permissions_boundary=f'arn:aws:iam::{cdk.Stack.region}:policy/core-ServiceRolePermissionsBoundary'
        )

        dead_letter_lambda = aws_lambda.Function(
            self, 'DeadLetterLambda',
            function_name=dead_letter_lambda_name.value_as_string,
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.from_asset('lambda/dead-letter'),
            memory_size=1024,
            timeout=cdk.Duration.seconds(10),
            handler='dead-letter.handler',
            security_groups=[dead_letter_lambda_security_group],
            role=dead_letter_lambda_execution_role,
            environment={
                'SNS_TOPIC_ARN': sns_notification_topic.value_as_string
            },
            environment_encryption=lambda_kms_key,
            vpc=VpcFromString,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)
        )
        dead_letter_lambda.add_to_role_policy(iam.PolicyStatement(
             actions=[
                'sns:Publish'
            ],
            resources=['*']
        ))

        lambda_kms_key.grant_encrypt_decrypt(update_partition_lambda_execution_role)
        lambda_kms_key.grant_encrypt_decrypt(dead_letter_lambda_execution_role)

        #~~~~~~~~~~~~~~
        # ENDPOINTS
        #~~~~~~~~~~~~~~
        
        imported_glue_sg = ec2.SecurityGroup.from_security_group_id(self, 'ImportedGlueSg', glue_endpoint_sg_id.value_as_string)
        imported_glue_sg.add_ingress_rule(update_partition_lambda_security_group, ec2.Port.tcp(443), 'Allow inbound 443 from Update Partition Lambda')
        update_partition_lambda_security_group.add_egress_rule(imported_glue_sg,ec2.Port.tcp(443), 'Allow outbound 443 to Glue Endpoint')

        imported_sns_sg = ec2.SecurityGroup.from_security_group_id(self, 'ImportedSnsSg', sns_endpoint_sg_id.value_as_string)
        imported_sns_sg.add_ingress_rule(update_partition_lambda_security_group, ec2.Port.tcp(443), 'Allow inbound 443 from Update Partition Lambda')
        imported_sns_sg.add_ingress_rule(dead_letter_lambda_security_group, ec2.Port.tcp(443), 'Allow inbound 443 from Dead Letter Lambda')
        update_partition_lambda_security_group.add_egress_rule(imported_sns_sg,ec2.Port.tcp(443), 'Allow outbound 443 to Sns Endpoint')
        dead_letter_lambda_security_group.add_egress_rule(imported_sns_sg,ec2.Port.tcp(443), 'Allow outbound 443 to Sns Endpoint')

        update_partition_lambda.add_environment('GLUE_ENDPOINT_DNS', glue_endpoint_dns.value_as_string)

        #~~~~~~~~~~~~~~
        # SQS KMS
        #~~~~~~~~~~~~~~

        sqs_kms_key = kms.Key(self, 'SqsKmsKey',
            enabled=True,
            description='KMS Key for use with the Update Partition Module Sqs Queues',
        )

        sqs_kms_key.grant_encrypt_decrypt(update_partition_lambda_execution_role)
        sqs_kms_key.grant_encrypt_decrypt(iam.ServicePrincipal('sqs.amazonaws.com'))
        sqs_kms_key.grant_encrypt_decrypt(iam.ServicePrincipal('sns.amazonaws.com'))
        sqs_kms_key.grant_admin(iam.AccountRootPrincipal())

        #~~~~~~~~~~~~~~
        # SQS
        #~~~~~~~~~~~~~~

        dlqueue = sqs.Queue(self, 'UpdatePartitionDlQueue',
            queue_name='MVP-Update-Partition-DLQueue',
            visibility_timeout=cdk.Duration.seconds(120),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=sqs_kms_key

        )
        dlqueue.grant_consume_messages(dead_letter_lambda_execution_role)
        
        queue = sqs.Queue(self, 'UpdatePartitionQueue',
            queue_name='MVP-Update-Partition-Queue',
            visibility_timeout=cdk.Duration.seconds(120),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlqueue
            ),
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=sqs_kms_key
        )
        queue.grant_consume_messages(update_partition_lambda_execution_role) 

        cfn_queue_policy = sqs.CfnQueuePolicy(self, 'UpdatePartitionQueuePolicy',
            queues=[queue.queue_name],
            policy_document=(iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=['sqs:SendMessage'],
                        resources=[queue.queue_arn],
                        principals=[iam.ServicePrincipal('sns.amazonaws.com')],                        
                        conditions={
                            'ArnEquals': {
                                'aws:SourceArn': sns_notification_topic.value_as_string
                            }
                        }
                    )
                ]
            ))
        )

        update_mapping = aws_lambda.EventSourceMapping(self, 'UpdatePartitionLambdaEvtSrc',
            target=update_partition_lambda,
            batch_size=1,
            enabled=True,
            event_source_arn=queue.queue_arn
        )

        dead_letter_mapping = aws_lambda.EventSourceMapping(self, 'DeadLetterLambdaEvtSrc',
            target=dead_letter_lambda,
            batch_size=1,
            enabled=True,
            event_source_arn=dlqueue.queue_arn
        )
