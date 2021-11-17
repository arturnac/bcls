

# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import (core,
                     aws_lambda as lambda_,
                     aws_iam as iam_)


class CustomRamShareResourceStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        handler = lambda_.Function(self, "custom_ram_share_deploy",
                    function_name="CustomRamShareResource",
                    runtime=lambda_.Runtime.PYTHON_3_6,
                    code=lambda_.Code.from_asset("resources"),
                    handler="custom_ram_share_resource.lambda_handler",
                    # The lambda timeout needs to be checked, how long we can let it run
                    timeout=core.Duration.seconds(300)
                    #TODO
                    #Lambda execution role. This is the role that will be assumed by the function upon execution. 
                    #This role name needs to be changed with the role that we can use 
                    #for granting lakeformation access and RAM share. This role should have the 
                    # required access grants for lakeformation/RAM. 

        )
