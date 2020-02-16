import boto3
import os

def get_password_from_source(password):
    if password.startswith('ssm://'):
        parameter_name = password.split('ssm://')[-1]
        ssm = boto3.client('ssm', region_name="us-west-2")
        response = ssm.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        return response['Parameter']['Value']
    elif password.startswith('file://'):
        password_file = password.split('file://')[-1]
        f = open(os.path.abspath(password_file), 'r')
        p = f.read()
        f.close()
        return p
    else:
        return password