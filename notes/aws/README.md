# RedShift Creation

This project creates a Redshift Cluster on AWS. 

It's for note taking purposes so backend was saved in S3 without using Pulumi Cloud.

## Deploying

  0. Setup pulumi, aws, dotnet core, and setup your backend to pulumi.
  1. Ensure an AWS_PROFILE is set or valid AWS Credentials are available in your shell.
  2. `pulumi config set aws:region <value>`
  2. `pulumi up`