"""
AWS Glue Deployment Bundle Generator
Produces IAM policies, roles, and CloudFormation/Terraform templates
for deploying converted Glue jobs.
"""
import json


def generate_iam_policy(job_name: str, s3_buckets: list, region: str = "us-east-1", account_id: str = "<AWS_ACCOUNT_ID>") -> dict:
    """Generate IAM policy for the Glue job."""
    s3_resources = []
    for bucket in s3_buckets:
        bucket_name = bucket.strip("s3://").split("/")[0] if bucket.startswith("s3://") else bucket
        s3_resources.append(f"arn:aws:s3:::{bucket_name}")
        s3_resources.append(f"arn:aws:s3:::{bucket_name}/*")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GlueServiceAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:*",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                ],
                "Resource": [
                    f"arn:aws:glue:{region}:{account_id}:catalog",
                    f"arn:aws:glue:{region}:{account_id}:database/*",
                    f"arn:aws:glue:{region}:{account_id}:table/*",
                    f"arn:aws:glue:{region}:{account_id}:job/{job_name}*",
                    f"arn:aws:logs:{region}:{account_id}:log-group:/aws-glue/*",
                ],
            },
            {
                "Sid": "S3DataAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ],
                "Resource": s3_resources if s3_resources else ["arn:aws:s3:::<YOUR-BUCKET>", "arn:aws:s3:::<YOUR-BUCKET>/*"],
            },
            {
                "Sid": "GlueScriptsBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                "Resource": [
                    f"arn:aws:s3:::{job_name.lower()}-glue-scripts/*",
                ],
            },
            {
                "Sid": "SecretsManagerAccess",
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                ],
                "Resource": [
                    f"arn:aws:secretsmanager:{region}:{account_id}:secret:*",
                ],
            },
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:PutMetricData",
                ],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "cloudwatch:namespace": "Glue"
                    }
                },
            },
        ],
    }
    return policy


def generate_iam_role(job_name: str, account_id: str = "<AWS_ACCOUNT_ID>") -> dict:
    """Generate IAM Role with trust policy for Glue."""
    role = {
        "RoleName": f"{job_name}-GlueRole",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
        "ManagedPolicyArns": [
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        ],
        "Description": f"IAM role for Glue job: {job_name} (migrated from ADF)",
    }
    return role


def generate_glue_job_config(job_name: str, script_location: str, connections: list = None, extra_jars: list = None) -> dict:
    """Generate AWS Glue Job configuration."""
    job_config = {
        "Name": job_name,
        "Role": f"{job_name}-GlueRole",
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--job-bookmark-option": "job-bookmark-enable",
            "--TempDir": f"s3://{job_name.lower()}-glue-scripts/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://{job_name.lower()}-glue-scripts/spark-logs/",
        },
        "MaxRetries": 1,
        "GlueVersion": "4.0",
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X",
        "Timeout": 60,
    }

    if connections:
        job_config["Connections"] = {"Connections": connections}

    if extra_jars:
        job_config["DefaultArguments"]["--extra-jars"] = ",".join(extra_jars)

    return job_config


def generate_cloudformation_template(job_name: str, policy: dict, role: dict, job_config: dict) -> dict:
    """Generate a CloudFormation template for the Glue deployment."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": f"CloudFormation stack for Glue ETL job: {job_name} (migrated from ADF)",
        "Parameters": {
            "Environment": {
                "Type": "String",
                "Default": "dev",
                "AllowedValues": ["dev", "staging", "prod"],
            },
            "S3BucketName": {
                "Type": "String",
                "Description": "S3 bucket for data and scripts",
            },
        },
        "Resources": {
            "GlueJobRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": {"Fn::Sub": f"{job_name}-${{Environment}}-GlueRole"},
                    "AssumeRolePolicyDocument": role["AssumeRolePolicyDocument"],
                    "ManagedPolicyArns": role["ManagedPolicyArns"],
                },
            },
            "GlueJobPolicy": {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": {"Fn::Sub": f"{job_name}-${{Environment}}-Policy"},
                    "PolicyDocument": policy,
                    "Roles": [{"Ref": "GlueJobRole"}],
                },
            },
            "GlueDatabase": {
                "Type": "AWS::Glue::Database",
                "Properties": {
                    "CatalogId": {"Ref": "AWS::AccountId"},
                    "DatabaseInput": {
                        "Name": {"Fn::Sub": f"{job_name.lower()}_${{Environment}}"},
                        "Description": f"Glue database for {job_name} (migrated from ADF)",
                    },
                },
            },
            "GlueJob": {
                "Type": "AWS::Glue::Job",
                "DependsOn": ["GlueJobRole", "GlueJobPolicy"],
                "Properties": {
                    "Name": {"Fn::Sub": f"{job_name}-${{Environment}}"},
                    "Role": {"Fn::GetAtt": ["GlueJobRole", "Arn"]},
                    "Command": {
                        "Name": "glueetl",
                        "ScriptLocation": {
                            "Fn::Sub": f"s3://${{S3BucketName}}/scripts/{job_name}.py"
                        },
                        "PythonVersion": "3",
                    },
                    "GlueVersion": "4.0",
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "DefaultArguments": job_config.get("DefaultArguments", {}),
                },
            },
            "ScriptsBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": {"Fn::Sub": f"{job_name.lower()}-glue-scripts-${{Environment}}"},
                    "VersioningConfiguration": {"Status": "Enabled"},
                },
            },
        },
        "Outputs": {
            "GlueJobName": {
                "Value": {"Ref": "GlueJob"},
                "Description": "Name of the created Glue job",
            },
            "GlueRoleArn": {
                "Value": {"Fn::GetAtt": ["GlueJobRole", "Arn"]},
                "Description": "ARN of the Glue IAM role",
            },
            "ScriptsBucketName": {
                "Value": {"Ref": "ScriptsBucket"},
                "Description": "S3 bucket for Glue scripts",
            },
        },
    }
    return template


def generate_terraform_template(job_name: str, region: str = "us-east-1") -> str:
    """Generate Terraform HCL for the Glue deployment."""
    tf = f'''# Terraform configuration for Glue ETL: {job_name}
# Migrated from Azure Data Factory

terraform {{
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region = "{region}"
}}

variable "environment" {{
  type    = string
  default = "dev"
}}

variable "s3_bucket_name" {{
  type        = string
  description = "S3 bucket for source data"
}}

# IAM Role for Glue
resource "aws_iam_role" "glue_role" {{
  name = "{job_name}-${{var.environment}}-GlueRole"

  assume_role_policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [{{
      Effect    = "Allow"
      Principal = {{ Service = "glue.amazonaws.com" }}
      Action    = "sts:AssumeRole"
    }}]
  }})
}}

resource "aws_iam_role_policy_attachment" "glue_service" {{
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}}

resource "aws_iam_role_policy" "glue_s3_access" {{
  name = "{job_name}-S3Access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [
      {{
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${{var.s3_bucket_name}}",
          "arn:aws:s3:::${{var.s3_bucket_name}}/*",
          "${{aws_s3_bucket.scripts.arn}}",
          "${{aws_s3_bucket.scripts.arn}}/*"
        ]
      }},
      {{
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "*"
      }}
    ]
  }})
}}

# Scripts bucket
resource "aws_s3_bucket" "scripts" {{
  bucket = "{job_name.lower()}-glue-scripts-${{var.environment}}"
}}

resource "aws_s3_bucket_versioning" "scripts" {{
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration {{
    status = "Enabled"
  }}
}}

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {{
  name = "{job_name.lower()}_${{var.environment}}"
}}

# Glue Job
resource "aws_glue_job" "etl" {{
  name     = "{job_name}-${{var.environment}}"
  role_arn = aws_iam_role.glue_role.arn

  command {{
    name            = "glueetl"
    script_location = "s3://${{aws_s3_bucket.scripts.id}}/scripts/{job_name}.py"
    python_version  = "3"
  }}

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
  max_retries       = 1

  default_arguments = {{
    "--job-language"                          = "python"
    "--job-bookmark-option"                   = "job-bookmark-enable"
    "--TempDir"                               = "s3://${{aws_s3_bucket.scripts.id}}/temp/"
    "--enable-metrics"                        = "true"
    "--enable-continuous-cloudwatch-log"       = "true"
    "--enable-spark-ui"                       = "true"
    "--spark-event-logs-path"                 = "s3://${{aws_s3_bucket.scripts.id}}/spark-logs/"
  }}
}}

output "glue_job_name" {{
  value = aws_glue_job.etl.name
}}

output "glue_role_arn" {{
  value = aws_iam_role.glue_role.arn
}}

output "scripts_bucket" {{
  value = aws_s3_bucket.scripts.id
}}
'''
    return tf


def generate_full_deployment_bundle(job_name: str, s3_buckets: list = None, region: str = "us-east-1", account_id: str = "<AWS_ACCOUNT_ID>") -> dict:
    """Generate the complete deployment bundle (flat dict)."""
    if not s3_buckets:
        s3_buckets = ["<YOUR-BUCKET>"]

    policy = generate_iam_policy(job_name, s3_buckets, region, account_id)
    role = generate_iam_role(job_name, account_id)
    job_config = generate_glue_job_config(
        job_name,
        f"s3://{job_name.lower()}-glue-scripts/scripts/{job_name}.py",
    )
    cfn_template = generate_cloudformation_template(job_name, policy, role, job_config)
    tf_template = generate_terraform_template(job_name, region)

    return {
        "iam_policy.json": json.dumps(policy, indent=2),
        "iam_role.json": json.dumps(role, indent=2),
        "glue_job_config.json": json.dumps(job_config, indent=2),
        "cloudformation.json": json.dumps(cfn_template, indent=2),
        "main.tf": tf_template,
    }


def _generate_cfn_yaml(job_name: str, script_name: str, s3_bucket: str, region: str, account_id: str, worker_type: str = "G.1X", num_workers: int = 2, timeout: int = 60) -> str:
    """Generate CloudFormation YAML (human-readable) for a Glue job deployment."""
    return f"""AWSTemplateFormatVersion: '2010-09-09'
Description: >
  AWS Glue ETL Job: {job_name}
  Migrated from Azure Data Factory

Parameters:
  Environment:
    Type: String
    Default: dev
    AllowedValues: [dev, staging, prod]
  S3BucketName:
    Type: String
    Default: {s3_bucket}
    Description: S3 bucket containing scripts and data
  GlueJobName:
    Type: String
    Default: {job_name}

Resources:
  # S3 Bucket for Glue scripts and data
  GlueS3Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${{S3BucketName}}-${{Environment}}"
      VersioningConfiguration:
        Status: Enabled
      Tags:
        - Key: Project
          Value: !Ref GlueJobName
        - Key: MigratedFrom
          Value: AzureDataFactory

  # IAM Role for Glue Job
  GlueJobRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${{GlueJobName}}-${{Environment}}-role"
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: !Sub "${{GlueJobName}}-s3-access"
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Sid: S3DataAccess
                Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                  - s3:DeleteObject
                  - s3:ListBucket
                  - s3:GetBucketLocation
                Resource:
                  - !Sub "arn:aws:s3:::${{S3BucketName}}-${{Environment}}"
                  - !Sub "arn:aws:s3:::${{S3BucketName}}-${{Environment}}/*"
              - Sid: SecretsManagerAccess
                Effect: Allow
                Action:
                  - secretsmanager:GetSecretValue
                  - secretsmanager:DescribeSecret
                Resource:
                  - !Sub "arn:aws:secretsmanager:${{AWS::Region}}:${{AWS::AccountId}}:secret:*"
              - Sid: CloudWatchLogs
                Effect: Allow
                Action:
                  - logs:CreateLogGroup
                  - logs:CreateLogStream
                  - logs:PutLogEvents
                Resource:
                  - !Sub "arn:aws:logs:${{AWS::Region}}:${{AWS::AccountId}}:log-group:/aws-glue/*"

  # Glue Catalog Database
  GlueCatalogDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: !Sub "${{GlueJobName}}_${{Environment}}"
        Description: !Sub "Glue database for ${{GlueJobName}} (migrated from ADF)"

  # Glue ETL Job
  GlueETLJob:
    Type: AWS::Glue::Job
    DependsOn:
      - GlueJobRole
    Properties:
      Name: !Sub "${{GlueJobName}}-${{Environment}}"
      Role: !GetAtt GlueJobRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: !Sub "s3://${{S3BucketName}}-${{Environment}}/scripts/{script_name}"
        PythonVersion: '3'
      GlueVersion: '4.0'
      NumberOfWorkers: {num_workers}
      WorkerType: {worker_type}
      Timeout: {timeout}
      MaxRetries: 1
      DefaultArguments:
        "--job-language": python
        "--job-bookmark-option": job-bookmark-enable
        "--TempDir": !Sub "s3://${{S3BucketName}}-${{Environment}}/temp/"
        "--enable-metrics": "true"
        "--enable-continuous-cloudwatch-log": "true"
        "--enable-spark-ui": "true"
        "--spark-event-logs-path": !Sub "s3://${{S3BucketName}}-${{Environment}}/spark-logs/"
      Tags:
        Project: !Ref GlueJobName
        MigratedFrom: AzureDataFactory

Outputs:
  GlueJobName:
    Value: !Ref GlueETLJob
    Description: Name of the deployed Glue job
  GlueRoleArn:
    Value: !GetAtt GlueJobRole.Arn
    Description: ARN of the Glue IAM role
  S3BucketName:
    Value: !Ref GlueS3Bucket
    Description: S3 bucket for scripts and data
  GlueDatabaseName:
    Value: !Ref GlueCatalogDatabase
    Description: Glue Catalog database name
"""


def _generate_readme(job_name: str, script_name: str, data_files: list, s3_bucket: str, region: str, worker_type: str = "G.1X", num_workers: int = 2) -> str:
    """Generate README.md for the deployment bundle."""
    data_section = ""
    if data_files:
        file_list = "\n".join(f"  - `{f}`" for f in data_files)
        data_section = f"""
## 2. Upload Data to S3

Upload the data files from `data/` to S3:

```bash
aws s3 cp data/ s3://{s3_bucket}/data/ --recursive
```

Data files included:
{file_list}
"""
    else:
        data_section = """
## 2. Data Sources

No local data files were detected. Configure your data source connections
in the Glue job or Glue Catalog as needed.
"""

    return f"""# AWS Glue Job: {job_name}

> Migrated from Azure Data Factory using ADF-to-AWS-Glue Convertor

## Folder Structure

```
aws-glue-{job_name.lower().replace("_", "-")}/
|
├── scripts/
│   └── {script_name}           # PySpark Glue ETL script
│
├── data/
│   └── (source data files)     # Data files for S3 upload
│
├── iam/
│   └── glue_trust_policy.json  # IAM trust policy & permissions
│
├── cloudformation/
│   └── {job_name.lower()}-glue-job.yaml  # CloudFormation template
│
└── README.md                   # This file
```

## Deployment Steps

### 1. Create S3 Bucket

```bash
aws s3 mb s3://{s3_bucket} --region {region}
aws s3api put-bucket-versioning \\
    --bucket {s3_bucket} \\
    --versioning-configuration Status=Enabled
```
{data_section}
### 3. Upload Glue Script to S3

```bash
aws s3 cp scripts/{script_name} s3://{s3_bucket}/scripts/{script_name}
```

### 4. Create IAM Role and Attach Permissions

```bash
# Create the IAM role with trust policy
aws iam create-role \\
    --role-name {job_name}-GlueRole \\
    --assume-role-policy-document file://iam/glue_trust_policy.json

# Attach AWS managed Glue service policy
aws iam attach-role-policy \\
    --role-name {job_name}-GlueRole \\
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Attach custom S3 + Secrets Manager policy (inline)
aws iam put-role-policy \\
    --role-name {job_name}-GlueRole \\
    --policy-name {job_name}-DataAccessPolicy \\
    --policy-document file://iam/glue_trust_policy.json
```

### 5. Deploy via CloudFormation (Recommended)

```bash
aws cloudformation deploy \\
    --template-file cloudformation/{job_name.lower()}-glue-job.yaml \\
    --stack-name {job_name.lower()}-stack \\
    --parameter-overrides \\
        Environment=dev \\
        S3BucketName={s3_bucket} \\
        GlueJobName={job_name} \\
    --capabilities CAPABILITY_NAMED_IAM \\
    --region {region}
```

### 6. Run the Glue Job

```bash
aws glue start-job-run --job-name {job_name}-dev --region {region}
```

## Job Configuration

| Parameter | Value |
|-----------|-------|
| Glue Version | 4.0 |
| Python Version | 3 |
| Worker Type | {worker_type} |
| Number of Workers | {num_workers} |
| Timeout (min) | 60 |
| Job Bookmarks | Enabled |
| Spark UI | Enabled |
| CloudWatch Logs | Enabled |

## Notes

- Replace `<YOUR-BUCKET>` placeholders in the script with actual S3 paths
- Store database credentials in AWS Secrets Manager before running
- The CloudFormation template creates all resources including the S3 bucket
- For production: increase workers, adjust timeout, enable job bookmarks
"""


def generate_job_folder_bundle(
    job_name: str,
    glue_script: str,
    data_files: dict = None,
    s3_buckets: list = None,
    region: str = "us-east-1",
    account_id: str = "<AWS_ACCOUNT_ID>",
    worker_type: str = "G.1X",
    num_workers: int = 2,
    timeout: int = 60,
) -> dict:
    """
    Generate a complete AWS Glue deployment bundle in the folder structure:

    aws-glue-<job>/
    ├── scripts/<job>.py
    ├── data/<files...>
    ├── iam/glue_trust_policy.json
    ├── cloudformation/<job>-glue-job.yaml
    └── README.md

    Returns a dict of {relative_path: file_content} for ZIP assembly.
    """
    if not s3_buckets:
        s3_buckets = ["<YOUR-BUCKET>"]
    if not data_files:
        data_files = {}

    s3_bucket = s3_buckets[0] if s3_buckets else f"{job_name.lower()}-glue-data"
    folder_name = f"aws-glue-{job_name.lower().replace('_', '-')}"
    script_name = f"{job_name}.py"

    # IAM: Combined trust policy + permissions document
    trust_and_permissions = {
        "Version": "2012-10-17",
        "RoleName": f"{job_name}-GlueRole",
        "TrustPolicy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
        "PermissionsPolicy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "GlueServiceAccess",
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetJob",
                        "glue:GetJobs",
                        "glue:GetJobRun",
                        "glue:GetJobRuns",
                        "glue:StartJobRun",
                        "glue:BatchStopJobRun",
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                        "glue:GetPartitions",
                        "glue:BatchCreatePartition",
                    ],
                    "Resource": [
                        f"arn:aws:glue:{region}:{account_id}:catalog",
                        f"arn:aws:glue:{region}:{account_id}:database/*",
                        f"arn:aws:glue:{region}:{account_id}:table/*",
                        f"arn:aws:glue:{region}:{account_id}:job/{job_name}*",
                    ],
                },
                {
                    "Sid": "S3DataAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{s3_bucket}",
                        f"arn:aws:s3:::{s3_bucket}/*",
                    ],
                },
                {
                    "Sid": "SecretsManagerAccess",
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    "Resource": [
                        f"arn:aws:secretsmanager:{region}:{account_id}:secret:*",
                    ],
                },
                {
                    "Sid": "CloudWatchLogs",
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                    ],
                    "Resource": [
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws-glue/*",
                    ],
                },
                {
                    "Sid": "CloudWatchMetrics",
                    "Effect": "Allow",
                    "Action": ["cloudwatch:PutMetricData"],
                    "Resource": "*",
                    "Condition": {"StringEquals": {"cloudwatch:namespace": "Glue"}},
                },
            ],
        },
        "ManagedPolicyArns": [
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        ],
        "WorkerConfiguration": {
            "WorkerType": worker_type,
            "NumberOfWorkers": num_workers,
            "GlueVersion": "4.0",
            "PythonVersion": "3",
            "Timeout": timeout,
            "MaxRetries": 1,
            "JobBookmarks": "Enabled",
        },
    }

    # CloudFormation YAML
    cfn_yaml = _generate_cfn_yaml(
        job_name, script_name, s3_bucket, region, account_id,
        worker_type, num_workers, timeout,
    )

    # README
    readme = _generate_readme(
        job_name, script_name, list(data_files.keys()),
        s3_bucket, region, worker_type, num_workers,
    )

    # Assemble bundle
    bundle = {}
    bundle[f"{folder_name}/scripts/{script_name}"] = glue_script
    bundle[f"{folder_name}/iam/glue_trust_policy.json"] = json.dumps(trust_and_permissions, indent=2)
    bundle[f"{folder_name}/cloudformation/{job_name.lower()}-glue-job.yaml"] = cfn_yaml
    bundle[f"{folder_name}/README.md"] = readme

    # Data files
    for filename, content in data_files.items():
        bundle[f"{folder_name}/data/{filename}"] = content

    # If no data files, add a placeholder .gitkeep
    if not data_files:
        bundle[f"{folder_name}/data/.gitkeep"] = "# Place source data files here before uploading to S3\n"

    return bundle
