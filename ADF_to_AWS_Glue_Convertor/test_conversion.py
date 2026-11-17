"""
Test script: Validates ADF to AWS Glue conversion using sample workspace files.
"""
import sys
sys.path.insert(0, "/workspace/ADF_to_AWS_Glue_Convertor")

import json
from converter import convert_linked_service, convert_dataset, convert_dataflow
from glue_deployment import generate_full_deployment_bundle

# Load sample ADF files from workspace
with open("/workspace/linkedService/Snowflake_Link.json") as f:
    ls_snowflake = json.load(f)

with open("/workspace/linkedService/ls_blob.json") as f:
    ls_blob = json.load(f)

with open("/workspace/dataset/ds_customers_src.json") as f:
    ds_customers = json.load(f)

with open("/workspace/dataset/SNF_TGT.json") as f:
    ds_snf_tgt = json.load(f)

with open("/workspace/dataflow/File_2_Snowflake.json") as f:
    df_file2snow = json.load(f)

print("=" * 70)
print("  TEST: ADF to AWS Glue Conversion Validation")
print("=" * 70)

# --- Test 1: Linked Services ---
print("\n" + "=" * 70)
print("  TEST 1: Linked Service Conversion")
print("=" * 70)

result_ls_snow = convert_linked_service(ls_snowflake)
print(f"\n[Snowflake_Link] ADF Type: {result_ls_snow['adf_type']}")
print(f"  Glue Connection Type: {result_ls_snow['glue_connection'].get('Type')}")
conn_props = result_ls_snow['glue_connection'].get('ConnectionProperties', {})
print(f"  JDBC URL: {conn_props.get('JDBC_CONNECTION_URL', 'N/A')}")
print(f"  Username: {conn_props.get('USERNAME', 'N/A')}")
print(f"  Database: {conn_props.get('DATABASE', 'N/A')}")
print(f"  Warehouse: {conn_props.get('WAREHOUSE', 'N/A')}")
print(f"  Notes: {result_ls_snow['notes']}")
assert result_ls_snow['glue_connection']['Type'] == 'JDBC', "FAIL: Snowflake should map to JDBC"
assert 'PE39900' in conn_props.get('JDBC_CONNECTION_URL', ''), "FAIL: Host not in JDBC URL"
print("  ✓ PASSED")

result_ls_blob = convert_linked_service(ls_blob)
print(f"\n[ls_blob] ADF Type: {result_ls_blob['adf_type']}")
print(f"  Glue Connection Type: {result_ls_blob['glue_connection'].get('Type')}")
print(f"  Notes: {result_ls_blob['notes']}")
assert result_ls_blob['glue_connection']['Type'] == 'S3', "FAIL: Blob should map to S3"
assert 'adfgluedemo' in result_ls_blob['notes'][0], "FAIL: Account name not extracted"
print("  ✓ PASSED")

# --- Test 2: Datasets ---
print("\n" + "=" * 70)
print("  TEST 2: Dataset Conversion")
print("=" * 70)

result_ds_csv = convert_dataset(ds_customers, {})
print(f"\n[ds_customers_src] ADF Type: {result_ds_csv['adf_type']}")
print(f"  Glue Catalog Table: {result_ds_csv['glue_catalog_entry'].get('TableInput', {}).get('Name')}")
print(f"  Code snippet preview:")
for line in result_ds_csv['glue_code_snippet'].split('\n')[:5]:
    print(f"    {line}")
assert result_ds_csv['adf_type'] == 'DelimitedText', "FAIL: Type mismatch"
assert 'source-data' in result_ds_csv['glue_code_snippet'], "FAIL: Container not in code"
assert 'customers.csv' in result_ds_csv['glue_code_snippet'], "FAIL: Filename not in code"
print("  ✓ PASSED")

result_ds_snf = convert_dataset(ds_snf_tgt, {})
print(f"\n[SNF_TGT] ADF Type: {result_ds_snf['adf_type']}")
print(f"  Code snippet preview:")
for line in result_ds_snf['glue_code_snippet'].split('\n')[:5]:
    print(f"    {line}")
assert 'SnowflakeV2Table' in result_ds_snf['adf_type'], "FAIL: Type mismatch"
assert 'DEV.CUST' in result_ds_snf['glue_code_snippet'], "FAIL: Schema.Table not in code"
print("  ✓ PASSED")

# --- Test 3: Dataflow (end-to-end ETL script) ---
print("\n" + "=" * 70)
print("  TEST 3: Dataflow Conversion (Full ETL Script)")
print("=" * 70)

# Build dataset lookup as the app would
dataset_lookup = {
    "ds_customers_src": ds_customers,
    "SNF_TGT": ds_snf_tgt,
}

result_df = convert_dataflow(df_file2snow, dataset_lookup)
print(f"\n[File_2_Snowflake] Sources: {result_df['sources']}")
print(f"  Sinks: {result_df['sinks']}")
print(f"  Transformations: {result_df['transformations']}")

script = result_df['glue_script']
print(f"\n  Script length: {len(script)} characters")
print(f"  Script preview (first 30 lines):")
for i, line in enumerate(script.split('\n')[:30]):
    print(f"    {i+1:3}: {line}")

# Validate script contents
assert 'from awsglue.transforms import *' in script, "FAIL: Missing Glue imports"
assert 'GlueContext' in script, "FAIL: Missing GlueContext"
assert 'job.init' in script, "FAIL: Missing job init"
assert 'job.commit()' in script, "FAIL: Missing job.commit()"
assert 'source-data' in script, "FAIL: S3 path missing container"
assert 'customers.csv' in script, "FAIL: Source filename missing"
assert 'CUSTOMER_ID' in script, "FAIL: Schema columns missing"
assert 'filter' in script.lower(), "FAIL: Filter transformation missing"
assert 'C0011' in script, "FAIL: Filter value missing"
assert 'DEV.CUST' in script, "FAIL: Snowflake sink target missing"
print("\n  ✓ ALL SCRIPT ASSERTIONS PASSED")

# --- Test 4: Deployment Bundle ---
print("\n" + "=" * 70)
print("  TEST 4: Deployment Bundle Generation")
print("=" * 70)

bundle = generate_full_deployment_bundle(
    job_name="File_2_Snowflake",
    s3_buckets=["my-data-bucket"],
    region="us-east-1",
    account_id="123456789012",
)

print(f"\n  Bundle files generated: {list(bundle.keys())}")
for fname, content in bundle.items():
    print(f"  - {fname}: {len(content)} chars")

# Validate IAM policy
policy = json.loads(bundle['iam_policy.json'])
assert policy['Version'] == '2012-10-17', "FAIL: Policy version"
statements = policy['Statement']
assert any(s['Sid'] == 'GlueServiceAccess' for s in statements), "FAIL: Missing Glue access"
assert any(s['Sid'] == 'S3DataAccess' for s in statements), "FAIL: Missing S3 access"
assert any(s['Sid'] == 'SecretsManagerAccess' for s in statements), "FAIL: Missing Secrets access"
assert any('my-data-bucket' in str(s.get('Resource', '')) for s in statements), "FAIL: Bucket not in policy"
print("  ✓ IAM Policy: PASSED")

# Validate IAM role
role = json.loads(bundle['iam_role.json'])
assert 'glue.amazonaws.com' in str(role['AssumeRolePolicyDocument']), "FAIL: Trust policy missing Glue"
print("  ✓ IAM Role: PASSED")

# Validate CloudFormation
cfn = json.loads(bundle['cloudformation.json'])
assert 'GlueJob' in cfn['Resources'], "FAIL: Missing Glue job resource"
assert 'GlueJobRole' in cfn['Resources'], "FAIL: Missing role resource"
assert 'GlueDatabase' in cfn['Resources'], "FAIL: Missing database resource"
assert 'ScriptsBucket' in cfn['Resources'], "FAIL: Missing S3 bucket resource"
print("  ✓ CloudFormation: PASSED")

# Validate Terraform
tf = bundle['main.tf']
assert 'aws_glue_job' in tf, "FAIL: Missing glue_job resource"
assert 'aws_iam_role' in tf, "FAIL: Missing iam_role resource"
assert 'aws_s3_bucket' in tf, "FAIL: Missing s3_bucket resource"
assert 'aws_glue_catalog_database' in tf, "FAIL: Missing catalog database"
print("  ✓ Terraform: PASSED")

# Validate Glue job config
job_cfg = json.loads(bundle['glue_job_config.json'])
assert job_cfg['GlueVersion'] == '4.0', "FAIL: Wrong Glue version"
assert job_cfg['Command']['PythonVersion'] == '3', "FAIL: Wrong Python version"
assert job_cfg['WorkerType'] == 'G.1X', "FAIL: Wrong worker type"
print("  ✓ Glue Job Config: PASSED")

print("\n" + "=" * 70)
print("  ALL TESTS PASSED ✓")
print("=" * 70)
