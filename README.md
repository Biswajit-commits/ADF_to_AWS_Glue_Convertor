# ADF_to_AWS_Glue_Convertor v1

A Streamlit in Snowflake app that converts **Azure Data Factory (ADF)** artifacts — Linked Services, Datasets and Mapping Data Flows — into **AWS Glue** PySpark ETL scripts, plus a ready-to-deploy bundle (IAM, CloudFormation, Terraform).

## Workspace layout

```
.
├── ADF_to_AWS_Glue_Convertor/     # Streamlit app
│   ├── streamlit_app.py           # UI: upload ADF JSON, run conversion, preview & download
│   ├── converter.py               # Conversion engine (linked services, datasets, dataflows)
│   ├── glue_deployment.py         # IAM / CloudFormation / Terraform / job-folder bundle generator
│   ├── test_conversion.py         # Validation script using the sample ADF files below
│   ├── snowflake.yml              # Streamlit deployment definition
│   ├── pyproject.toml             # Python dependencies
│   └── .streamlit/config.toml     # Theme & sleep settings
├── linkedService/                 # Sample ADF linked services
│   ├── Snowflake_Link.json        # SnowflakeV2
│   └── ls_blob.json               # AzureBlobStorage
├── dataset/                       # Sample ADF datasets
│   ├── ds_customers_src.json      # DelimitedText (customers.csv in Blob container source-data)
│   └── SNF_TGT.json               # SnowflakeV2Table (DEV.CUST)
└── dataflow/                      # Sample ADF Mapping Data Flow
    ├── File_2_Snowflake.json
    └── File_2_Snowflake.dsl       # Source → filter(CUSTOMER_ID == 'C0011') → Snowflake sink
```

## Conversion mapping

| ADF artifact | AWS Glue output |
|---|---|
| Linked Service – Snowflake | Glue JDBC connection (password via Secrets Manager) |
| Linked Service – Azure Blob Storage | S3 connection placeholder (data must be migrated to S3) |
| Linked Service – Azure SQL / SQL Server | Glue JDBC connection (credentials via Secrets Manager) |
| Linked Service – other | `CUSTOM` placeholder flagged for manual review |
| Dataset – DelimitedText | Glue Catalog table (OpenCSVSerde) + `create_dynamic_frame` read snippet |
| Dataset – Snowflake table | Glue Catalog entry + JDBC write snippet |
| Mapping Data Flow | Full Glue PySpark job: schema from DSL (ADF → Spark types), sources, filter transformations, sinks |

Each conversion also returns **notes** highlighting manual migration steps.

## Deployment bundle

For every dataflow the app produces a downloadable ZIP:

```
aws-glue-<job>/
├── scripts/<job>.py                    # Glue PySpark script
├── data/<files...>                     # Optional uploaded source data
├── iam/glue_trust_policy.json          # Role trust policy + permissions
├── cloudformation/<job>-glue-job.yaml  # CloudFormation stack
└── README.md                           # Deployment instructions
```

`glue_deployment.py` can also generate standalone IAM policies/roles, Glue job configs and Terraform templates. Worker type (default `G.1X`), number of workers (default 2), timeout, region and AWS account ID are configurable.

## Using the app

1. In the sidebar upload Linked Service, Dataset and Dataflow JSON files (optionally source data files: CSV/JSON/Parquet/TXT/TSV).
2. Click **Run Conversion Engine**.
3. Review results in the tabs: **Bundle**, **Scripts**, **Connections**, **Datasets**, **Download**.
4. Download the deployment bundle ZIP and follow its README to deploy to AWS.

## Deployment (Snowflake)

Defined in `ADF_to_AWS_Glue_Convertor/snowflake.yml`:

- Object: `DWH.DEV.ADF_TO_AWS_GLUE_CONVERTOR`
- Query warehouse: `COMPUTE_WH`
- Compute pool: `SYSTEM_COMPUTE_POOL_CPU` (container runtime, `SpcsOnly`)
- Requires Python ≥ 3.11, `streamlit[snowflake]>=1.54.0`

## Testing

Run the validation script against the sample files in this workspace:

```bash
python ADF_to_AWS_Glue_Convertor/test_conversion.py
```

It converts the sample linked services, datasets and the `File_2_Snowflake` dataflow, then generates a full deployment bundle and prints the results.

## Limitations

- Supported dataflow transformations are currently sources, filters and sinks; other transformations need manual conversion.
- Azure Blob data is not moved automatically — copy it to S3 and replace `<YOUR-BUCKET>` placeholders.
- Credentials are never migrated; configure them in AWS Secrets Manager.
