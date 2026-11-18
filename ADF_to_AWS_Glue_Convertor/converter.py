"""
ADF to AWS Glue Converter Engine
Converts Azure Data Factory artifacts (Dataflows, Datasets, Linked Services)
into AWS Glue compatible PySpark scripts and configurations.
"""
import json
import re


# --- Type Mapping ---
ADF_TO_SPARK_TYPES = {
    "string": "StringType()",
    "integer": "IntegerType()",
    "int": "IntegerType()",
    "long": "LongType()",
    "double": "DoubleType()",
    "float": "FloatType()",
    "boolean": "BooleanType()",
    "date": "DateType()",
    "timestamp": "TimestampType()",
    "binary": "BinaryType()",
    "decimal": "DecimalType()",
    "short": "ShortType()",
    "byte": "ByteType()",
}


def convert_linked_service(ls_json: dict) -> dict:
    """Convert an ADF Linked Service to Glue Connection config."""
    name = ls_json.get("name", "unknown")
    props = ls_json.get("properties", {})
    ls_type = props.get("type", "")
    type_props = props.get("typeProperties", {})

    result = {
        "name": name,
        "adf_type": ls_type,
        "glue_connection": {},
        "notes": [],
    }

    if "Snowflake" in ls_type:
        result["glue_connection"] = {
            "Type": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": f"jdbc:snowflake://{type_props.get('host', '<HOST>')}/",
                "USERNAME": type_props.get("user", "<USERNAME>"),
                "PASSWORD": "{{resolve:secretsmanager:snowflake-credentials:SecretString:password}}",
                "DATABASE": type_props.get("database", "<DATABASE>"),
                "WAREHOUSE": type_props.get("warehouse", "<WAREHOUSE>"),
            },
        }
        result["notes"].append("Store Snowflake credentials in AWS Secrets Manager")

    elif "AzureBlobStorage" in ls_type:
        conn_str = type_props.get("connectionString", "")
        account_match = re.search(r"AccountName=([^;]+)", conn_str)
        account_name = account_match.group(1) if account_match else "<STORAGE_ACCOUNT>"
        result["glue_connection"] = {
            "Type": "S3",
            "Description": f"Migrated from Azure Blob: {account_name}",
            "ConnectionProperties": {
                "S3_BUCKET": f"s3://<YOUR-BUCKET-NAME>/  # was Azure Blob account: {account_name}",
            },
        }
        result["notes"].append(
            f"Azure Blob '{account_name}' must be migrated to S3 or accessed via S3-compatible gateway"
        )

    elif "AzureSqlDatabase" in ls_type or "SqlServer" in ls_type:
        result["glue_connection"] = {
            "Type": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:sqlserver://<HOST>:<PORT>;databaseName=<DB>",
                "USERNAME": "{{resolve:secretsmanager:sql-credentials:SecretString:username}}",
                "PASSWORD": "{{resolve:secretsmanager:sql-credentials:SecretString:password}}",
            },
        }
        result["notes"].append("Migrate SQL connection details to Secrets Manager")

    else:
        result["glue_connection"] = {
            "Type": "CUSTOM",
            "Description": f"Manual migration needed for ADF type: {ls_type}",
        }
        result["notes"].append(f"No automatic mapping for ADF type '{ls_type}' — manual review required")

    return result


def convert_dataset(ds_json: dict, linked_services: dict) -> dict:
    """Convert an ADF Dataset to Glue Catalog Table / source config."""
    name = ds_json.get("name", "unknown")
    props = ds_json.get("properties", {})
    ds_type = props.get("type", "")
    type_props = props.get("typeProperties", {})
    ls_ref = props.get("linkedServiceName", {}).get("referenceName", "")

    result = {
        "name": name,
        "adf_type": ds_type,
        "glue_catalog_entry": {},
        "glue_code_snippet": "",
        "notes": [],
    }

    if ds_type == "DelimitedText":
        location = type_props.get("location", {})
        container = location.get("container", "<container>")
        file_name = location.get("fileName", "*")
        delimiter = type_props.get("columnDelimiter", ",")
        has_header = type_props.get("firstRowAsHeader", True)

        result["glue_catalog_entry"] = {
            "DatabaseName": "<GLUE_DATABASE>",
            "TableInput": {
                "Name": name.lower(),
                "StorageDescriptor": {
                    "Location": f"s3://<YOUR-BUCKET>/{container}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        "Parameters": {"separatorChar": delimiter},
                    },
                },
                "Parameters": {"skip.header.line.count": "1" if has_header else "0"},
            },
        }
        result["glue_code_snippet"] = (
            f'# Read source: {name}\n'
            f'df_{name} = glueContext.create_dynamic_frame.from_options(\n'
            f'    connection_type="s3",\n'
            f'    format="csv",\n'
            f'    format_options={{"withHeader": {has_header}, "separator": "{delimiter}"}},\n'
            f'    connection_options={{"paths": ["s3://<YOUR-BUCKET>/{container}/{file_name}"]}},\n'
            f'    transformation_ctx="{name}_ctx"\n'
            f')\n'
        )

    elif "Snowflake" in ds_type:
        schema = type_props.get("schema", "public")
        table = type_props.get("table", "<TABLE>")
        result["glue_catalog_entry"] = {
            "DatabaseName": "<GLUE_DATABASE>",
            "TableInput": {
                "Name": name.lower(),
                "StorageDescriptor": {
                    "Location": f"jdbc:snowflake (schema={schema}, table={table})",
                },
            },
        }
        result["glue_code_snippet"] = (
            f'# Sink: {name} (Snowflake via JDBC)\n'
            f'df_{name} = glueContext.write_dynamic_frame.from_options(\n'
            f'    frame=<SOURCE_FRAME>,\n'
            f'    connection_type="custom.jdbc",\n'
            f'    connection_options={{\n'
            f'        "connectionName": "{ls_ref}",\n'
            f'        "dbtable": "{schema}.{table}",\n'
            f'    }},\n'
            f'    transformation_ctx="{name}_sink_ctx"\n'
            f')\n'
        )
        result["notes"].append("Ensure Snowflake JDBC driver is added to Glue job jars")

    else:
        result["glue_code_snippet"] = f"# TODO: Manual migration needed for dataset type: {ds_type}\n"
        result["notes"].append(f"Dataset type '{ds_type}' requires manual conversion")

    return result


def _parse_dsl_columns(script_lines: list) -> list:
    """Extract column definitions from ADF dataflow script."""
    columns = []
    in_output = False
    for line in script_lines:
        stripped = line.strip().rstrip(",")
        if "output(" in stripped:
            in_output = True
            continue
        if in_output and ")" in stripped and "as" not in stripped:
            in_output = False
            continue
        if in_output:
            match = re.match(r"(\w+)\s+as\s+(\w+)", stripped)
            if match:
                columns.append({"name": match.group(1), "type": match.group(2)})
    return columns


def _parse_filter_expression(script_lines: list) -> str:
    """Extract filter expressions from ADF dataflow script."""
    for line in script_lines:
        if "filter(" in line:
            match = re.search(r"filter\((.+?)\)\s*~>", line)
            if match:
                expr = match.group(1)
                expr = expr.replace("==", "==").replace("&&", " & ").replace("||", " | ")
                return expr
    return ""


def convert_dataflow(df_json: dict, datasets: dict) -> dict:
    """Convert an ADF Mapping Data Flow to a Glue PySpark ETL script."""
    name = df_json.get("name", "unknown")
    props = df_json.get("properties", {})
    type_props = props.get("typeProperties", {})

    sources = type_props.get("sources", [])
    sinks = type_props.get("sinks", [])
    script_lines = type_props.get("scriptLines", [])
    transformations = type_props.get("transformations", [])

    columns = _parse_dsl_columns(script_lines)
    filter_expr = _parse_filter_expression(script_lines)

    # Build the Glue PySpark script
    lines = []
    lines.append('"""')
    lines.append(f"AWS Glue ETL Job: {name}")
    lines.append(f"Converted from ADF Mapping Data Flow: {name}")
    lines.append('"""')
    lines.append("import sys")
    lines.append("from awsglue.transforms import *")
    lines.append("from awsglue.utils import getResolvedOptions")
    lines.append("from pyspark.context import SparkContext")
    lines.append("from awsglue.context import GlueContext")
    lines.append("from awsglue.job import Job")
    lines.append("from pyspark.sql import functions as F")
    lines.append("from pyspark.sql.types import *")
    lines.append("")
    lines.append("# Initialize Glue context")
    lines.append('args = getResolvedOptions(sys.argv, ["JOB_NAME"])')
    lines.append("sc = SparkContext()")
    lines.append("glueContext = GlueContext(sc)")
    lines.append("spark = glueContext.spark_session")
    lines.append('job = Job(glueContext)')
    lines.append('job.init(args["JOB_NAME"], args)')
    lines.append("")

    # Schema definition
    if columns:
        lines.append("# Schema definition (from ADF dataflow)")
        lines.append("schema = StructType([")
        for col in columns:
            spark_type = ADF_TO_SPARK_TYPES.get(col["type"], "StringType()")
            lines.append(f'    StructField("{col["name"]}", {spark_type}, True),')
        lines.append("])")
        lines.append("")

    # Sources
    for src in sources:
        ds_ref = src.get("dataset", {}).get("referenceName", "unknown_source")
        src_name = src.get("name", "source")
        ds_info = datasets.get(ds_ref, {})
        ds_type = ds_info.get("properties", {}).get("type", "")

        if ds_type == "DelimitedText":
            loc = ds_info.get("properties", {}).get("typeProperties", {}).get("location", {})
            container = loc.get("container", "<container>")
            file_name = loc.get("fileName", "*")
            delimiter = ds_info.get("properties", {}).get("typeProperties", {}).get("columnDelimiter", ",")
            lines.append(f"# Source: {src_name} (from ADF dataset: {ds_ref})")
            lines.append(f'{src_name}_dyf = glueContext.create_dynamic_frame.from_options(')
            lines.append(f'    connection_type="s3",')
            lines.append(f'    format="csv",')
            lines.append(f'    format_options={{"withHeader": True, "separator": "{delimiter}"}},')
            lines.append(f'    connection_options={{"paths": ["s3://<YOUR-BUCKET>/{container}/{file_name}"]}},')
            lines.append(f'    transformation_ctx="{src_name}_ctx"')
            lines.append(f")")
            lines.append(f"{src_name}_df = {src_name}_dyf.toDF()")
        else:
            lines.append(f"# Source: {src_name} (from dataset: {ds_ref})")
            lines.append(f"# TODO: Configure source connection for type '{ds_type}'")
            lines.append(f'{src_name}_df = spark.read.format("jdbc").load()  # Configure manually')
        lines.append("")

    # Transformations (filters)
    if filter_expr:
        lines.append("# Transformation: Filter")
        lines.append(f'filtered_df = {sources[0]["name"]}_df.filter("{filter_expr}")')
        lines.append("")
        last_df = "filtered_df"
    elif transformations:
        for t in transformations:
            t_name = t.get("name", "transform")
            lines.append(f"# Transformation: {t_name}")
            lines.append(f"# TODO: Implement transformation logic for '{t_name}'")
        lines.append("")
        last_df = f"{sources[0]['name']}_df" if sources else "source_df"
    else:
        last_df = f"{sources[0]['name']}_df" if sources else "source_df"

    # Sinks
    for sink in sinks:
        ds_ref = sink.get("dataset", {}).get("referenceName", "unknown_sink")
        sink_name = sink.get("name", "sink")
        ds_info = datasets.get(ds_ref, {})
        ds_type = ds_info.get("properties", {}).get("type", "")

        if "Snowflake" in ds_type:
            tp = ds_info.get("properties", {}).get("typeProperties", {})
            schema = tp.get("schema", "public")
            table = tp.get("table", "<TABLE>")
            lines.append(f"# Sink: {sink_name} -> Snowflake ({schema}.{table})")
            lines.append(f"output_dyf = DynamicFrame.fromDF({last_df}, glueContext, '{sink_name}_dyf')")
            lines.append(f"glueContext.write_dynamic_frame.from_options(")
            lines.append(f'    frame=output_dyf,')
            lines.append(f'    connection_type="custom.jdbc",')
            lines.append(f"    connection_options={{")
            lines.append(f'        "connectionName": "<GLUE_SNOWFLAKE_CONNECTION>",')
            lines.append(f'        "dbtable": "{schema}.{table}",')
            lines.append(f"    }},")
            lines.append(f'    transformation_ctx="{sink_name}_ctx"')
            lines.append(f")")
        else:
            lines.append(f"# Sink: {sink_name} (type: {ds_type})")
            lines.append(f"output_dyf = DynamicFrame.fromDF({last_df}, glueContext, '{sink_name}_dyf')")
            lines.append(f"glueContext.write_dynamic_frame.from_catalog(")
            lines.append(f'    database="<GLUE_DATABASE>",')
            lines.append(f'    table_name="{ds_ref.lower()}",')
            lines.append(f'    transformation_ctx="{sink_name}_ctx"')
            lines.append(f")")
        lines.append("")

    lines.append("job.commit()")

    return {
        "name": name,
        "glue_script": "\n".join(lines),
        "sources": [s.get("name") for s in sources],
        "sinks": [s.get("name") for s in sinks],
        "transformations": [t.get("name") for t in transformations],
        "notes": [],
    }
