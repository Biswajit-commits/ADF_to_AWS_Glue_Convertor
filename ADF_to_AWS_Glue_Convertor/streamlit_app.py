"""
ADF to AWS Glue Convertor
A Streamlit application to upload Azure Data Factory artifacts and convert them
to AWS Glue compatible PySpark scripts with deployment bundles.
"""
import io
import json
import zipfile
import streamlit as st

from converter import convert_linked_service, convert_dataset, convert_dataflow
from glue_deployment import generate_job_folder_bundle

st.set_page_config(page_title="ADF to AWS Glue Convertor", page_icon="🔄", layout="wide")

st.title("ADF to AWS Glue Convertor")
st.markdown("Upload Azure Data Factory JSON files and convert them to AWS Glue PySpark ETL scripts with full deployment bundles (IAM, CloudFormation, Terraform).")

st.divider()

# --- Session State Initialization ---
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {"dataflow": {}, "dataset": {}, "linkedService": {}, "data": {}}
if "conversion_results" not in st.session_state:
    st.session_state.conversion_results = None
if "conversion_done" not in st.session_state:
    st.session_state.conversion_done = False


# --- Sidebar: Upload Section ---
with st.sidebar:
    st.header("Upload ADF Files")
    st.markdown("Upload JSON files from your ADF repository folders.")

    st.subheader("Linked Services")
    ls_files = st.file_uploader(
        "Upload Linked Service JSON files",
        type=["json"],
        accept_multiple_files=True,
        key="ls_upload",
    )
    if ls_files:
        for f in ls_files:
            content = json.loads(f.read())
            f.seek(0)
            st.session_state.uploaded_files["linkedService"][f.name] = content
        st.success(f"{len(ls_files)} linked service(s) uploaded")

    st.subheader("Datasets")
    ds_files = st.file_uploader(
        "Upload Dataset JSON files",
        type=["json"],
        accept_multiple_files=True,
        key="ds_upload",
    )
    if ds_files:
        for f in ds_files:
            content = json.loads(f.read())
            f.seek(0)
            st.session_state.uploaded_files["dataset"][f.name] = content
        st.success(f"{len(ds_files)} dataset(s) uploaded")

    st.subheader("Dataflows")
    df_files = st.file_uploader(
        "Upload Dataflow JSON files",
        type=["json"],
        accept_multiple_files=True,
        key="df_upload",
    )
    if df_files:
        for f in df_files:
            content = json.loads(f.read())
            f.seek(0)
            st.session_state.uploaded_files["dataflow"][f.name] = content
        st.success(f"{len(df_files)} dataflow(s) uploaded")

    st.subheader("Data Files (Optional)")
    data_files_upload = st.file_uploader(
        "Upload source data files (CSV, JSON, etc.)",
        type=["csv", "json", "parquet", "txt", "tsv"],
        accept_multiple_files=True,
        key="data_upload",
    )
    if data_files_upload:
        for f in data_files_upload:
            st.session_state.uploaded_files["data"][f.name] = f.read().decode("utf-8", errors="replace")
            f.seek(0)
        st.success(f"{len(data_files_upload)} data file(s) uploaded")

    st.divider()
    total = sum(len(v) for v in st.session_state.uploaded_files.values())
    st.metric("Total Files Uploaded", total)


# --- Main Area: File Overview ---
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Linked Services")
    if st.session_state.uploaded_files["linkedService"]:
        for fname, content in st.session_state.uploaded_files["linkedService"].items():
            ls_type = content.get("properties", {}).get("type", "Unknown")
            st.markdown(f"- **{fname}** (`{ls_type}`)")
    else:
        st.info("No linked services uploaded yet")

with col2:
    st.subheader("Datasets")
    if st.session_state.uploaded_files["dataset"]:
        for fname, content in st.session_state.uploaded_files["dataset"].items():
            ds_type = content.get("properties", {}).get("type", "Unknown")
            st.markdown(f"- **{fname}** (`{ds_type}`)")
    else:
        st.info("No datasets uploaded yet")

with col3:
    st.subheader("Dataflows")
    if st.session_state.uploaded_files["dataflow"]:
        for fname, content in st.session_state.uploaded_files["dataflow"].items():
            df_type = content.get("properties", {}).get("type", "Unknown")
            st.markdown(f"- **{fname}** (`{df_type}`)")
    else:
        st.info("No dataflows uploaded yet")

st.divider()

# --- Conversion Engine ---
st.subheader("Run Conversion Engine")

col_btn, col_region, col_account = st.columns([1, 1, 1])
with col_region:
    aws_region = st.text_input("AWS Region", value="us-east-1")
with col_account:
    aws_account_id = st.text_input("AWS Account ID", value="<AWS_ACCOUNT_ID>")

col_bucket, col_worker, col_workers = st.columns([1, 1, 1])
with col_bucket:
    s3_bucket_name = st.text_input("S3 Bucket Name", value="<YOUR-BUCKET>")
with col_worker:
    worker_type = st.selectbox("Worker Type", ["G.1X", "G.2X", "G.4X", "G.8X", "Z.2X"], index=0)
with col_workers:
    num_workers = st.number_input("Number of Workers", min_value=2, max_value=100, value=2)

st.write("")
run_conversion = st.button("Run Conversion Engine", type="primary", use_container_width=True)

if run_conversion:
    uploaded = st.session_state.uploaded_files
    if not any(uploaded[k] for k in ["dataflow", "dataset", "linkedService"]):
        st.error("Please upload at least one ADF file before running conversion.")
    else:
        with st.spinner("Converting ADF artifacts to AWS Glue..."):
            results = {
                "linked_services": [],
                "datasets": [],
                "dataflows": [],
                "deployment_bundles": {},
            }

            # Convert linked services
            for fname, content in uploaded["linkedService"].items():
                converted = convert_linked_service(content)
                results["linked_services"].append(converted)

            # Build dataset lookup
            dataset_lookup = {}
            for fname, content in uploaded["dataset"].items():
                dataset_lookup[content.get("name", fname.replace(".json", ""))] = content

            # Convert datasets
            for fname, content in uploaded["dataset"].items():
                converted = convert_dataset(content, uploaded["linkedService"])
                results["datasets"].append(converted)

            # Convert dataflows and generate folder-structured bundles
            for fname, content in uploaded["dataflow"].items():
                converted = convert_dataflow(content, dataset_lookup)
                results["dataflows"].append(converted)

                # Generate deployment folder bundle per dataflow
                bundle = generate_job_folder_bundle(
                    job_name=converted["name"],
                    glue_script=converted["glue_script"],
                    data_files=uploaded.get("data", {}),
                    s3_buckets=[s3_bucket_name],
                    region=aws_region,
                    account_id=aws_account_id,
                    worker_type=worker_type,
                    num_workers=num_workers,
                )
                results["deployment_bundles"][converted["name"]] = bundle

            st.session_state.conversion_results = results
            st.session_state.conversion_done = True

        st.success("Conversion complete!")

# --- Results Display ---
if st.session_state.conversion_done and st.session_state.conversion_results:
    results = st.session_state.conversion_results
    st.divider()
    st.header("Conversion Results")

    # Tabs for different output types
    tab_bundle, tab_scripts, tab_connections, tab_datasets, tab_download = st.tabs(
        ["Deployment Bundle", "Glue ETL Scripts", "Connections", "Datasets", "Download All"]
    )

    with tab_bundle:
        for job_name, bundle in results["deployment_bundles"].items():
            folder_name = f"aws-glue-{job_name.lower().replace('_', '-')}"
            st.subheader(f"Bundle: `{folder_name}/`")

            # Show folder tree
            st.markdown("**Folder Structure:**")
            tree_lines = sorted(bundle.keys())
            tree_display = "\n".join(f"  {p}" for p in tree_lines)
            st.code(tree_display, language="text")

            # Show each file in expandable sections
            for filepath, content in sorted(bundle.items()):
                filename = filepath.split("/")[-1]
                with st.expander(f"{filepath}"):
                    if filename.endswith(".py"):
                        st.code(content, language="python")
                    elif filename.endswith(".json"):
                        st.code(content, language="json")
                    elif filename.endswith(".yaml") or filename.endswith(".yml"):
                        st.code(content, language="yaml")
                    elif filename == "README.md":
                        st.markdown(content)
                    else:
                        st.text(content)

    with tab_scripts:
        for df_result in results["dataflows"]:
            st.subheader(f"Job: {df_result['name']}")
            st.code(df_result["glue_script"], language="python")
            if df_result["notes"]:
                for note in df_result["notes"]:
                    st.warning(note)

    with tab_connections:
        for ls_result in results["linked_services"]:
            st.subheader(f"Connection: {ls_result['name']}")
            st.markdown(f"**ADF Type:** `{ls_result['adf_type']}`")
            st.json(ls_result["glue_connection"])
            if ls_result["notes"]:
                for note in ls_result["notes"]:
                    st.info(note)

    with tab_datasets:
        for ds_result in results["datasets"]:
            st.subheader(f"Dataset: {ds_result['name']}")
            st.markdown(f"**ADF Type:** `{ds_result['adf_type']}`")
            if ds_result["glue_code_snippet"]:
                st.code(ds_result["glue_code_snippet"], language="python")
            if ds_result["glue_catalog_entry"]:
                st.json(ds_result["glue_catalog_entry"])
            if ds_result["notes"]:
                for note in ds_result["notes"]:
                    st.info(note)

    with tab_download:
        st.subheader("Download Deployment Bundles")
        st.markdown("Each bundle is a self-contained folder ready for your AWS teammate to deploy.")

        for job_name, bundle in results["deployment_bundles"].items():
            folder_name = f"aws-glue-{job_name.lower().replace('_', '-')}"
            st.markdown(f"### {folder_name}/")

            # Build ZIP in memory
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for filepath, content in bundle.items():
                    zf.writestr(filepath, content)

            zip_buffer.seek(0)
            st.download_button(
                label=f"Download {folder_name}.zip",
                data=zip_buffer.getvalue(),
                file_name=f"{folder_name}.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True,
                key=f"dl_{job_name}",
            )

            st.markdown(f"""
```
{folder_name}/
│
├── scripts/
│   └── {job_name}.py                    # PySpark Glue ETL script
│
├── data/
│   └── (source data files)              # Upload to S3 before running
│
├── iam/
│   └── glue_trust_policy.json           # IAM role + trust + permissions
│
├── cloudformation/
│   └── {job_name.lower()}-glue-job.yaml # Full CloudFormation stack
│
└── README.md                            # Step-by-step deployment guide
```
            """)
            st.divider()
