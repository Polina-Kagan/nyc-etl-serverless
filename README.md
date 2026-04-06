# NYC Taxi Serverless ETL Pipeline 

![AWS](https://img.shields.io/badge/AWS-Serverless-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Wrangling-150458?style=for-the-badge&logo=pandas&logoColor=white)

An event-driven, fully serverless Data Engineering pipeline built on AWS to extract, transform, and load (ETL) New York City Taxi trip data. The entire infrastructure is provisioned using Terraform (IaC).

## Architecture Overview

The pipeline automates the ingestion of monthly yellow taxi trip records (Parquet) and zone lookup tables (CSV), cleans the data, joins it, and stores the curated dataset for downstream analytics.
1. **Extract Lambda:** Downloads raw Parquet & CSV files from the web directly into the S3 `Raw Zone`.
2. **Transform Lambda:** Triggered dynamically, this function cleans the data, filters anomalies, performs an inner join with the lookup table, and writes the analytical-ready data to the S3 `Curated Zone` using Snappy compression.
3. **Infrastructure as Code:** All resources (S3, Lambdas, strict IAM Roles) are managed via Terraform.

## Key Engineering Highlights & Problem Solving
This project goes beyond a simple "A to B" data transfer. It addresses several real-world cloud engineering challenges:

### 1. Overcoming "Pinball" Race Conditions (Orchestration)
Relying on standard S3 `ObjectCreated` triggers often leads to race conditions (e.g., triggering the Transform function twice when both a CSV and Parquet file land). 
* **Solution:** Implemented **Direct Lambda Invocation**. The Extract Lambda acts as an orchestrator, ensuring both files exist before asynchronously triggering the Transform Lambda (`InvocationType='Event'`), saving compute time and preventing duplicate runs.

### 2. Defeating PyArrow `zstd` & Storage Limits
The AWS Managed Pandas Layer is compiled without C++ `zstd` support, causing `ArrowNotImplementedError` when reading raw taxi data.
* **Solution:** Used **Runtime Dependency Injection**. The Lambda dynamically downloads a lightweight `pyarrow==15.0.0` wheel into the ephemeral `/tmp/` storage and overrides `sys.path`. Added the `--no-deps` flag to prevent `pip` from exhausting the 512MB `/tmp/` storage limit (Errno 28).

### 3. Solving `OutOfMemory` (OOM) Exceptions in Pandas
Processing millions of rows in a 2GB memory Lambda caused the function to crash during the DataFrame `merge` operation.
* **Solution:** Implemented **Column Pruning** using `pyarrow.parquet.read_table`. By explicitly loading only the required columns before converting to Pandas, the memory footprint was reduced by ~70%, entirely eliminating OOM errors and reducing billing costs.

### 4. Zero-Trust Security (Least Privilege)
Moved away from wildcards like `AmazonS3FullAccess`. 
* **Solution:** Configured strict, custom IAM roles in Terraform. The Extract Lambda can only write to the Raw bucket, and the Transform Lambda can only read from Raw and write to Curated. 

##  Tech Stack
* **Cloud:** AWS (S3, Lambda, IAM, CloudWatch)
* **IaC:** Terraform
* **Data Processing:** Python, Pandas, PyArrow
* **Formats:** Parquet, CSV

## How to Run (Deploying the Infrastructure)

Ensure you have the AWS CLI configured with appropriate credentials and Terraform installed.

```bash
# 1. Clone the repository
git clone [https://github.com/Polina-Kagan/nyc-taxi-serverless-etl.git](https://github.com/Polina-Kagan/nyc-taxi-serverless-etl.git)
cd nyc-taxi-serverless-etl/v3_terraform

# 2. Initialize Terraform
terraform init

# 3. Review the deployment plan
terraform plan

# 4. Deploy the infrastructure to AWS
terraform apply
```

To trigger the pipeline, invoke the Extract Lambda function (nyc-extract-pipeline) with the following test event payload:
```
JSON
{
  "year": "2025",
  "month": "01"
}
```

To tear down the infrastructure and avoid AWS charges:
```
terraform destroy
```

## Baseline Performance Metrics (v1.0)
- **Processing Speed:** ~3.4 million rows processed in < 10 seconds.
- **Memory Optimization:** Successfully utilized PyArrow/Pandas within AWS Lambda (Max memory used: ~1.3 GB out of 2.0 GB allocated).
- **Data Compression:** S3 storage footprint maintained efficiently using Parquet format.


