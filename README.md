README — GCP Streaming & Batch Pipeline (Yelp JSON)
Stack: Google Cloud Platform (Pub/Sub, Cloud Storage, Dataflow), Apache Beam (Python), BigQuery, Data Studio, Looker, Terraform (plus manual CLI/Console steps).
Goal: Ingest Yelp JSON (streaming & batch), transform with Beam/Dataflow, store curated tables in BigQuery, visualize with Data Studio & Looker, and expose processed data for downstream APIs / ML.
This README is an end-to-end guide to build, deploy, and extend the pipeline from scratch. Follow sections in order.
________________________________________
Table of contents
1.	Overview
2.	Prerequisites
3.	High-level Architecture
4.	GCP Project & IAM Setup (Manual & Terraform)
5.	Storage & Messaging Setup (GCS, Pub/Sub)
6.	BigQuery Schema & Partitioning
7.	Apache Beam (Python) Dataflow Pipeline
8.	Deploying the Dataflow Job (Manual & Terraform)
9.	Visualization: Data Studio & Looker
10.	Downstream APIs & ML Integration
11.	CI/CD Recommendations
12.	Monitoring, Logging & Cost Controls
13.	Security & Best Practices
14.	Appendix: Sample Terraform & Beam Code Links / Snippets
________________________________________
Overview
You will build a pipeline that supports:
•	Real-time ingestion via Pub/Sub (publish JSON messages).
•	Batch ingestion via Google Cloud Storage (gcsfuse or gsutil cp).
•	Processing: Apache Beam (Python) pipelines executed on Dataflow, with transformations, enrichment, validation, and writing to BigQuery.
•	Analytical storage: BigQuery (partitioned & clustered tables).
•	Visualization: Google Data Studio (Looker Studio) dashboards and Looker reports.
•	Downstream: Serve curated datasets to APIs and ML models (e.g., Vertex AI), with provisioning and pattern suggestions.
________________________________________
Prerequisites
•	GCP account and billing enabled.
•	gcloud, gsutil, terraform installed locally.
•	Python 3.8+ (3.9 recommended) and pip.
•	Apache Beam Python SDK (provided in the project requirements).
•	Optional: Looker access and workspace (Looker admin).
•	Basic familiarity with BigQuery, IAM, Dataflow concepts.
________________________________________
High-level Architecture
(Refers to your diagram)
1.	Source: Yelp JSON (stream — Pub/Sub; batch — GCS).
2.	Pub/Sub (topic + subscription) for streaming.
3.	GCS for raw batch storage.
4.	Apache Beam pipeline (Python) authoring transforms for both streaming and batch.
5.	Dataflow runner to execute pipelines with autoscaling workers.
6.	BigQuery for curated datasets (partitioned + clustered).
7.	Data Studio & Looker for visualization and business reports.
8.	Downstream APIs / ML read BigQuery or exported data.
________________________________________
GCP Project & IAM Setup (Manual & Terraform)
Recommended service accounts & roles
1.	dataflow-sa@<PROJECT>.iam.gserviceaccount.com — for Dataflow jobs
o	Roles: roles/dataflow.worker, roles/storage.objectAdmin (or limited write to staging bucket), roles/bigquery.dataEditor (or least-privileged equivalent), roles/pubsub.subscriber (for streaming), roles/iam.serviceAccountUser (if impersonation).
2.	pubsub-publisher-sa — if you have a producer service pushing to Pub/Sub.
o	Role: roles/pubsub.publisher.
3.	dataproc-sa / vertex-ai-sa — if you integrate later for ML.
Principle: grant least privileges. Use custom roles if needed.
Manual (Console / CLI) steps — quick
1.	Create project:
gcloud projects create my-yelp-pipeline --name="Yelp Pipeline"
gcloud config set project my-yelp-pipeline
gcloud billing projects link my-yelp-pipeline --billing-account=YOUR_BILLING_ID
2.	Enable APIs:
gcloud services enable compute.googleapis.com dataflow.googleapis.com storage.googleapis.com pubsub.googleapis.com bigquery.googleapis.com iam.googleapis.com
3.	Create service account:
gcloud iam service-accounts create dataflow-sa --display-name "Dataflow service account"
4.	Assign roles:
gcloud projects add-iam-policy-binding my-yelp-pipeline \
  --member="serviceAccount:dataflow-sa@my-yelp-pipeline.iam.gserviceaccount.com" \
  --role="roles/dataflow.worker"
# add storage and bigquery roles similarly
Terraform skeleton (recommended)
Create main.tf with provider and resources (snippet in Appendix). Key resources:
•	google_storage_bucket (raw + staging + temp)
•	google_pubsub_topic & google_pubsub_subscription
•	google_bigquery_dataset & google_bigquery_table (you can create tables via DDL later)
•	google_service_account and google_project_iam_binding / google_project_iam_member
Example (short):
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-raw-bucket"
  location = var.region
  uniform_bucket_level_access = true
}

resource "google_pubsub_topic" "yelp_stream" {
  name = "yelp-json-topic"
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-sa"
  display_name = "Dataflow SA"
}

# IAM bindings to attach roles...
Use terraform init / plan / apply.
________________________________________
Storage & Messaging Setup (GCS, Pub/Sub)
Pub/Sub (streaming)
•	Create topic: yelp-json-topic.
•	Create subscription(s): yelp-json-sub (push or pull). For Dataflow streaming, use pull subscriptions.
•	Configure publisher(s) to publish JSON messages with attributes optionally (source, ingest_ts).
GCS (batch)
•	Buckets:
o	gs://<project>-raw — store raw JSON files (ingest).
o	gs://<project>-staging — Dataflow staging.
o	gs://<project>-temp — Dataflow temp.
•	Upload sample files:
gsutil cp sample_yelp_file.json gs://my-yelp-pipeline-raw/
Folder structure (recommended)
gs://my-yelp-pipeline-raw/yyyy=2025/mm=10/dd=07/data.json
This folder partitioning helps batch processing.
________________________________________
BigQuery Schema & Partitioning
Recommended tables
1.	dataset.raw_yelp — raw JSON (optional; or keep raw in GCS)
o	Schema: store raw JSON as JSON type or STRING + ingestion metadata
2.	dataset.reviews — curated review facts
o	Columns: review_id STRING, user_id STRING, business_id STRING, rating INT64, text STRING, date DATE, city STRING, state STRING, ingest_timestamp TIMESTAMP
o	Partition by date (DATE) or ingest_timestamp (DAY) and cluster on business_id, user_id.
3.	dataset.businesses — business dimension
o	Columns: business_id, name, address, city, state, categories ARRAY<STRING>, latitude, longitude
4.	dataset.users — user dimension
o	Columns: user_id, name, review_count, yelping_since, etc.
5.	dataset.aggregates — precomputed metrics (daily aggregates) for fast dashboards
Partitioning & Clustering
•	Partition tables by date (ingest or event date).
•	Cluster on high-cardinality filter columns (business_id, city) to optimize queries and reduce costs.
________________________________________
Apache Beam (Python) Dataflow Pipeline
Below is a production-ready skeleton pipeline supporting both streaming and batch modes. It includes validation, enrichment, and writes to BigQuery.
Save this as pipeline/main.py. See Appendix for complete code + requirements.txt.
# pipeline/main.py
import argparse
import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

class ParseAndValidateDoFn(beam.DoFn):
    def process(self, element):
        # element: Pub/Sub message payload (bytes or str) or GCS line
        try:
            if isinstance(element, bytes):
                element = element.decode('utf-8')
            record = json.loads(element)
            # Basic validation — adjust for your schema
            required = ['review_id', 'user_id', 'business_id', 'stars', 'text', 'date']
            for r in required:
                if r not in record:
                    return
            # Normalize / enrich
            record_out = {
                'review_id': record['review_id'],
                'user_id': record['user_id'],
                'business_id': record['business_id'],
                'rating': int(record.get('stars', 0)),
                'text': record.get('text', ''),
                'date': record.get('date'),
                'ingest_ts': datetime.utcnow().isoformat() + 'Z'
            }
            yield record_out
        except Exception as e:
            logging.exception("Failed to parse: %s", e)
            return

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--input_subscription', default=None)
    parser.add_argument('--input_gcs_pattern', default=None)
    parser.add_argument('--output_bq_table', required=True)  # e.g., project:dataset.reviews
    parser.add_argument('--streaming', action='store_true')
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=args.streaming)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project
    google_cloud_options.region = args.region
    google_cloud_options.staging_location = args.staging_location
    google_cloud_options.temp_location = args.temp_location

    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = args.streaming

    with beam.Pipeline(options=options) as p:
        if args.streaming and args.input_subscription:
            raw = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription).with_output_types(bytes)
        elif args.input_gcs_pattern:
            raw = p | "ReadFromGCS" >> beam.io.ReadFromText(args.input_gcs_pattern)
        else:
            raise RuntimeError("Provide either input_subscription (streaming) or input_gcs_pattern (batch)")

        parsed = (
            raw
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidateDoFn())
        )

        # Optional: write raw records to staging table
        # parsed | 'WriteRawBQ' >> WriteToBigQuery(...)

        # Write curated records to BigQuery
        parsed | "WriteToBQ" >> WriteToBigQuery(
            args.output_bq_table,
            schema={
              'fields': [
                {'name': 'review_id','type':'STRING','mode':'REQUIRED'},
                {'name': 'user_id','type':'STRING','mode':'NULLABLE'},
                {'name': 'business_id','type':'STRING','mode':'NULLABLE'},
                {'name': 'rating','type':'INT64','mode':'NULLABLE'},
                {'name': 'text','type':'STRING','mode':'NULLABLE'},
                {'name': 'date','type':'DATE','mode':'NULLABLE'},
                {'name': 'ingest_ts','type':'TIMESTAMP','mode':'NULLABLE'}
              ]
            },
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run()
requirements.txt
apache-beam[gcp]==2.50.0  # pick compatible version
google-cloud-bigquery
google-cloud-storage
Important pipeline notes
•	For streaming, use --streaming --input_subscription projects/PROJECT/subscriptions/SUB_NAME.
•	For batch: --input_gcs_pattern gs://bucket/path/*.json.
•	Use Dataflow templates for repeated runs or use Dataflow flex templates for more control.
________________________________________
Deploying the Dataflow Job (Manual & Terraform)
Manual (via gcloud)
python -m pip install -r requirements.txt

# Example batch run
python pipeline/main.py \
  --project=my-yelp-pipeline \
  --region=us-central1 \
  --staging_location=gs://my-yelp-pipeline-staging/staging \
  --temp_location=gs://my-yelp-pipeline-temp/temp \
  --input_gcs_pattern=gs://my-yelp-pipeline-raw/*.json \
  --output_bq_table=my-yelp-pipeline.dataset.reviews

# Example streaming run (on Dataflow)
python pipeline/main.py \
  --project=my-yelp-pipeline \
  --region=us-central1 \
  --staging_location=gs://my-yelp-pipeline-staging/staging \
  --temp_location=gs://my-yelp-pipeline-temp/temp \
  --input_subscription=projects/my-yelp-pipeline/subscriptions/yelp-json-sub \
  --streaming \
  --output_bq_table=my-yelp-pipeline.dataset.reviews \
  --runner=DataflowRunner
When running on Dataflow, set --runner=DataflowRunner and appropriate --worker_region, --worker_zone, and machine type flags if needed:
--worker_machine_type=n1-standard-4
--num_workers=2
--max_num_workers=50
--autoscaling_algorithm=THROUGHPUT_BASED
Terraform approach for Dataflow templates
•	Build containerized pipeline or Flex Template — push to GCS and create a Dataflow job resource via Terraform or use gcloud dataflow jobs run referencing template.
•	Terraform resource: google_dataflow_job (if using templates), or use null_resource with local-exec to invoke gcloud dataflow jobs run.
________________________________________
Visualization: Data Studio & Looker
Data Studio (Looker Studio)
1.	Connect to BigQuery dataset (use the curated tables).
2.	Create data source in Data Studio using a BigQuery custom query for aggregated metrics:
o	Daily review counts, average rating by city, top businesses by review count, sentiment metrics (if present).
3.	Recommended charts:
o	Time series for daily average rating.
o	Bar chart: top 10 businesses by review count.
o	Geo map: reviews by city/state.
o	Table: recent reviews with text snippet.
4.	Share dashboards with stakeholders and set up scheduled refresh (Data Studio uses cached results; BigQuery billing applies).
Looker
1.	Create a Looker project (if you have Looker).
2.	Model BigQuery dataset in LookML:
o	Create view definitions for reviews, businesses, users.
o	Add derived tables or persistent derived tables (PDTs) for heavy aggregations.
3.	Build Explores and Looks for product teams:
o	Precompute daily aggregates (PDT) for dashboard performance.
4.	Use access filters and permissions to control data access.
________________________________________
Downstream APIs & ML Integration
Serving curated data to APIs
•	Option 1: Build an API (Cloud Run) that queries BigQuery for real-time responses:
o	Cloud Run service connected to BigQuery with appropriate service account (roles/bigquery.dataViewer).
o	Example endpoints: /business/<id>/summary, /search?q=...
•	Option 2: Export curated data to Cloud SQL or Firestore for low-latency read use cases (if required by API SLA).
ML integrations
•	Vertex AI / AutoML:
o	Use BigQuery as training table or export to GCS as TFRecords.
o	Feature engineering: build features in Beam and write to BigQuery features_table.
o	Use Vertex Feature Store for production features.
•	Batch training / Online serving:
o	Schedule training jobs with Vertex AI using BigQuery datasets.
o	Store model predictions back to BigQuery or to a serving database (Redis, Bigtable) for ultra-low latency.
Example flow for ML
1.	Beam job computes features → writes to dataset.features (BigQuery partitioned).
2.	Vertex AI training job reads dataset.features and outputs a model to Artifact Registry / Vertex Model.
3.	Create an endpoint in Vertex for online predictions, or batch predict into dataset.predictions.
________________________________________
CI/CD Recommendations
•	Use GitHub Actions / Cloud Build to run:
o	Lint and unit tests for Beam pipelines.
o	Build and publish container image (if you containerize pipeline).
o	Build Dataflow Flex Templates and push to GCS.
o	Terraform plan on PR and apply on merge to main (via GitHub Actions with limited service account key).
•	Keep secrets in Secret Manager; use workload identity where possible.
________________________________________
Monitoring, Logging & Cost Controls
•	Enable Stackdriver Logging & Monitoring (Cloud Monitoring).
•	Create alerts:
o	Dataflow job failure / low throughput / worker crashes.
o	Pub/Sub subscription backlog threshold.
o	BigQuery daily cost exceed budget.
•	Use labels on resources (env=prod,team=data) to attribute billing.
•	Use BigQuery cost controls: query quotas, use cached results, limit exposed tables.
________________________________________
Security & Best Practices
•	Use Workload Identity instead of service account keys on GKE / Cloud Run.
•	Use private GCS buckets and uniformBucketLevelAccess.
•	Restrict dataset access in BigQuery via IAM and authorized views to avoid exposing PII.
•	Use data retention & lifecycle rules on GCS for raw data (e.g., delete after 90 days if not needed).
•	Maintain an audit log via Cloud Audit Logs for major operations.
________________________________________
Appendix — Sample Terraform & Beam Code Snippets
Terraform: minimal resources (main.tf)
variable "project_id" {}
variable "region" { default = "us-central1" }

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-raw"
  location = var.region
  uniform_bucket_level_access = true
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 365 }
  }
}

resource "google_pubsub_topic" "yelp" {
  name = "yelp-json-topic"
}

resource "google_pubsub_subscription" "yelp_sub" {
  name  = "yelp-json-sub"
  topic = google_pubsub_topic.yelp.name
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-sa"
  display_name = "Dataflow SA"
}

# IAM bindings omitted for brevity; add storage admin, dataflow worker, bigquery.dataEditor
Beam: packaging & requirements
•	Build a setup.py or use a container image with apache-beam[gcp] and your pipeline.
•	For Flex Templates: build Dockerfile, push image, create flex template JSON and put in GCS, run via Dataflow.
Example BigQuery create dataset
bq --location=US mk --dataset --description "Yelp curated dataset" my-yelp-pipeline:dataset
________________________________________
Quick Start — Step-by-step (Minimal path)
1.	Create GCP project and enable billing + APIs.
2.	Create buckets: raw, staging, temp.
3.	Create Pub/Sub topic & subscription.
4.	Create BigQuery dataset.
5.	Create Dataflow service account and attach necessary roles.
6.	Clone repo (contains pipeline/, terraform/, docs/).
7.	Upload sample Yelp JSON to GCS or start publishing to Pub/Sub.
8.	Install deps: pip install -r requirements.txt.
9.	Run pipeline (batch first for quick validation):
10.	python pipeline/main.py --project=... --region=... --staging_location=gs://.../staging --temp_location=gs://.../temp --input_gcs_pattern=gs://.../raw/*.json --output_bq_table=project:dataset.reviews
11.	Open BigQuery: validate received rows.
12.	Build dashboards in Data Studio using project.dataset.reviews.
________________________________________
Deliverables included
•	pipeline/main.py (Beam pipeline skeleton for batch + streaming)
•	requirements.txt
•	terraform/ (skeleton main.tf, variables.tf, outputs.tf)
•	docs/README.md (this file)
•	Example gcloud commands for manual deployment and troubleshooting.
________________________________________
Next steps I can perform now (choose any; I will deliver immediately)
•	Generate the full pipeline/main.py file with additional transforms (dedupe, error handling, schema evolution).
•	Create full Terraform main.tf, variables.tf, and outputs.tf.
•	Produce Data Studio dashboard steps and sample LookML model snippet for Looker.
•	Provide a CI/CD GitHub Actions workflow for building & deploying Dataflow templates + Terraform.
Tell me which of these to produce next and I’ll include the full file(s) in the chat.

