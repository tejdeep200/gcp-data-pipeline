# pipeline/main.py
import argparse
import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParseAndValidateDoFn(beam.DoFn):
    def process(self, element):
        try:
            # element may be bytes (from Pub/Sub) or string (from file)
            if isinstance(element, bytes):
                element = element.decode('utf-8')
            record = json.loads(element)
            required = ['review_id', 'user_id', 'business_id', 'stars', 'text', 'date']
            for r in required:
                if r not in record:
                    # skip invalid records
                    return
            output = {
                'review_id': str(record.get('review_id')),
                'user_id': str(record.get('user_id')),
                'business_id': str(record.get('business_id')),
                'rating': int(record.get('stars', 0)),
                'text': record.get('text', ''),
                'date': record.get('date'),
                'ingest_ts': datetime.utcnow().isoformat() + 'Z'
            }
            yield output
        except Exception:
            logger.exception("Failed to parse record")
            return

def to_json_str(record):
    return json.dumps(record, ensure_ascii=False)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=False)
    parser.add_argument('--region', required=False)
    parser.add_argument('--temp_location', required=False)
    parser.add_argument('--staging_location', required=False)
    parser.add_argument('--input_subscription', default=None)
    parser.add_argument('--input_gcs_pattern', default=None)
    parser.add_argument('--output_bq_table', required=False)  # e.g., project:dataset.reviews
    parser.add_argument('--streaming', action='store_true')
    parser.add_argument('--dry_run', action='store_true', help='If set, write JSON lines to --dry_run_output instead of BigQuery')
    parser.add_argument('--dry_run_output', default='output/dry_run_out', help='prefix for WriteToText when --dry_run')
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    if args.project:
        options.view_as(GoogleCloudOptions).project = args.project
    if args.region:
        options.view_as(GoogleCloudOptions).region = args.region
    options.view_as(StandardOptions).streaming = args.streaming

    with beam.Pipeline(options=options) as p:
        if args.streaming and args.input_subscription:
            raw = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription).with_output_types(bytes)
        elif args.input_gcs_pattern:
            raw = p | "ReadFromGCS" >> beam.io.ReadFromText(args.input_gcs_pattern)
        else:
            raise RuntimeError("Provide either --input_subscription (streaming) or --input_gcs_pattern (batch)")

        parsed = raw | "ParseAndValidate" >> beam.ParDo(ParseAndValidateDoFn())

        if args.dry_run:
            # write newline-delimited json strings for inspection
            parsed | "ToJsonStr" >> beam.Map(to_json_str) \
                   | "WriteDryRun" >> beam.io.WriteToText(args.dry_run_output, file_name_suffix=".jsonl", shard_name_template="-SS-of-NN")
        else:
            if not args.output_bq_table:
                raise RuntimeError("output_bq_table is required unless --dry_run is used")
            schema = {
              'fields': [
                {'name': 'review_id','type':'STRING','mode':'REQUIRED'},
                {'name': 'user_id','type':'STRING','mode':'NULLABLE'},
                {'name': 'business_id','type':'STRING','mode':'NULLABLE'},
                {'name': 'rating','type':'INT64','mode':'NULLABLE'},
                {'name': 'text','type':'STRING','mode':'NULLABLE'},
                {'name': 'date','type':'DATE','mode':'NULLABLE'},
                {'name': 'ingest_ts','type':'TIMESTAMP','mode':'NULLABLE'}
              ]
            }
            parsed | "WriteToBQ" >> WriteToBigQuery(
                args.output_bq_table,
                schema=schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )

if __name__ == '__main__':
    run()

