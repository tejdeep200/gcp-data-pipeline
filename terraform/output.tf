output "raw_bucket" {
  value = google_storage_bucket.raw.name
}
output "staging_bucket" {
  value = google_storage_bucket.staging.name
}
output "temp_bucket" {
  value = google_storage_bucket.temp.name
}
output "pubsub_topic" {
  value = google_pubsub_topic.yelp_topic.name
}
output "bq_dataset" {
  value = google_bigquery_dataset.dataset.dataset_id
}

