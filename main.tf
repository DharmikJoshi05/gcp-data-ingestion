# Provider configuration for Google Cloud Platform
provider "google" {
  project = "rugged-cooler-375520"
  credentials = file("/Users/dharmik/test/terraform-gcp/rugged-cooler-375520-c2abf72c6ba9.json")
  region  = "us-central1"
}

# Create a new Google Cloud Storage bucket
resource "google_storage_bucket" "example_bucket" {
  name          = "sep721-weather-data-bucket"
  location      = "US"
  storage_class = "STANDARD"
}

# Create a BigQuery dataset to import CSV files
resource "google_bigquery_dataset" "example_dataset" {
  dataset_id = "weather_dataset"
}

# Create a service account to perform the above actions
resource "google_service_account" "example_service_account" {
  account_id   = "weather-data-service-account"
  display_name = "Weather Data Service Account"
}

# Grant the service account permission to access the Storage bucket
resource "google_storage_bucket_iam_member" "example_bucket_iam_member" {
  bucket = google_storage_bucket.example_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.example_service_account.email}"
}

resource "google_bigquery_dataset_iam_member" "example_dataset_iam_member" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.example_service_account.email}"
}

resource "google_bigquery_dataset_iam_member" "example_dataset_iam_member_2" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  role       = "roles/bigquery.admin"
  member     = "serviceAccount:${google_service_account.example_service_account.email}"
}

# Grant the service account permission to access the BigQuery dataset
resource "google_bigquery_dataset_access" "example_dataset_access" {
  dataset_id        = google_bigquery_dataset.example_dataset.dataset_id
  role              = "OWNER"
  user_by_email     = google_service_account.example_service_account.email

}

