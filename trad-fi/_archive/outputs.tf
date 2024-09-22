# Output the public IP address of the VM
output "vm_public_ip" {
  description = "The public IP address of the VM"
  value       = google_compute_instance.de-projects-orchestrator.network_interface[0].access_config[0].nat_ip
}

# Output the name of the storage bucket
output "storage_bucket_name" {
  description = "The name of the GCS storage bucket"
  value       = google_storage_bucket.data_bucket.name
}

# Output the BigQuery dataset ID
output "bigquery_dataset_id" {
  description = "The BigQuery dataset ID"
  value       = google_bigquery_dataset.my_dataset.dataset_id
}

# Output the Git repository URL (useful for logging or automation)
output "git_repo_url" {
  description = "The Git repository URL where Airflow DAGs and DBT projects are stored"
  value       = var.git_repo_url
}
