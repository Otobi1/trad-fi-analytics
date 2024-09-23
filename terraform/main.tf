terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file(var.ssh_key_file)
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}

resource "google_compute_instance" "airflow_dbt_vm" {
  name         = var.vm_name
  machine_type = var.machine_type

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-pip git

    # Install and set up Airflow
    export AIRFLOW_HOME=~/airflow
    pip3 install apache-airflow==2.5.0
    airflow db init
    airflow users create --username admin --firstname Air --lastname Flow --role Admin --email admin@example.com --password admin

    # Start Airflow web server and scheduler as background services
    nohup airflow webserver > /dev/null 2>&1 &
    nohup airflow scheduler > /dev/null 2>&1 &

    # Install DBT
    pip3 install dbt-core dbt-bigquery

    # Clone the Git repository
    git clone ${var.git_repo_url} /home/${var.project_id}/project-repo/

    # Move Airflow DAGs to the appropriate directory
    mv /home/${var.project_id}/project-repo/dags/* /home/${var.project_id}/airflow/dags/

    # Move DBT projects to the appropriate directory
    mv /home/${var.project_id}/project-repo/dbt_projects/* /home/${var.project_id}/dbt_projects/
  EOT
}

# Define Storage Bucket
resource "google_storage_bucket" "data_bucket" {
  name          = var.storage_bucket_name
  location      = var.region
  force_destroy = true
}

# Define BigQuery Dataset
resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = var.bigquery_dataset_id
  location   = var.region
}
