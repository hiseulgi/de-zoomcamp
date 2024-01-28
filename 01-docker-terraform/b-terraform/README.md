# Terraform + GCP Setup

## Walkthrough Guide

1. Create a new project in GCP and add a billing account
2. Create a new service account with the following roles:
   - BigQuery Admin
   - Storage Admin
   - Storage Object Admin
3. Create a new key for the service account and download it as a JSON file
4. Create terraform files for the BigQuery dataset and GCS bucket (`main.tf` and `variables.tf`)
5. Terraform commands:
    - `terraform init`: Initialize a working directory containing Terraform configuration files
    - `terraform plan`: Generate and show an execution plan
    - `terraform apply`: Builds or changes infrastructure
    - `terraform destroy`: Destroy Terraform-managed infrastructure
    - `terraform fmt`: Rewrites config files to canonical format (Optional)

## Acknowledgements
- [Data Engineering Zoomcamp - Local Setup Terraform](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp)