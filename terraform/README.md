# Terraform

## Config

The **Terraform** service uses **Vault** to safely provide temporal **AWS** credentials

Install terraform, valut: 

- https://learn.hashicorp.com/tutorials/terraform/install-cli
- https://learn.hashicorp.com/tutorials/terraform/secrets-vault

Start server:

```{bash}
cd terraform
vault server -dev -dev-root-token-id="steam"
```
Export credentials
```{bash}
export TF_VAR_aws_access_key=<AWS_ACCESS_KEY_ID>
export TF_VAR_aws_secret_key=<AWS_SECRET_ACCESS_KEY>
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=steam
```

Initialize admin workspace: 

```{bash}
cd vault-admin-workspace/
terraform init
terraform apply
```
Initialize admin workspace: 
```{bash}
cd ../operator-workspace/
terraform init
terraform apply
```

## Description
`terraform apply` create de following resources: 
- GCP bucket  `steam-datalake-store-alldata`
- GCP bucket  `steam-datalake-reviews`
- BigQuery dataset `steam_raw`
- Storage transfer job from AWS from S3 bucket to GCP bucket `steam-datalake-store-alldata`
- Storage transfer job from AWS from S3 bucket to GCP bucket `steam-datalake-reviews`




