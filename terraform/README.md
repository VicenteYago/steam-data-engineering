# Terraform

Install terraform, valut: 

- https://learn.hashicorp.com/tutorials/terraform/install-cli
- https://learn.hashicorp.com/tutorials/terraform/secrets-vault

Start server:
```{bash}
vault server -dev -dev-root-token-id="steam"
```
Export credentials
```{bash}
export TF_VAR_aws_access_key=<AWS_ACCESS_KEY_ID>
export TF_VAR_aws_secret_key=<AWS_SECRET_ACCESS_KEY>
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=steam
```
