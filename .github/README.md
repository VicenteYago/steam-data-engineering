
- https://github.com/google-github-actions/auth

```{bash}
export PROJECT_ID="my-project" # update with your value

gcloud iam service-accounts create "my-service-account" \
  --project "${PROJECT_ID}"

gcloud services enable iamcredentials.googleapis.com \
  --project "${PROJECT_ID}"
  
gcloud iam workload-identity-pools create "my-pool" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="Demo pool"
  
gcloud iam workload-identity-pools describe "my-pool" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --format="value(name)"
```

```{bash}
export WORKLOAD_IDENTITY_POOL_ID="..." # value from above
```

```{bash}
gcloud iam workload-identity-pools providers create-oidc "my-provider" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="my-pool" \
  --display-name="Demo provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com"
```

```{bash}
export REPO="VicenteYago/steam-data-engineering" 
```

```{bash}
gcloud iam service-accounts add-iam-policy-binding "my-service-account@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${WORKLOAD_IDENTITY_POOL_ID}/attribute.repository/${REPO}"
```
