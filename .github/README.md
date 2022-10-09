
- https://github.com/google-github-actions/auth

```{bash}
export PROJECT_ID="steam-data-engineering-gcp" # update with your value

gcloud iam service-accounts create "github-actions-service-account" \
  --project "${PROJECT_ID}"

gcloud services enable iamcredentials.googleapis.com \
  --project "${PROJECT_ID}"
  
gcloud iam workload-identity-pools create "github-action-pool" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="Github Actions pool"
  
gcloud iam workload-identity-pools describe "github-action-pool" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --format="value(name)"
```

```{bash}
export WORKLOAD_IDENTITY_POOL_ID="projects/146724372394/locations/global/workloadIdentityPools/github-action-pool"
```

```{bash}
gcloud iam workload-identity-pools providers create-oidc "github-action-provider" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github-action-pool" \
  --display-name="Github Action provider" \
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
