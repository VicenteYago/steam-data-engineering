# steam-data-engineering

Create compute engine instance: 

```{bash}
gcloud compute instances create steam-de --project=steam-data-engineering-gcp --zone=europe-southwest1-a --machine-type=e2-medium --network-interface=network-tier=PREMIUM,subnet=default --maintenance-policy=MIGRATE --provisioning-model=STANDARD --service-account=146724372394-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append --create-disk=auto-delete=yes,boot=yes,device-name=steam-de,image=projects/ubuntu-os-cloud/global/images/ubuntu-1804-bionic-v20220901,mode=rw,size=30,type=projects/steam-data-engineering-gcp/zones/europe-southwest1-a/diskTypes/pd-balanced --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any
```
