# Airflow


## Description

* **Execution graph (DAG)** : 
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_graph.png)

* **Grid** : 
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_grid.png)

* **Gantt** : 
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_gant.png)


## **Config**

### dbt config necessary to run the final task 

1. Create a service account in GCP and export credentials into `~/.google/credentials/google_credentials.json`
2. Create `~/.dbt/profiles.yml`: 

```{yml}
#profiles.yml
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: steam-data-engineering-gcp
      dataset: dbt_development
      threads: 1
      keyfile: '/home/vyago/.google/credentials/google_credentials.json'
      location: europe-southwest1

airflow:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: steam-data-engineering-gcp
      dataset: dbt_development
      threads: 1
      keyfile: '/.google/credentials/google_credentials.json'
      location: europe-southwest1
```


### Building the Airflow service

- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Built & run : 
```{bash}
sudo docker compose build --no-cache
sudo docker compose up airflow-init
sudo docker compose up -d 
chmod 777 dbt
```

Clean: 
```{bash}
sudo docker compose down --volumes --rmi all
```
