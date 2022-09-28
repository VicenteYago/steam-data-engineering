# Airflow


## Description
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_graph.png)
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_grid.png)
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/airflow_gant.png)


## Config 

1. Create `.google/credentials/google_credentials.json`
2. Create `.dbt/profiles.yml`

```{yml}
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



- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

```{bash}
sudo docker compose build --no-cache
sudo docker compose up airflow-init
sudo docker compose up -d 
chmod 777 dbt
```

```{bash}
sudo docker compose down --volumes --rmi all
```
