# Airflow

- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

```{bash}
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d 
```

```{bash}
docker compose down --volumes --rmi all
```
