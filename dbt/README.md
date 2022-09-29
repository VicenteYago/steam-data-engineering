# dbt

The heavy transformations are performed in **dbt**, it takes as inputs an array of dirty tables and outputs a fully curated dataset ready to be ingested by BI analysts.

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_1.png)
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_2.png)
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_3.png)

## Some transformations

- some tables, like `developers` do not have an ID, so the `developers.name` has to be used for this purpose, with all the problems it have, since same developers are not always named equally. For example `"FromSoftware, Inc."` and `"Fromsoftware inc ®"` are both transformed into `fromsoftware` using **macros**.
- extracting dates from text
- some columns needed to be splitted,for example `owners` is splitted into `owners.high` and `owners.low`  : 

  <p align="center">
  <img src="https://github.com/VicenteYago/steam-data-engineering/blob/main/img/owners_old.png" >
  </p>
  
  <p align="center">
  <img src="https://github.com/VicenteYago/steam-data-engineering/blob/main/img/owners_new.png" >
  </p>

- multiple type casts.

## Denormalization
Denormalized tables with nested and repeated files are implemented following  [google recommendations](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-working-joins-nested-repeated-data).

  <p align="center">
  <img src="https://github.com/VicenteYago/steam-data-engineering/blob/main/img/schema_denorm.png" >
  </p>

## 2 enviroments
As dbt recommends a **development** and **production** enviroments are used for safety.


## Tests
In progess
