# dbt

The heavy transformations are performed in **dbt**, it takes as inputs an array of dirty tables and outputs a fully curated dataset ready to be ingested by BI analysts.

![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_1.png)
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_2.png)
![](https://github.com/VicenteYago/steam-data-engineering/blob/main/img/lineage_dbt_3.png)

## Some transformations

- some tables, like `developers` do not have an ID, so the `developers.name` has to be used for this purpose, with all the problems it have, since same developers are not always named equally. For example `"FromSoftware, Inc."` and `"Fromsoftware inc ®"` are both transformed into `fromsoftware` using **macros**.
- extracting dates from text
- some columns needed to be splitted, i.e. `owners` in `steam_spy_scrap` was represented as : 

<p align="center">
<img src="https://user-images.githubusercontent.com/16523144/190527411-9fd2439e-3516-4199-97ef-9fda8fd733b3.png" width="100" height="100">
</p>




- casts
- 
-
-
-

## 2 enviroments



## Tests
