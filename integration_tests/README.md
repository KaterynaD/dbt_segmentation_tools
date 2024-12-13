# This project is used for testing Segmentation Custom Materialization dbt Package

To run the project add your specific **integration_tests** profile.

dbt_utils and dbt_segmentation_tools are included in packages.yml

Run

```
dbt deps 
```

Staging data and etalon tests data are provided in **seeds** folder as csv files.

Before testing run

```
dbt seed
```

Run and Test

```
dbt run --select RFM_it

dbt test --select RFM_it
```

```
dbt run --select Kmeans_it

dbt test --select Kmeans_it
```

```
dbt run --select Gaussian_it

dbt test --select Gaussian_it
```

```
dbt run --select AgglomerativeClustering_it

dbt test --select AgglomerativeClustering_it
```


```
dbt run --select DBSCAN_it

dbt test --select DBSCAN_it
```