# RDD Programming with Apache Spark

Solution for the two exercises in **Lab 1: RDD Programming with Apache Spark**.

## Requirements

- Java 17 or later
- Maven 3.9 or later
- Internet connection for the first Maven build (Spark dependencies are cached locally afterwards)

The project uses Apache Spark 4.0.1 and Java RDD APIs. It includes sample inputs in `data/` so the lab can be run immediately.

## Build and run locally

From the project root:

```powershell
mvn clean package
java -jar target/rdd-programming-with-apache-spark-1.0.0.jar all
```

Run one exercise only, or supply another input file:

```powershell
java -jar target/rdd-programming-with-apache-spark-1.0.0.jar sales data/ventes.txt
java -jar target/rdd-programming-with-apache-spark-1.0.0.jar logs data/access.log
```

For a cluster run, use `spark-submit --master <master-url> --class ma.salma.spark.Main target/rdd-programming-with-apache-spark-1.0.0.jar all`; Spark's submit master is respected, and the application defaults to `local[2]` when no master is configured.

## Exercise 1: sales by city and year

`data/ventes.txt` uses one transaction per line:

```text
date city product price
```

The program loads the file as a `JavaRDD<String>`, filters malformed rows, maps each sale to `(city, amount)` and `(city, year, amount)` pairs, and reduces the pairs with `reduceByKey`. Both result sets are sorted by key for repeatable output. Prices are displayed to two decimal places.

## Exercise 2: Apache access-log analysis

`data/access.log` contains five sample records in Apache combined-log format. A regular expression extracts the client IP, timestamp, method, resource, status, response size, referrer, and user agent. Invalid lines are ignored.

The log RDD is reused for the total request count, error count and rate, top five IPs, top five requested resources, and status-code distribution. Error status is defined as 400 or above. Top-five ties are resolved alphabetically so results remain deterministic.

## Verified sample results

The local Spark run processed 9 sales transactions and 5 log entries. City sales were Casablanca 2220.50, Marrakech 1000.00, and Rabat 1860.00. The log contained 2 errors (40.00%); status counts were 200: 2, 302: 1, 404: 1, and 500: 1.

## Project layout

```text
src/main/java/ma/salma/spark/Main.java  Spark RDD applications
src/main/resources/log4j2.properties   Console logging configuration
data/ventes.txt                        Sales sample data
data/access.log                        Apache log sample data
pom.xml                                Maven build and Spark dependency
```
