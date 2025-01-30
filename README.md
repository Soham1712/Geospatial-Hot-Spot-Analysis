# Geospatial-Hot-Spot-Analysis

1. **Hot Zone Analysis** - Performing a range join operation between a rectangle dataset and a point dataset to calculate the density of points within each rectangle.
2. **Hot Cell Analysis** - Identifying statistically significant spatial hot spots using **Getis-Ord G-statistic** on a spatio-temporal big dataset.

## Repository Structure
```
|-- src/
    |-- main/
        |-- scala/
            |-- HotZoneAnalysis.scala
            |-- HotZoneUtils.scala
            |-- HotCellAnalysis.scala
            |-- HotCellUtils.scala
            |-- Main.scala
|-- target/
|-- CSE511-assembly-0.1.0.jar
|-- README.md
```

## Dependencies & Setup
- **Apache Spark**
- **Scala 2.11**
- **SBT (Scala Build Tool)**
- **Docker** (optional, if using provided environment)

If you don't have Spark installed, follow [Apache Spark Installation Guide](https://spark.apache.org/downloads.html).


## Downloading the Data
The dataset for this assignment consists of NYC Taxi trip pickup locations. You can download the necessary data files from the following sources:
- **NYC Taxi Data** (2009-2012): [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Select the required monthly dataset (e.g., `yellow_tripdata_2009-01.csv`).
- Pre-process the dataset to extract only the pickup locations in the required format.
- Save the processed dataset as `yellow_tripdata_YYYY-MM_point.csv`.

## Running the Project
### 1. Compiling and Packaging
To build the project and create the executable JAR file, run:
```sh
sbt assembly
```
This generates `geospatial-assembly-0.1.0.jar` inside `./target/scala-2.11/`.

### 2. Running the Spark Job
#### Hot Zone Analysis
```sh
spark-submit geospatial-assembly-0.1.0.jar result/output hotzoneanalysis <path_to_point_data> <path_to_zone_data>
```
Example:
```sh
spark-submit geospatial-assembly-0.1.0.jar result/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv
```

#### Hot Cell Analysis
```sh
spark-submit geospatial-assembly-0.1.0.jar result/output hotcellanalysis <path_to_nyc_taxi_data>
```
Example:
```sh
spark-submit geospatial-assembly-0.1.0.jar result/output hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv
```

## Input & Output Formats
### Hot Zone Analysis
- **Input:**
  - **Point Dataset:** NYC Taxi trip pickup locations (`point-hotzone.csv`).
  - **Zone Dataset:** Rectangle zones (`zone-hotzone.csv`).
- **Output:**
  - Rectangles sorted in ascending order with their respective counts.

### Hot Cell Analysis
- **Input:** NYC Taxi trip pickup locations for a month (`yellow_tripdata_YYYY-MM_point.csv`).
- **Output:**
  - Top 50 hot cells sorted by their Getis-Ord G-score (coordinates only, no G-score output).

Example output:
```
-7399,4075,15
-7399,4075,29
-7399,4075,22
```

## Debugging & Development
### Running in an IDE
1. Open the project in **IntelliJ IDEA** (with Scala plugin installed) or VS Code.
2. Set up the **Scala SDK and SBT**.
3. Run `Main.scala` with appropriate parameters.

## Contribution
Feel free to fork and contribute! Open issues or pull requests if you encounter problems.

## License
MIT License

