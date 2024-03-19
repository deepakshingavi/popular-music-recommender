# Popular Music Recommender

This project is a Music log Processor that includes Spark-based processing. 
Below are some useful commands for building, testing, and running the application.

## Prerequisites
Before running this application, ensure that you have the following installed:
- [JDK 1.8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
- [Scala 2.12](https://www.scala-lang.org/download/2.12.14.html)
- You need the following for executing the Spark Job:
    - [Gradle](https://gradle.org/install/) for building and testing the project.
    - [Apache Spark](https://spark.apache.org/downloads.html) for submitting the jar to local Spark cluster processing.


## Run Unit Test

To run the unit tests, use the following command:
```bash
./gradlew test
```

## Package

To package the application, use the following command:
```bash
./gradlew build
```
This should build jar at `build/libs/popular-music-recommender-1.0.jar` location

## Run Spark Job using SBT cmd
To start Spark processor using spark-submit command
* `--input` : Directory of the input files
* `--output` : Directory for the output files
* `jar-path` : Refer the jar path which was build in previous step
```bash
/opt/spark/bin/spark-submit --driver-memory 2g --class com.dp.example.StartMusicRecommender --master local[*] <jar-path> --input /app/input/ --output /tmp/output/
```
Note : Driver will require at least 2G of memory in order rto process the entire sample data of size 2.53 GB.


## Check the output
Once the program has finished it execution it should create the result file in path mentioned in `--output` directory.
```bash
# List the files in Output directory.
ls -la /tmp/output/

# View the file content
cat /tmp/output/part-*.csv
```

## Output
**CSV file** - This will be a tab separated CSV file with columns `trackName` and `no_of_times_played` where records are sort in descending order to list top ranked Track names.
```
|-----------------------------------------|---------------------|
| trackName                               | no_of_times_played  |
|-----------------------------------------|---------------------|
| Jolene                                  | 1215                |
| Heartbeats                              | 868                 |
| How Long Will It Take                   | 726                 |
| Anthems For A Seventeen Year Old Girl   | 659                 |
| St. Ides Heaven                         | 646                 |
| Bonus Track                             | 644                 |
| Starin' Through My Rear View            | 617                 |
| Beast Of Burden                         | 613                 |
| The Swing                               | 604                 |
| See You In My Nightmares                | 536                 |
|-----------------------------------------|---------------------|
```

### Assumptions
1. Everytime program it's ok to overwrite the output directory.


### Improvement
1. For advanced schema evolution, such as dynamic typing, AVRO is recommended as a superior file format.
2. There can be Data Quality flag which is true when all the values in the row passes data contract checks (e.g. not null values). This can be used by consumers to consume only valid data.
3. If this dataset is going to be frequently filter, joined and aggregated based on sessionId then it should be saved by partitioning based on sessionId for read optimisation. 

