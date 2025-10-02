# Compile and run IntervalJoin_HP-Calibration

## Compile
mvn clean install
mvn package

## Configuration of tests

### Parameters
```
--parallelism <nRSource,nLSource,nJoin,nSink>
--type <r_dataset_pathname>
--lower <lower bound in ms>
--upper <upper bound in ms>
[--hybrid <max splitting degree>]
[--load <threshold>]
[--chaining]
```

## Example
```
java -jar target/IntervalJoin_DP-1.0.jar --parallelism 1,1,1,1 --type r_filedataset.txt --lower -500 --upper 500
```

In the example above, we start the program with parallelism 1 for each operator (Right Source, Left Source, Join, Sink). The interval range is computed as [timestamp-500, timestamp+500] in ms precision. We define the dataset file for the right stream (the filename of the left stream is built by replacing 'r_' with 'l_' at the beginning of the filename).
