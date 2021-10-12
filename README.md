[![build](https://github.com/gmodena/spark-privacy/actions/workflows/build.yml/badge.svg?branch=devel-wip)](https://github.com/gmodena/spark-privacy/actions/workflows/build.yml)

# spark-privacy

Distributed differential privacy on top of Apache Spark and Google's [differential-privacy](https://github.com/google/differential-privacy/tree/main/examples/java) Java API.

## Build

Thin and fat jars can be built with
```bash
./gradlew build
```

or
```bash
./gradlew shadow
```

## Example

An example of private aggregation is available under `examples`.