# Gluten-Trino: Plugin to Double Trino's Performance

This plugin is still under active development now, and doesn't have a stable release. Welcome to evaluate it. If you encounter any issues or have any suggestions, please submit to our issue list. We'd like to hear your feedback

## Introduction

This repo provides a Trino plugin to enable Velox native compute engine for Trino. This project is based on [Trino](https://github.com/trinodb/trino) and [Velox](https://github.com/facebookincubator/velox). Meanwhile we referred a lot from [Prestissimo](https://github.com/prestodb/presto/tree/master/presto-native-execution). We appreciate the excellent work from Trino, Velox and Prestissimo.

## Archtecture

Gluten-Trino provides task level native offload mechanism via JNI. We will try to translate trino logical plan fragment into native acceptable formats, if the whole plan node tree are supported to be executed in native side, it will offload, otherwise it will fall back to vanilla Java path to execute. Below is Gluten Trino archtecture diagram.

![alt diagram](./docs/gluten-trino-arch.png "Gluten-Trino-Arch")

### Plan Fragment Conversion

To minimize develop effort, we reused Prestissimo Plan represenatation and serde framework. Original Prestissmo plan conversion code path is like:
```
PlanFragment(Presto-Java) -> Json -> PlanFragment(Presto-cpp) -> PlanFragment(Velox)
```
Our solution is translate Trino PlanFragment to Presto-like PlanFragment, and reuse later code path. 
```
PlanFragment(Trino-Java) -> PlanFragment(Trino-Java, Presto like) -> Json -> PlanFragment(Trino-cpp) -> PlanFragment(Velox)
```

### Data 
For data source, we will convert connector/split info and pass to native, leverage Velox connector to fetch data from data source. Currently, only hive connector is supported.

For exchange and output data, we will serialize Velox RowVector to trino protocal binary data at native side, and Trino java side will wrap this to ByteBuffer and wait for downstream task to pull data. Downstream native exchange node will pull data and deserialize to Velox RowVector.

### Current Status

We can pass all TPC-H queries with all plan fragments offloaded. TPC-DS support is WIP.
## Get the Source
```
git clone --recursive https://github.com/oap-project/gluten-trino.git
cd libraries.databases.thirdparty.trino-velox-bridge
# if you are updating an existing checkout
git submodule sync --recursive
git submodule update --init --recursive
```

## Setting up on x86_64 Linux (Ubuntu 20.04 or later)
```
# python3
$ pip install regex pyyaml chevron clang-format
$ apt-get install jq
```

## how to build

### build CPP

```
cd ${REPO}/trino-cpp
export TRINO_HOME=/path/to/trino/repo 
make depends # this will install all dependency 
make release
```
Once build completed, you can get `src/libtrino_bridge.so` under build dir.

### build Java

```
cd ${REPO}/GlutenTrinoPlugin
mvn clean package -DskipTests=True -Dair.check.skip-duplicate-finder=True
```

## how to use

1. copy `libtrino_bridge.so` to your system `LD_LIBRARY_PATH`
2. modify `launcher.py`, add `gluten-trino-plugin-411.jar` to your classpath, make sure it is the first.
3. add such configs to your `config.properties` file:
```
query.launch-native-engine=true 
optimizer.optimize-hash-generation=false 
enable-dynamic-filtering=true
trino-cpp.plugin.dir=/path/to/gluten-trino-plugin-411.jar
```
4. start trino.

