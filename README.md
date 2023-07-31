# Gluten: Plugin to Double Trino's Performance

This plugin is still under active development now, and doesn't have a stable release. Welcome to evaluate it. If you encounter any issues or have any suggestions, please submit to our issue list. We'd like to hear your feedback

## Introduction

This repo provides a Trino plugin to enable Velox native compute engine for Trino.

## Archtecture

(to be added)

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
cd ${REPO}/trino-cpp-plugin
mvn clean package    -DskipTests=True
```

## how to use

1. copy `libtrino_bridge.so` to your system `LD_LIBRARY_PATH`
2. add `trino-cpp-plugin-411.jar` to your classpath
3. start trino.

