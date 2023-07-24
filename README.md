# Trino Cpp Plugin

This repo provide a Trino plugin to enable Velox native compute engine for Trino.

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

