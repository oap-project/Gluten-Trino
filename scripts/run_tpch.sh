#!/bin/bash

if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

if [ -z "${TRINO_HOME}" ]; then
    echo "TRINO_HOME is not set" >&2
    exit 1
fi

if [ -z "${REPO_HOME}" ]; then
  export REPO_HOME="$(cd "`dirname "$0"`"; cd ..; pwd)"
fi

if [ -z "${TRINO_NATIVE_PATH}" ]; then
    export TRINO_NATIVE_PATH="${REPO_HOME}/cpp/build/src"
fi

if [ -z "${MAVEN_LOCAL_REPOSITORY_HOME}" ]; then
  export MAVEN_LOCAL_REPOSITORY_HOME="${HOME}/.m2/repository"
fi

queries=($(echo {1..22}))
if [ $# -gt 0 ]; then
  queries=($(echo "$@"))
fi

COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/antlr/antlr/2.7.7/antlr-2.7.7.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/aopalliance/aopalliance/1.0/aopalliance-1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/ch/qos/logback/logback-core/1.4.5/logback-core-1.4.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/amazonaws/aws-java-sdk-core/1.12.261/aws-java-sdk-core-1.12.261.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/amazonaws/aws-java-sdk-glue/1.12.261/aws-java-sdk-glue-1.12.261.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/amazonaws/aws-java-sdk-kms/1.12.261/aws-java-sdk-kms-1.12.261.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/amazonaws/aws-java-sdk-s3/1.12.261/aws-java-sdk-s3-1.12.261.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/amazonaws/aws-java-sdk-sts/1.12.261/aws-java-sdk-sts-1.12.261.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/amazonaws/jmespath-java/1.12.261/jmespath-java-1.12.261.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-core/1.25.0/azure-core-1.25.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-core-http-netty/1.11.7/azure-core-http-netty-1.11.7.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-identity/1.4.4/azure-identity-1.4.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-storage-blob/12.14.4/azure-storage-blob-12.14.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-storage-blob-batch/12.11.4/azure-storage-blob-batch-12.11.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-storage-common/12.14.3/azure-storage-common-12.14.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/azure/azure-storage-internal-avro/12.1.4/azure-storage-internal-avro-12.1.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/beust/jcommander/1.48/jcommander-1.48.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/carrotsearch/thirdparty/simple-xml-safe/2.7.1/simple-xml-safe-2.7.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/clearspring/analytics/stream/2.9.5/stream-2.9.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/esri/geometry/esri-geometry-api/2.2.4/esri-geometry-api-2.2.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.14.2/jackson-dataformat-cbor-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/dataformat/jackson-dataformat-smile/2.14.2/jackson-dataformat-smile-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/dataformat/jackson-dataformat-xml/2.13.1/jackson-dataformat-xml-2.13.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/datatype/jackson-datatype-guava/2.14.2/jackson-datatype-guava-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.14.2/jackson-datatype-jdk8-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/datatype/jackson-datatype-joda/2.14.2/jackson-datatype-joda-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/datatype/jackson-datatype-jsr310/2.14.2/jackson-datatype-jsr310-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/jackson/module/jackson-module-parameter-names/2.14.2/jackson-module-parameter-names-2.14.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/fasterxml/woodstox/woodstox-core/6.2.7/woodstox-core-6.2.7.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/ben-manes/caffeine/caffeine/3.0.5/caffeine-3.0.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/docker-java/docker-java-api/3.2.13/docker-java-api-3.2.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/docker-java/docker-java-transport/3.2.13/docker-java-transport-3.2.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/docker-java/docker-java-transport-zerodep/3.2.13/docker-java-transport-zerodep-3.2.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/luben/zstd-jni/1.5.2-3/zstd-jni-1.5.2-3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/oshi/oshi-core/5.8.5/oshi-core-5.8.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/stephenc/findbugs/findbugs-annotations/1.3.9-1/findbugs-annotations-1.3.9-1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/stephenc/jcip/jcip-annotations/1.0-1/jcip-annotations-1.0-1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/github/vertical-blank/sql-formatter/2.0.2/sql-formatter-2.0.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/api/api-common/2.1.5/api-common-2.1.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/api-client/google-api-client/1.33.2/google-api-client-1.33.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/api/gax/2.17.0/gax-2.17.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/api/gax-httpjson/0.97.2/gax-httpjson-0.97.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/api/grpc/proto-google-common-protos/2.8.3/proto-google-common-protos-2.8.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/api/grpc/proto-google-iam-v1/1.2.6/proto-google-iam-v1-1.2.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/apis/google-api-services-storage/v1-rev20220210-1.32.1/google-api-services-storage-v1-rev20220210-1.32.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/auth/google-auth-library-credentials/1.6.0/google-auth-library-credentials-1.6.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/auth/google-auth-library-oauth2-http/1.6.0/google-auth-library-oauth2-http-1.6.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/auto/value/auto-value-annotations/1.9/auto-value-annotations-1.9.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.8/gcs-connector-hadoop2-2.2.8-shaded.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/cloud/google-cloud-core/2.5.6/google-cloud-core-2.5.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/cloud/google-cloud-core-http/2.5.6/google-cloud-core-http-2.5.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/cloud/google-cloud-storage/2.5.1/google-cloud-storage-2.5.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/code/gson/gson/2.9.0/gson-2.9.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/errorprone/error_prone_annotations/2.18.0/error_prone_annotations-2.18.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/http-client/google-http-client/1.41.4/google-http-client-1.41.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/http-client/google-http-client-apache-v2/1.41.4/google-http-client-apache-v2-1.41.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/http-client/google-http-client-appengine/1.41.4/google-http-client-appengine-1.41.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/http-client/google-http-client-gson/1.41.4/google-http-client-gson-1.41.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/http-client/google-http-client-jackson2/1.41.4/google-http-client-jackson2-1.41.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/inject/guice/5.1.0/guice-5.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/protobuf/protobuf-java/3.21.6/protobuf-java-3.21.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/google/protobuf/protobuf-java-util/3.21.6/protobuf-java-util-3.21.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/h2database/h2/2.1.214/h2-2.1.214.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/linkedin/coral/coral-common/2.0.77/coral-common-2.0.77.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/linkedin/coral/coral-hive/2.0.77/coral-hive-2.0.77.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/linkedin/coral/coral-trino/2.0.77/coral-trino-2.0.77.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/linkedin/coral/coral-trino-parser/2.0.77/coral-trino-parser-2.0.77.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/microsoft/azure/msal4j/1.11.0/msal4j-1.11.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/microsoft/azure/msal4j-persistence-extension/1.1.0/msal4j-persistence-extension-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/commons-codec/commons-codec/1.15/commons-codec-1.15.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/nimbusds/content-type/2.1/content-type-2.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/nimbusds/lang-tag/1.5/lang-tag-1.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/nimbusds/nimbus-jose-jwt/9.14/nimbus-jose-jwt-9.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/nimbusds/oauth2-oidc-sdk/9.18/oauth2-oidc-sdk-9.18.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/ons/19.3.0.0/ons-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/oraclepki/19.3.0.0/oraclepki-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/osdt_cert/19.3.0.0/osdt_cert-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/osdt_core/19.3.0.0/osdt_core-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/simplefan/19.3.0.0/simplefan-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/oracle/ojdbc/ucp/19.3.0.0/ucp-19.3.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/qubole/rubix/rubix-presto-shaded/0.3.18/rubix-presto-shaded-0.3.18.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/squareup/okhttp3/okhttp/3.14.9/okhttp-3.14.9.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/squareup/okhttp3/okhttp-urlconnection/3.14.9/okhttp-urlconnection-3.14.9.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/squareup/okio/okio/1.17.2/okio-1.17.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/sun/istack/istack-commons-runtime/3.0.12/istack-commons-runtime-3.0.12.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/teradata/re2j-td/1.4/re2j-td-1.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/typesafe/netty/netty-reactive-streams/2.0.5/netty-reactive-streams-2.0.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/com/typesafe/netty/netty-reactive-streams-http/2.0.5/netty-reactive-streams-http-2.0.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/dev/failsafe/failsafe/3.3.0/failsafe-3.3.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/aircompressor/0.24/aircompressor-0.24.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/bootstrap/228/bootstrap-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/bytecode/1.4/bytecode-1.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/concurrent/228/concurrent-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/configuration/228/configuration-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/discovery/228/discovery-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/discovery/discovery-server/1.32/discovery-server-1.32.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/event/228/event-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/http-client/228/http-client-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/http-server/228/http-server-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/jaxrs/228/jaxrs-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/jmx/228/jmx-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/jmx-http/228/jmx-http-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/joni/2.1.5.3/joni-2.1.5.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/json/228/json-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/log/228/log-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/log-manager/228/log-manager-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/node/228/node-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/openmetrics/228/openmetrics-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/security/228/security-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/slice/0.45/slice-0.45.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/stats/228/stats-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/testing/228/testing-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/trace-token/228/trace-token-228.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/airlift/units/1.8/units-1.8.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/grpc/grpc-context/1.44.1/grpc-context-1.44.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/jsonwebtoken/jjwt-api/0.11.5/jjwt-api-0.11.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/jsonwebtoken/jjwt-impl/0.11.5/jjwt-impl-0.11.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/jsonwebtoken/jjwt-jackson/0.11.5/jjwt-jackson-0.11.5.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/leangen/geantyref/geantyref/1.3.13/geantyref-1.3.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/minio/minio/7.1.4/minio-7.1.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-buffer/4.1.79.Final/netty-buffer-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-codec/4.1.79.Final/netty-codec-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-codec-dns/4.1.79.Final/netty-codec-dns-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-codec-http2/4.1.79.Final/netty-codec-http2-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-codec-http/4.1.79.Final/netty-codec-http-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-codec-socks/4.1.79.Final/netty-codec-socks-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-common/4.1.79.Final/netty-common-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-handler/4.1.79.Final/netty-handler-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-handler-proxy/4.1.79.Final/netty-handler-proxy-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-resolver/4.1.79.Final/netty-resolver-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-resolver-dns/4.1.79.Final/netty-resolver-dns-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-resolver-dns-classes-macos/4.1.79.Final/netty-resolver-dns-classes-macos-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-resolver-dns-native-macos/4.1.79.Final/netty-resolver-dns-native-macos-4.1.79.Final-osx-x86_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-boringssl-static/2.0.53.Final/netty-tcnative-boringssl-static-2.0.53.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-boringssl-static/2.0.53.Final/netty-tcnative-boringssl-static-2.0.53.Final-linux-aarch_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-boringssl-static/2.0.53.Final/netty-tcnative-boringssl-static-2.0.53.Final-linux-x86_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-boringssl-static/2.0.53.Final/netty-tcnative-boringssl-static-2.0.53.Final-osx-aarch_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-boringssl-static/2.0.53.Final/netty-tcnative-boringssl-static-2.0.53.Final-osx-x86_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-boringssl-static/2.0.53.Final/netty-tcnative-boringssl-static-2.0.53.Final-windows-x86_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-tcnative-classes/2.0.53.Final/netty-tcnative-classes-2.0.53.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-transport/4.1.79.Final/netty-transport-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-transport-classes-epoll/4.1.79.Final/netty-transport-classes-epoll-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-transport-classes-kqueue/4.1.79.Final/netty-transport-classes-kqueue-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-transport-native-epoll/4.1.79.Final/netty-transport-native-epoll-4.1.79.Final-linux-x86_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-transport-native-kqueue/4.1.79.Final/netty-transport-native-kqueue-4.1.79.Final-osx-x86_64.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/netty/netty-transport-native-unix-common/4.1.79.Final/netty-transport-native-unix-common-4.1.79.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/opencensus/opencensus-api/0.31.0/opencensus-api-0.31.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/opencensus/opencensus-contrib-http-util/0.31.0/opencensus-contrib-http-util-0.31.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/projectreactor/netty/reactor-netty-core/1.0.14/reactor-netty-core-1.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/projectreactor/netty/reactor-netty-http/1.0.14/reactor-netty-http-1.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/projectreactor/reactor-core/3.4.13/reactor-core-3.4.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/trino/hadoop/hadoop-apache/3.2.0-18/hadoop-apache-3.2.0-18.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/trino/hive/hive-apache/3.1.2-20/hive-apache-3.1.2-20.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/trino/hive/hive-thrift/1/hive-thrift-1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/trino/orc/orc-protobuf/14/orc-protobuf-14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/trino/tpcds/tpcds/1.4/tpcds-1.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/io/trino/tpch/tpch/1.1/tpch-1.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/it/unimi/dsi/fastutil/8.3.0/fastutil-8.3.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/javax/activation/javax.activation-api/1.2.0/javax.activation-api-1.2.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/javax/annotation/javax.annotation-api/1.3.2/javax.annotation-api-1.3.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/javax/inject/javax.inject/1/javax.inject-1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/javax/validation/validation-api/2.0.1.Final/validation-api-2.0.1.Final.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/javax/ws/rs/javax.ws.rs-api/2.1.1/javax.ws.rs-api-2.1.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/joda-time/joda-time/2.12.2/joda-time-2.12.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/junit/junit/4.13.2/junit-4.13.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/net/bytebuddy/byte-buddy/1.14.1/byte-buddy-1.14.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/net/java/dev/jna/jna/5.12.1/jna-5.12.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/net/java/dev/jna/jna-platform/5.12.1/jna-platform-5.12.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/net/minidev/accessors-smart/2.4.7/accessors-smart-2.4.7.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/net/minidev/json-smart/2.4.7/json-smart-2.4.7.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/antlr/antlr/3.4/antlr-3.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/antlr/antlr4-runtime/4.11.1/antlr4-runtime-4.11.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/antlr/antlr-runtime/3.4/antlr-runtime-3.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/antlr/ST4/4.0.4/ST4-4.0.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/antlr/stringtemplate/3.2.1/stringtemplate-3.2.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/bval/bval-jsr/2.0.6/bval-jsr-2.0.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/commons/commons-compress/1.22/commons-compress-1.22.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/datasketches/datasketches-java/3.3.0/datasketches-java-3.3.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/datasketches/datasketches-memory/2.1.0/datasketches-memory-2.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/httpcomponents/client5/httpclient5/5.1/httpclient5-5.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/httpcomponents/core5/httpcore5/5.1.1/httpcore5-5.1.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/httpcomponents/core5/httpcore5-h2/5.1.1/httpcore5-h2-5.1.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/httpcomponents/httpcore/4.4.13/httpcore-4.4.13.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/iceberg/iceberg-api/1.1.0/iceberg-api-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/iceberg/iceberg-bundled-guava/1.1.0/iceberg-bundled-guava-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/iceberg/iceberg-common/1.1.0/iceberg-common-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/iceberg/iceberg-core/1.1.0/iceberg-core-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/iceberg/iceberg-orc/1.1.0/iceberg-orc-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/iceberg/iceberg-parquet/1.1.0/iceberg-parquet-1.1.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/ivy/ivy/2.4.0/ivy-2.4.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/logging/log4j/log4j-api/2.17.1/log4j-api-2.17.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/logging/log4j/log4j-to-slf4j/2.17.1/log4j-to-slf4j-2.17.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/lucene/lucene-analyzers-common/8.4.1/lucene-analyzers-common-8.4.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/lucene/lucene-core/8.4.1/lucene-core-8.4.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/orc/orc-core/1.8.0/orc-core-1.8.0-nohive.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/orc/orc-shims/1.8.0/orc-shims-1.8.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/apache/thrift/libthrift/0.17.0/libthrift-0.17.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/assertj/assertj-core/3.24.2/assertj-core-3.24.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/checkerframework/checker-qual/3.25.0/checker-qual-3.25.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/http2/http2-client/10.0.14/http2-client-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/http2/http2-common/10.0.14/http2-common-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/http2/http2-hpack/10.0.14/http2-hpack-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/http2/http2-http-client-transport/10.0.14/http2-http-client-transport-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/http2/http2-server/10.0.14/http2-server-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-alpn-client/10.0.14/jetty-alpn-client-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-alpn-java-client/10.0.14/jetty-alpn-java-client-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-client/10.0.14/jetty-client-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-http/10.0.14/jetty-http-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-io/10.0.14/jetty-io-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-jmx/10.0.14/jetty-jmx-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-security/10.0.14/jetty-security-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-server/10.0.14/jetty-server-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-servlet/10.0.14/jetty-servlet-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/jetty-util/10.0.14/jetty-util-10.0.14.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/eclipse/jetty/toolchain/jetty-servlet-api/4.0.6/jetty-servlet-api-4.0.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/flywaydb/flyway-core/7.15.0/flyway-core-7.15.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/gaul/modernizer-maven-annotations/2.5.0/modernizer-maven-annotations-2.5.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/hk2/hk2-api/2.6.1/hk2-api-2.6.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/hk2/hk2-locator/2.6.1/hk2-locator-2.6.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/hk2/hk2-utils/2.6.1/hk2-utils-2.6.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/hk2/osgi-resource-locator/1.0.3/osgi-resource-locator-1.0.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jaxb/jaxb-runtime/2.3.4/jaxb-runtime-2.3.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jaxb/txw2/2.3.4/txw2-2.3.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jersey/containers/jersey-container-servlet/2.39/jersey-container-servlet-2.39.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jersey/containers/jersey-container-servlet-core/2.39/jersey-container-servlet-core-2.39.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jersey/core/jersey-client/2.39/jersey-client-2.39.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jersey/core/jersey-common/2.39/jersey-common-2.39.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jersey/core/jersey-server/2.39/jersey-server-2.39.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/glassfish/jersey/inject/jersey-hk2/2.39/jersey-hk2-2.39.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/hdrhistogram/HdrHistogram/2.1.9/HdrHistogram-2.1.9.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/javassist/javassist/3.29.2-GA/javassist-3.29.2-GA.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/jdbi/jdbi3-core/3.32.0/jdbi3-core-3.32.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/jdbi/jdbi3-sqlobject/3.32.0/jdbi3-sqlobject-3.32.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/jetbrains/annotations/19.0.0/annotations-19.0.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/locationtech/jts/io/jts-io-common/1.16.1/jts-io-common-1.16.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/locationtech/jts/jts-core/1.16.1/jts-core-1.16.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/openjdk/jmh/jmh-core/1.36/jmh-core-1.36.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/openjdk/jmh/jmh-generator-annprocess/1.36/jmh-generator-annprocess-1.36.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/openjdk/jol/jol-core/0.16/jol-core-0.16.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/ow2/asm/asm/9.4/asm-9.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/ow2/asm/asm-analysis/9.4/asm-analysis-9.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/ow2/asm/asm-tree/9.4/asm-tree-9.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/ow2/asm/asm-util/9.4/asm-util-9.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/pcollections/pcollections/2.1.2/pcollections-2.1.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/reactivestreams/reactive-streams/1.0.3/reactive-streams-1.0.3.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/rnorth/duct-tape/duct-tape/1.0.8/duct-tape-1.0.8.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/roaringbitmap/RoaringBitmap/0.9.35/RoaringBitmap-0.9.35.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/roaringbitmap/shims/0.9.35/shims-0.9.35.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/slf4j/jcl-over-slf4j/2.0.6/jcl-over-slf4j-2.0.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/slf4j/log4j-over-slf4j/2.0.6/log4j-over-slf4j-2.0.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/slf4j/slf4j-api/2.0.6/slf4j-api-2.0.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/slf4j/slf4j-jdk14/2.0.6/slf4j-jdk14-2.0.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/testcontainers/testcontainers/1.17.6/testcontainers-1.17.6.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/testng/testng/6.10/testng-6.10.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/threeten/threetenbp/1.5.2/threetenbp-1.5.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/threeten/threeten-extra/1.7.1/threeten-extra-1.7.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/weakref/jmxutils/1.22/jmxutils-1.22.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/annotations/2.17.151/annotations-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/apache-client/2.17.151/apache-client-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/arns/2.17.151/arns-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/auth/2.17.151/auth-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/aws-core/2.17.151/aws-core-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/aws-query-protocol/2.17.151/aws-query-protocol-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/aws-xml-protocol/2.17.151/aws-xml-protocol-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/http-client-spi/2.17.151/http-client-spi-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/json-utils/2.17.151/json-utils-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/metrics-spi/2.17.151/metrics-spi-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/netty-nio-client/2.17.151/netty-nio-client-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/profiles/2.17.151/profiles-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/protocol-core/2.17.151/protocol-core-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/regions/2.17.151/regions-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/s3/2.17.151/s3-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/sdk-core/2.17.151/sdk-core-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/sts/2.17.151/sts-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/third-party-jackson-core/2.17.151/third-party-jackson-core-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/awssdk/utils/2.17.151/utils-2.17.151.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/eventstream/eventstream/1.0.1/eventstream-1.0.1.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${MAVEN_LOCAL_REPOSITORY_HOME}/software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/client/trino-client/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/core/trino-main/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/core/trino-main/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/core/trino-parser/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/core/trino-spi/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/core/trino-spi/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-array/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-bridge/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-collect/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-filesystem/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-geospatial-toolkit/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-hadoop-toolkit/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-hdfs/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-hive-formats/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-matching/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-memory-context/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-orc/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-parquet/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/lib/trino-plugin-toolkit/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-base-jdbc/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-base-jdbc/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-blackhole/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-exchange-filesystem/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-hive/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-hive/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-iceberg/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-iceberg/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-memory/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-memory/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-resource-group-managers/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-resource-group-managers/target/test-classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-tpcds/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/plugin/trino-tpch/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/testing/trino-benchmark-queries/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/testing/trino-testing-containers/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/testing/trino-testing-services/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/testing/trino-testing/target/classes:${COMMON_CLASS_PATH}
COMMON_CLASS_PATH=${TRINO_HOME}/testing/trino-tests/target/test-classes:${COMMON_CLASS_PATH}

CLASSPATH1=${CLASSPATH}:${COMMON_CLASS_PATH}:${TRINO_HOME}/testing/trino-tests/target/classes
CLASSPATH2=${COMMON_CLASS_PATH}:${TRINO_HOME}/client/trino-jdbc/target/classes

LOG_DIR=logs-$(date +"%Y-%m-%d-%H-%M-%S")
if [[ -d ${LOG_DIR} ]]; then
    rm -rf ${LOG_DIR};
fi
mkdir ${LOG_DIR}

for queryId in "${queries[@]}"; do
    echo "RUNNING query : "$queryId
    LOG_PREFIX=query-$queryId

    ${RUNNER} -classpath ${REPO_HOME}/artifact/Gluten-Trino-411.jar:$CLASSPATH1 \
              -Djava.library.path=${REPO_HOME}/artifact/ \
              io.trino.tests.tpch.TpchQueryRunner > ${LOG_DIR}/$LOG_PREFIX-server.log 2>&1 &
    server_pid=$!
    sleep 15

    ${RUNNER} -classpath ${REPO_HOME}/artifact/Gluten-Trino-411.jar:${REPO_HOME}/artifact/Gluten-Trino-411-tests.jar:$CLASSPATH2 \
              io.trino.gluten.tests.tpch.TpchFuncTest $queryId > ${LOG_DIR}/$LOG_PREFIX-client.log 2>&1 &
    client_pid=$!
    sleep 20

    kill -9 $client_pid
    kill -9 $server_pid
done
