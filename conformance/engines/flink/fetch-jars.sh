#!/usr/bin/env sh
# Downloads the jars the Flink SQL smoke test needs into ./jars/.
# Versions are pinned; see README.md for the compatibility notes.
set -eu

cd "$(dirname "$0")"
mkdir -p jars

ICEBERG_VERSION=1.11.0

fetch() {
    url="$1"
    out="jars/$(basename "$url")"
    if [ -f "$out" ]; then
        echo "already present: $out"
    else
        echo "fetching $url"
        curl -fL --retry 3 -o "$out.tmp" "$url"
        mv "$out.tmp" "$out"
    fi
}

# Iceberg's Flink connector (fat jar) for Flink 1.20.
fetch "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"

# AWS SDK bundle for Iceberg's S3FileIO (used for the MinIO-backed warehouse).
fetch "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# Hadoop classes: iceberg-flink still needs org.apache.hadoop.conf.Configuration
# on the classpath for its catalog factory. The flink-shaded uber jar is the
# smallest self-contained way to provide it inside the stock flink image.
fetch "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"

echo "done:"
ls -l jars/
