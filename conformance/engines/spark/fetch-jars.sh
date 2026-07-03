#!/usr/bin/env sh
# Downloads the jars the Spark smoke test needs into ./jars/.
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

# Iceberg's Spark connector (fat jar) for Spark 3.5 / Scala 2.12.
fetch "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# AWS SDK bundle for Iceberg's S3FileIO (used for the MinIO-backed warehouse).
fetch "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

echo "done:"
ls -l jars/
