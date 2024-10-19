#!/bin/sh

./gradlew assemble

OS_RELEASE="opensearch-2.17.1"
SQL_PLUGIN="opensearch-sql-2.17.1.0"
SQL_PLUGIN_ZIP="${SQL_PLUGIN}-SNAPSHOT.zip"

rm -rf /Users/penghuo/release/opensearch/${OS_RELEASE}/plugins/${SQL_PLUGIN}
cp /Users/penghuo/oss/os-sql/plugin/build/distributions/${SQL_PLUGIN_ZIP} /Users/penghuo/release/opensearch/${OS_RELEASE}/plugins 
unzip /Users/penghuo/release/opensearch/${OS_RELEASE}/plugins/${SQL_PLUGIN_ZIP} -d /Users/penghuo/release/opensearch/${OS_RELEASE}/plugins/${SQL_PLUGIN}
rm /Users/penghuo/release/opensearch/${OS_RELEASE}/plugins/${SQL_PLUGIN_ZIP}