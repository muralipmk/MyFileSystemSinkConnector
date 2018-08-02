# MyFileSystemSinkConnector

It contains producer which produces records in avro format. I use kafka schema registry to registry avro schema.
Functionality of this connector:
1) Producer promts for entering directory you want to create a new file with no content to different location in the same filesystem.
2) It send value with avro schema.
3) MyFileSystemSinkConnector just receives the file details from the topic specified and just simply create the file in the target location
specified in the properties.


Note: This is public so that people interested can use this as a reference to build their own connectors.

How to run or install?

1) mvn clean package
2) Make changes to "plugin.path" in worker.properties.

    plugin.path=<Your path to project>/MyFileSystemConnector/target/MyFileSystemConnector-1.0-SNAPSHOT-package/share/java/MyFileSystemConnector

3) $CONFLUENT/bin/connect-standalone config/worker.properties config/MyFileSystemSinkConnector.properties
