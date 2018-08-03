package org.murali.filesystemconnector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyFileSystemSinkConnector extends SinkConnector {
    private static Logger log = LoggerFactory.getLogger(MyFileSystemSinkConnector.class);
    private Map<String, String> config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {


        //TODO: Add things you need to do to setup your connector.
        config = map;
        /**
         * This will be executed once per connector. This can be used to handle connector level setup.
         */

    }

    @Override
    public Class<? extends Task> taskClass() {
        //TODO: Return your task implementation.
        return MyFileSystemSinkTask.class;
    }

    /**
     * This is used to schedule the number of tasks that will be running. This should not exceed maxTasks.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //TODO: Define the individual task configurations that will be executed.
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(config);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        //TODO: Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return MyFileSystemSinkConnectorConfig.conf();
    }
}
