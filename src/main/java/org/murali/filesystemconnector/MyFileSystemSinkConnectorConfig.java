package org.murali.filesystemconnector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MyFileSystemSinkConnectorConfig extends AbstractConfig {
    public static final String TARGET_DIRECTORY = "target.dir";
    private static final String TARGET_DIRECTORY_DOC = "Target directory path where the files need to stored";

    public MyFileSystemSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MyFileSystemSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TARGET_DIRECTORY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TARGET_DIRECTORY_DOC);
    }

    public String getTargetDirectory() {
        return this.getString(TARGET_DIRECTORY);
    }
}
