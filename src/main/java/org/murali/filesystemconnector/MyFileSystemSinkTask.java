package org.murali.filesystemconnector;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.my.fun.FileData;
import org.my.fun.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class MyFileSystemSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(MyFileSystemSinkTask.class);
    MyFileSystemSinkConnectorConfig fileSystemSinkConfg;
    private File folder;
    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        fileSystemSinkConfg = new MyFileSystemSinkConnectorConfig(map);
        //TODO: Create resources like database or api connections here.
        String folderName = fileSystemSinkConfg.getTargetDirectory();

        if (folderName == null) {
            System.out.println("Hi dude: " + fileSystemSinkConfg);
            throw new ConnectException("Missing target folder!");
        }
        log.info("Folder name {}", folderName);
        folder = new File(folderName);
        if (!folder.exists()) {
            if (!folder.mkdirs()) {
                throw new ConnectException("Unable to create target folder");
            }
        } else {
            if (!folder.isDirectory()) {
                throw new ConnectException(folderName + " is not a folder");
            }
        }
    }


    @Override
    public void put(Collection<SinkRecord> collection) {

        for(SinkRecord sinkRecord: collection){
            try {
               // Struct fileData= (Struct) sinkRecord.value();
                FileData fileData= MyAvroStuctConverter.getFileData((Struct)sinkRecord.value());
               /* Struct data= (Struct) sinkRecord.value();
                Struct metaData= (Struct) data.getStruct("metaData");
                */
                File file= new File(folder.getAbsolutePath()  + File.separator + fileData.getFileName());
                file.createNewFile();
                log.info("File Data Received: " + fileData);
                log.info("File with " + fileData.getFileName() + " has created successfully...");
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {
        //Close resources here.
    }

}
