package org.murali.filesystemconnector;

import org.apache.kafka.connect.data.Struct;
import org.my.fun.FileData;
import org.my.fun.MetaData;


public class MyAvroStuctConverter {

    public static FileData getFileData(Struct struct){
        FileData.Builder fileData= FileData.newBuilder();

        Struct metaStruct= struct.getStruct("metaData");
        MetaData metaData= MetaData.newBuilder()
                .setSize(metaStruct.getInt64("size")).build();

        FileData fileData1=  fileData.setAbsolutePath(struct.getString("absolutePath"))
                .setFileName(struct.getString("fileName"))
                .setMetaData(metaData)
                .build();

        return fileData1;
    }
}
