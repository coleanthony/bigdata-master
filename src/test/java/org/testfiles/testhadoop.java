package org.testfiles;

import java.io.IOException;
import org.hadoop.basic.Hadoop_config;

public class testhadoop {

    public static void main(String[] args) throws IOException{
        Hadoop_config hadoop_config=new Hadoop_config();
        hadoop_config.connect();
        /*
        try {
            hadoop_config.mkdir();
        }catch (Exception e){
            e.printStackTrace();
        }*/
        try{
            hadoop_config.disconnect();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
