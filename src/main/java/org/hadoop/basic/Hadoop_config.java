package org.hadoop.basic;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;

//some basic implementation of hadoop
public class Hadoop_config {
    private static FileSystem fileSystem=null;

    public void connect(){
        try{
            Configuration configuration= new Configuration();
            configuration.set("fs.defaultFS","hdfs://master:9000");
            fileSystem=FileSystem.get(configuration);
            System.out.println("connect");
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() throws Exception{
        fileSystem=null;
        System.out.println("disconnect");
    }

    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/testhadoop"));
        System.out.println("make dir");
    }

    public void createfile() throws Exception{
        FSDataOutputStream out=fileSystem.create(new Path("/testhadoop/a.txt"),true,4096);
        out.write("test hadoop".getBytes());
        out.flush();
        out.close();
        System.out.println("write file");
    }

    public void judgefileexist() throws Exception{
        boolean exist=fileSystem.exists(new Path("/testhadoop/b.txt"));
        System.out.println(exist);
    }

    public void showfile() throws Exception{
        FSDataInputStream inputstream=fileSystem.open(new Path("/testhadoop/a.txt"));
        String context=inputstreamtostring(inputstream);
        System.out.println(context);
    }

    private java.lang.String inputstreamtostring(InputStream inputStream){
        try{
            java.lang.String encode="utf-8";
            BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuilder sb=new StringBuilder();
            java.lang.String str="";
            while ((str=reader.readLine())!=null){
                sb.append(str).append("\n");
            }
            return sb.toString();
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    public void rename() throws Exception {
        Path oldpath = new Path("/testhadoop/a.txt");
        Path newpath = new Path("/testhadoop/b.txt");
        boolean rena = fileSystem.rename(oldpath, newpath);
        System.out.println(rena);
    }
}
