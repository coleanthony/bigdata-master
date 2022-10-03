package org.hbase.secondaryindexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/*
 *created by cwh on 2022/5/7
 *secondary index: coprocessor2
 *build the index and keep the index uniformity
 */

//2.0版本之前是采用 extends BaseRegionObserver实现的
public class TestCoprocessor2 implements RegionObserver, RegionCoprocessor {
    static Connection connection = null;
    static Table table = null;
    static Table indextable = null;
    private RegionCoprocessorEnvironment env = null;
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("Subway"));
            indextable = connection.getTable(TableName.valueOf("index"));
        } catch (Exception var2) {
            var2.printStackTrace();
        }
    }

    public TestCoprocessor2() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) {
        this.env = (RegionCoprocessorEnvironment)e;
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        //nothing to do there
    }


    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        try {
            for(Cell cell:results){
                String indexrowkey=Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                String columnfamily=Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String columnqualifier=Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                if(columnfamily.equals("INFO")&&columnqualifier.equals("Tag")&&value.equals("1"))
                    return;
            }
            for(Cell cell:results){
                String indexrowkey=Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                String columnfamily=Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String columnqualifier=Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                if(columnfamily.equals("INFO")&&columnqualifier.equals("Tag")){
                    //将indexrowkey分解为datarowkey
                    String[] temp=indexrowkey.split("_");
                    String datarowkey=temp[temp.length-1];
                    Get newget=new Get(datarowkey.getBytes());
                    Result res=table.get(newget);
                    if(res==null) {
                        results = null;
                        Delete del = new Delete(indexrowkey.getBytes());
                        indextable.delete(del);
                    }
                    return;
                }
            }

        }catch (Exception v12){
            //System.out.println("Error");
            return;
        }
    }
}

