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
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/*
*created by cwh on 2022/5/2
*secondary index: coprocessor
*build the index and keep the index uniformity
 */

//2.0版本之前是采用 extends BaseRegionObserver实现的
public class TestCoprocessor implements RegionObserver, RegionCoprocessor {
    static Connection connection = null;
    static Table table = null;
    private RegionCoprocessorEnvironment env = null;
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("index"));
        } catch (Exception var2) {
            var2.printStackTrace();
        }
    }

    public TestCoprocessor() {
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
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            List<Put> rows=new ArrayList<>();
            List<Cell> Keyvalue=put.get("INFO".getBytes(),"CardId".getBytes());
            for(Cell cell:Keyvalue){
                String rowkey=Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                String value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                String newky="a_"+value+"_"+rowkey;   //a is a IndexName
                Put newput = new Put(Bytes.toBytes(newky));
                byte[] columnfamily = Bytes.toBytes("INFO");
                byte[] qualifier = Bytes.toBytes("Tag");
                byte[] indexvalue = Bytes.toBytes("0");
                newput.addColumn(columnfamily, qualifier, indexvalue);
                rows.add(newput);
            }
            table.put(rows);
            table.close();
        } catch (Exception v12) {
            //System.out.println("Error");
            return;
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            List<Put> rows=new ArrayList<>();
            List<Cell> Keyvalue=put.get("INFO".getBytes(),"CardId".getBytes());
            for(Cell cell:Keyvalue){
                String rowkey=Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                String value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                String newky="a_"+value+"_"+rowkey;   //a is a IndexName
                Put newput = new Put(Bytes.toBytes(newky));
                byte[] columnfamily = Bytes.toBytes("INFO");
                byte[] qualifier = Bytes.toBytes("Tag");
                byte[] indexvalue = Bytes.toBytes("1");
                newput.addColumn(columnfamily, qualifier, indexvalue);
                rows.add(newput);
            }
            table.put(rows);
            table.close();
        }catch (Exception e){
            //System.out.println("Error");
            return;
        }
    }
}
/*

 */