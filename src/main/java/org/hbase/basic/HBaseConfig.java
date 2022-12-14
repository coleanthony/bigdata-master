package org.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


/*
create by cwh on 2022/5/1
connect hbase and some basic implementation

!!!
you don't need to care about the warning when connecting to HBase.
we don't need 'winutils.exe' because we run hbase on linux instead of on windows
!!!
 */

public class HBaseConfig {
    //conn
    public static Connection connection=null;
    public static Admin admin=null;
    public static Configuration configuration=HBaseConfiguration.create();
    static {
        try {
            configuration.set("hbase.rootdir","hdfs://192.168.128.31:9000/hbase");
            configuration.set("hbase.zookeeper.quorum","master,slave1,slave2");
            connection=ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /*
    close the admin and connection
     */
    public void HBaseClose(){
        //close the admin and connection
        if(admin!=null){
            try {
                admin.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        if(connection!=null){
            try {
                connection.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public void gettablename() throws IOException {
        TableName[] tables=admin.listTableNames();
        System.out.println(tables.toString());
    }
    /*
    create table
    @param tablename
    @param columnfamily string[] ??????????????????????????????
     */
    public void createTable(String tablename,String[] columnfamily){
        TableName table = TableName.valueOf(tablename);
        try {
            if(admin.tableExists(table)){
                System.out.println(tablename+" already exist!");
            }else{
                //create table
                TableDescriptorBuilder tableDescriptorBuilder=TableDescriptorBuilder.newBuilder(table);
                Arrays.stream(columnfamily).forEach(cf->{
                    ColumnFamilyDescriptor columnfamilydecs=ColumnFamilyDescriptorBuilder.of(cf);
                    tableDescriptorBuilder.setColumnFamily(columnfamilydecs);
                });
                admin.createTable(tableDescriptorBuilder.build());
                System.out.println(tablename+" builds successfully!");
            }
        }catch (IOException e){
            e.printStackTrace();
            System.out.println("Error!");
        }
    }

    /*
    @param:tablename: delete "tablename"
     */
    public void deleteTable(String tablename){
        TableName table =TableName.valueOf(tablename);
        try{
            if(admin.tableExists(table)){
                admin.disableTable(table);
                admin.deleteTable(table);
                System.out.println(tablename+=" is deleted successfully!");
            }
            else
                System.out.println(tablename+" does not exists!");
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error!");
        }
    }

    /*
    insert data
    just one record
     */
    public void insertTable(String tablename,String Rowkey,String columnfamily,
                            String columnqualifier,String value){
        try{
            Table table=connection.getTable(TableName.valueOf(tablename));
            Put put=new Put(Rowkey.getBytes());

            put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes(columnqualifier),Bytes.toBytes(value));
            table.put(put);
            System.out.println(tablename+" "+Rowkey+" insert successfully");
            table.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("insert data unsuccessfully!");
        }
    }

    /*
    @param:tablename
    @param:rowkey
     */
    public void deleterecordbyRowkey(String tablename,String Rowkey){
        try {
            Table table = connection.getTable(TableName.valueOf(tablename));
            Delete del=new Delete(Rowkey.getBytes());
            table.delete(del);
            System.out.println(tablename+" "+Rowkey+" "+"delete successfully!");
            table.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("delete data unsuccessfully!");
        }
    }

    /*
@param:tablename
@param:rowkey
 */
    public Result getrecordbyRowkey(String tablename,String Rowkey){
        /*
        return:res->Result
         */
        try{
            //long start,end;
            //start=System.currentTimeMillis();
            Table table =connection.getTable(TableName.valueOf(tablename));
            Get get=new Get(Rowkey.getBytes());
            Result res=table.get(get);
            System.out.println("get data successfully!");
            //end=System.currentTimeMillis();
            //System.out.print(end-start);
            table.close();
            return res;
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error!");
            return null;
        }
    }

    /*
    output the data
    @param:result->Result
     */
    public void outputrecord(Result result){
        try {
            List<Cell> cells = result.listCells();
            if (cells != null) {
                for (Cell cell : cells) {
                    String columnfamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String columnqualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("column family:" + columnfamily + " column qualifier:" + columnqualifier + " value:" + value);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error!");
        }
    }

    /*
    @param:tablename
     */
    public List<Result> Scantable(String tablename){
        try{
            Table table=connection.getTable(TableName.valueOf(tablename));
            Scan scan= new Scan();
            ResultScanner scanner=table.getScanner(scan);
            List<Result> res=new ArrayList<>();
            for(Result rs:scanner)
                res.add(rs);
            scanner.close();
            table.close();
            System.out.println("get all data successfully");
            return res;

        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error!");
            return null;
        }
    }

    /*
scan from startkey->endkey
 */
    public List<Result> ScantablebyRowkey(String tablename ,String startrow,String endrow){
        try {
            Table table=connection.getTable(TableName.valueOf(tablename));
            Scan scan=new Scan(Bytes.toBytes(startrow),Bytes.toBytes(endrow));
            ResultScanner resultScanner=table.getScanner(scan);
            List<Result> res=new ArrayList<>();
            for(Result rs:resultScanner)
                res.add(rs);
            resultScanner.close();
            System.out.println("Successfully get data!");
            table.close();
            return res;
        }catch (Exception e){
        e.printStackTrace();
        System.out.println("Error!");
        return null;
    }
}

    /*
    output all the data from a scan
     */
    public void outputAllRecord(List<Result> datalist){
        try {
            for(Result res:datalist){
                outputrecord(res);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error!");
        }
    }

    public void insertdatabycsv(String filename,String tablename,String columnfamily){
        String csvdivider=",";
        String charsetName="GBK";
        BufferedReader br=null;

        try{
            br=new BufferedReader(new InputStreamReader(new FileInputStream(filename),charsetName));
        }catch(Exception e){
            e.printStackTrace();
        }
        String line="";
        try{
            Table table=connection.getTable(TableName.valueOf(tablename));
            int count=0;
            br.readLine();
            long starttime,endtime;
            starttime = System.currentTimeMillis();
            List<Put> puttable=new ArrayList<>();
            while((line=br.readLine())!=null){
                //??????,??????????????????,????????????,????????????,?????????,????????????,????????????,????????????,?????????,????????????,????????????
                //280045973,20131231221001,????????????,435,500,268004129,???????????????,????????????,OGT-129,1,20140101
                /*
                data = {'INFO:CardID': str(row['??????']), 'INFO:Time': str(row['??????????????????']), 'INFO:Type': str(row['????????????']),
                    'INFO:Line': str(row['????????????']), 'INFO:Station': str(row["????????????"])}
                rowkey=S[row["????????????"]]+'_'+str(row['??????????????????'])+'_'+str(row['??????'])
                 */
                String[] items=line.split(csvdivider);
                String Rowkey=items[7]+"_"+items[1]+"_"+items[0];
                Put put=new Put(Rowkey.getBytes());
                put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes("CardId"),Bytes.toBytes(items[0]));
                put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes("Time"),Bytes.toBytes(items[1]));
                put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes("Type"),Bytes.toBytes(items[2]));
                put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes("Line"),Bytes.toBytes(items[6]));
                put.addColumn(Bytes.toBytes(columnfamily),Bytes.toBytes("Station"),Bytes.toBytes(items[7]));
                puttable.add(put);
                count++;
                if(count%10000==0) {
                    table.put(puttable);
                    puttable.clear();
                    System.out.println("insert " + Integer.toString(count));
                }
                //if(count==1000000)
                //    break;
            }
            table.put(puttable);
            puttable.clear();
            endtime = System.currentTimeMillis();
            System.out.println("???????????????" + (endtime - starttime) + "(ms)");
            System.out.println("insert successfully!");
            table.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /*
    ??????????????????????????????
    ??????????????????indextable???scan?????????????????????????????????indextable???rowkey?????????table???rowkey??????????????????
     */
    public void ReadbySecondaryIndex(String tablename ,String indextablename,String startrow,String endrow){
        try{
            long start,end;
            start=System.currentTimeMillis();
            List<Result> results=ScantablebyRowkey(indextablename,startrow,endrow);
            Set<String> datatablerowkey=new HashSet<>();
            for(Result result:results) {
                List<Cell> cells = result.listCells();
                if (cells != null) {
                    for (Cell cell : cells) {
                        String Rowkey=Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                        String[] temp=Rowkey.split("_");
                        //???rowkey??????
                        String rk=temp[2]+"_"+temp[3]+"_"+temp[4];
                        datatablerowkey.add(rk);
                    }
                }
            }
            for(String rk:datatablerowkey){
                Result result=getrecordbyRowkey(tablename,rk);
                outputrecord(result);
            }
            end=System.currentTimeMillis();
            System.out.println("???????????????" + (end - start) + "(ms)");

        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Error!");
        }
    }

    /*
    ?????????????????????????????????????????????????????????
     */
    public void singleColumnValueFilter(String tablename) throws IOException{
        try {
            long starttime, endtime;
            starttime = System.currentTimeMillis();
            Table table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("INFO".getBytes(), "CardId".getBytes(), CompareFilter.CompareOp.EQUAL, "322919915".getBytes());
            scan.setFilter((singleColumnValueFilter));

            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                //?????????????????????rowKey???
                byte[] row = result.getRow();
                System.out.println("?????????rowKey???" + Bytes.toString(row));
                //????????????????????????cell???
                List<Cell> cells = result.listCells();
                //???????????????????????????cell
                for (Cell cell : cells) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String family = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    //id??????age??????????????????
                    if ("INFO".equals(family) && "CardId".equals(qualifier))
                        System.out.println("?????????"+family+"?????????"+qualifier+"?????????"+value);
                }
            }
            System.out.println("Successfully get the data");
            endtime = System.currentTimeMillis();
            System.out.println("???????????????" + (endtime - starttime) + "(ms)");
            table.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /*
    ???????????????????????????????????????????????????????????????????????????
     */
    public void singleColumnValueFilterscan(String tablename) throws IOException{
        try {
            long starttime, endtime;
            starttime = System.currentTimeMillis();
            Table table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("INFO".getBytes(), "CardId".getBytes(), CompareOperator.GREATER_OR_EQUAL, "322919915".getBytes());
            SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter("INFO".getBytes(), "CardId".getBytes(), CompareOperator.LESS, "322919929".getBytes());
            FilterList filterList = new FilterList(singleColumnValueFilter,singleColumnValueFilter2);
            scan.setFilter(filterList);

            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                //?????????????????????rowKey???
                byte[] row = result.getRow();
                System.out.println("?????????rowKey???" + Bytes.toString(row));
                //????????????????????????cell???
                List<Cell> cells = result.listCells();
                //???????????????????????????cell
                for (Cell cell : cells) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String family = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    //id??????age??????????????????
                    if ("INFO".equals(family) && "CardId".equals(qualifier))
                        System.out.println("?????????"+family+"?????????"+qualifier+"?????????"+value);
                }
            }
            System.out.println("Successfully get the data");
            endtime = System.currentTimeMillis();
            System.out.println("???????????????" + (endtime - starttime) + "(ms)");
            table.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void Scanlimit(String tablename,int limit){

    }
}

