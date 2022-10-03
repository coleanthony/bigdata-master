package org.Phoenix.basic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;

public class PhoenixConfig {
    Statement  statement=null;
    Connection connection=null;
    ResultSet resultset=null;
    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void getConnction(){
        try {
            connection=DriverManager.getConnection("jdbc:phoenix:master,slave1,slave2");
            System.out.println("connect successfully!");
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public  void close(){
        System.out.println("close.........");
        if(resultset!=null){
            try{
                resultset.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        if(statement!=null){
            try{
                statement.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        if(connection!=null){
            try{
                connection.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
    }

    public void createtable(){
        try{
            String sql="create table subwaytest (rowkey varchar primary key,cardid varchar,time varchar,type varchar,line varchar,station varchar)";
            PreparedStatement ps=connection.prepareStatement(sql);
            System.out.println("Create table successfully!");
            ps.execute();
            ps.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void read(){
        try{
            String sql="select * from subwaytest limit 1";
            PreparedStatement ps=connection.prepareStatement(sql);
            System.out.println("get data successfully!");
            ps.execute();
            ps.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void upsertbyCSV(String filename){
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
            String upsertsql="upsert into subwaytest values(?,?,?,?,?,?)";
            PreparedStatement stat=connection.prepareStatement(upsertsql);
            int count=0;
            br.readLine();
            long starttime,endtime;
            starttime = System.currentTimeMillis();
            int btach=10000;
            while((line=br.readLine())!=null){
                //卡号,交易日期时间,交易类型,交易金额,交易值,设备编码,公司名称,线路站点,车牌号,联程标记,结算日期
                //280045973,20131231221001,地铁出站,435,500,268004129,地铁一号线,大剧院站,OGT-129,1,20140101
                /*
                data = {'INFO:CardID': str(row['卡号']), 'INFO:Time': str(row['交易日期时间']), 'INFO:Type': str(row['交易类型']),
                    'INFO:Line': str(row['公司名称']), 'INFO:Station': str(row["线路站点"])}
                rowkey=S[row["线路站点"]]+'_'+str(row['交易日期时间'])+'_'+str(row['卡号'])
                 */
                String[] items=line.split(csvdivider);
                String Rowkey=items[7]+"_"+items[1]+"_"+items[0];
                String cardid=items[0];
                String time=items[1];
                String type=items[2];
                String carline=items[6];
                String station=items[7];
                stat.setString(1, Rowkey);
                stat.setString(2,cardid);
                stat.setString(3,time);
                stat.setString(4,type);
                stat.setString(5,carline);
                stat.setString(6,station);
                stat.execute();
                count++;
                if(count%btach==0) {
                    connection.commit();
                    System.out.println("insert " + Integer.toString(count));
                }
                //if(count==1000000)
                 //   break;
            }
            connection.commit();
            endtime = System.currentTimeMillis();
            System.out.println("运行时间：" + (endtime - starttime) + "(ms)");
            System.out.println("insert successfully!");

            stat.close();
            br.close();

        }catch (SQLException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
