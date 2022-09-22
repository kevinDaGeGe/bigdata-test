package com.zqpay.bigdata.jar.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("deprecation")
public class TestHBase {
    private static Configuration conf = HBaseConfiguration.create();
    private static Admin admin;
    private static Connection connection = null;

    static {
        //HBase Master地址，在hbase-site.xml中配置
        conf.set("hbase.rootdir", "hdfs://master01:9000/hbase");
        //ZooKeeper三个服务器地址
        conf.set("hbase.zookeeper.quorum", "b-sl-yy-bigdata-01,b-sl-yy-bigdata-02,b-sl-yy-bigdata-03");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        TestHBase test = new TestHBase();
//         test.createTable("java_test");//建表
//        test.createTable2("java_test","f1","f2");//建表（制定行键）
//        test.createTable2("department","info");//建表（制定行键）
//        test.addData("java_test");//插入数据
//        test.addData("person");//插入数据
//        test.insertBatchData("java_test");//插入多个数据
//        test.getData("java_test");//通过Get查找单行数据,注意列值的类型
//        test.scanAllData("java_test");//通过Scan方法扫描全表
        test.scanAllData("department");//通过Scan方法扫描全表
        // test.deleteData("java_test");//删除表数据
//        test.deleteTable("java_test");//删除表
//        test.deleteTable("department");//删除表
        admin.close();
        connection.close();
    }


    public void createTable2(String tableName, String... columnFamily) {

        TableName tableNameObj = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tableNameObj)) {
                System.out.println("Table: " + tableName + " already exists!");
            } else {
                HTableDescriptor tb = new HTableDescriptor(tableNameObj);
                for (int i = 0; i < columnFamily.length; i++) {
                    HColumnDescriptor family = new HColumnDescriptor(columnFamily[i]);
                    tb.addFamily(family);
                }
                admin.createTable(tb);
                System.out.println(tableName + "创建表成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(tableName + "创建表失败");
        }
    }


    public void createTable(String tableName) throws IOException {

        //通过管理员对象创建表
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //给表添加列族
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        //将两个列族设置到 创建的表中
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        //创建表
        admin.createTable(hTableDescriptor);
        //关闭连接
        admin.close();
        connection.close();
        System.out.println("创建表成功");
    }

    public void addData(String tableName) throws IOException {

        //获取表对象
        Table myuser = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小明"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(22));
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("青岛"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000001"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("你好"));
        myuser.put(put);
        //关闭表
        myuser.close();
        System.out.println("插入数据成功");
    }

    public void insertBatchData(String tableName) throws IOException {

        //获取表
        Table myuser = connection.getTable(TableName.valueOf(tableName));
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小红"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(21));
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("女"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("济南"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000002"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("Hello！"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小强"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(22));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("北京"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000003"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("俺北京……"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小花"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(23));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("女"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("重庆"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000004"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("要得"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小杰"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(24));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("上海"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000005"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("阿拉桑海人"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(6));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小雨"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(22));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("女"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("广东"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000006"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("洒洒水"));

        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(7));
        put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("小瓜"));
        put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(22));
        put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("江苏"));
        put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15900000007"));
        put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("我是苏南的"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        myuser.put(listPut);
        myuser.close();
        System.out.println("插入多个数据成功");
    }


    public void getData(String tableName) throws IOException {


        //获取表对象
        Table myuser = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get("0005".getBytes());
        get.addColumn("f2".getBytes(), "address".getBytes());//列族；列名

        //获取返回结果
        Result result = myuser.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            //获取列族的名称
            String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            //获取列的名称
            String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            if (familyName.equals("f1") && columnName.equals("id") || columnName.equals("age")) {
                int value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("列族名： " + familyName + " ,列名： " + columnName + " ,列值：" + value);
            } else {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("列族名： " + familyName + " ,列名： " + columnName + " ,列值：" + value);
            }
        }
        //关闭表
        myuser.close();
    }

    public void scanAllData(String tableName) throws IOException {

        //获取表对象
        Table myuser = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //设置起始和结束的rowkey,扫描结果是：[)类型
        scan.setStartRow("0001".getBytes());
        scan.setStopRow("0008".getBytes());
        ResultScanner scanner = myuser.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //获取列族的名称
                String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                //获取列的名称
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if (familyName.equals("f1") && columnName.equals("id") || columnName.equals("age")) {
                    int value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("列族名： " + familyName + " ,列名： " + columnName + " ,列值：" + value);
                } else {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("列族名： " + familyName + " ,列名： " + columnName + " ,列值：" + value);
                }
            }
        }
        //获取返回结果
        myuser.close();
    }

    public void deleteData(String talbeName) throws IOException {

        //获取表对象
        Table myuser = connection.getTable(TableName.valueOf(talbeName));

        Delete delete = new Delete("0001".getBytes());
        myuser.delete(delete);

        myuser.close();

        System.out.println("删除成功");
    }

    public void deleteTable(String tableName) throws IOException {

        //获取管理员对象
        Admin admin = connection.getAdmin();
        //禁用表
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("删除表成功");
    }
}
