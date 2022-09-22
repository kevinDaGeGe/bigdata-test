package com.zqpay.bigdata.jar.hive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
 
public class QueryHiveUtils {
    private static Connection conn=JDBCToHiveUtils.getConnnection();
    private static PreparedStatement ps;
    private static ResultSet rs;
    public static void query(String sql)
    {
        System.out.println(sql);
        try {
            ps=JDBCToHiveUtils.prepare(conn, sql);
            rs=ps.executeQuery();
            int columns=rs.getMetaData().getColumnCount();
            while(rs.next())
            {
                for(int i=1;i<=columns;i++)
                {
                    System.out.print(rs.getString(i));
                    System.out.print("");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    public static void getAll(String tablename)
    {
        String sql="select * from "+tablename;
        System.out.println(sql);
        try {
            ps=JDBCToHiveUtils.prepare(conn, sql);
            rs=ps.executeQuery();
            int columns=rs.getMetaData().getColumnCount();
            while(rs.next())
            {
                for(int i=1;i<=columns;i++)
                {
                    System.out.print(rs.getString(i));  
                    System.out.print("");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
 
    }
 
	public static void main(String[] args) {        
		String tablename="hbase_user";
//		String tablename="zf_account_t_subject";
        QueryHiveUtils.getAll(tablename);
//        QueryHiveUtils.query("select * from zf_account_t_subject limit 3");
        String sql="select payment.PAYEE_CUST_ID cust_id, \n" +
                "    sum(payment.order_amt) drjine\n" +
                "       from t_payment_order payment\n" +
                "        where payment.settle_date = date_format(date_add(current_date(),-1),'yyyyMMdd') \n" +
                "        AND payment.order_status = '01' AND \n" +
                "        EXISTS \n" +
                "        (\n" +
                "            SELECT 1 FROM ZF_ACCOUNT_T_ACCOUNT a WHERE payment.PAYEE_CUST_ID=a.cust_id AND a.SUBJECT_NO_2 IN\n" +
                "                (SELECT SUBJECT_NO FROM zf_account_t_subject\n" +
                "                     WHERE PARENT_SUBJECT IN ('1002', '1012')\n" +
                "                ) \n" +
                "        )\n" +
                "       group by payment.PAYEE_CUST_ID";
//        QueryHiveUtils.query(sql);
    }
 
}