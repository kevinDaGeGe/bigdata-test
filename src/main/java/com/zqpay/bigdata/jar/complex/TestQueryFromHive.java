package com.zqpay.bigdata.jar.complex;

import com.zqpay.bigdata.jar.hive.QueryHiveUtils;

/**
 * @author yu.han
 * @version 1.0.0
 * @Description
 * @ClassName TestQueryFromHive.java
 * @createTime 2022年09月19日 16:05:00
 */
public class TestQueryFromHive {
    public static void main(String[] args) {
        QueryHiveUtils.query("select p.* from person p where p.dm=1");
        QueryHiveUtils.query("select p.*,d.name from person p join department d on p.dm=d.id");
    }
}
