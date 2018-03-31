package com.adatafun.dp.service;

import com.adatafun.dp.conf.ESMysqlSpark;
import com.adatafun.dp.model.RestaurantUser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.Properties;

/**
 * UserCIPAverageOrderAmount.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/2.
 */
public class UserCIPAverageOrderAmount {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();
        try{
            Dataset<Row> orderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order_cip",prop)
                    .select("order_no", "pro_code").na().drop();
            Dataset<Row> tbOrderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop)
                    .select("order_no","user_id", "actual_amount").na().drop();
            Dataset<Row> togetherDS = orderDS.join(tbOrderDS, orderDS.col("order_no").equalTo(tbOrderDS
                    .col("order_no")),"left_outer").select("user_id", "pro_code", "actual_amount")
                    .na().drop().groupBy("user_id", "pro_code").avg("actual_amount");
            JavaRDD<RestaurantUser> resultRDD = togetherDS.toJavaRDD().map((Row row) -> {
                RestaurantUser result = new RestaurantUser();
                result.setId(row.get(0).toString() + row.getString(1));
                result.setUserId(row.get(0).toString());
                result.setRestaurantCode(row.getString(1));
                result.setAverageOrderAmount((float) row.getDouble(2));
                return result;
            });
//            togetherDS.show(100);
            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(resultRDD, RestaurantUser.class);
//            ds.toJavaRDD().saveAsTextFile("C:/spark-test-data/result");
            EsSparkSQL.saveToEs(ds,"cip/cip");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
