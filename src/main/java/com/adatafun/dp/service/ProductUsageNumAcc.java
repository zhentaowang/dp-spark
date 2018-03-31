package com.adatafun.dp.service;

import com.adatafun.dp.conf.ESMysqlSpark;
import com.adatafun.dp.model.RestaurantUser;
import com.adatafun.dp.util.DateUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.Date;
import java.util.Properties;

/**
 * ProductUsageNumAcc.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/21.
 */
public class ProductUsageNumAcc {

    public static void main(String[] args){

        SparkConf sparkConf = ESMysqlSpark.getSparkConf();
        System.out.println(sparkConf.get("es.update.script"));
        sparkConf.set("es.update.script", sparkConf.get("es.update.script").replaceAll("fieldName", "limousineUsageNum"));
        System.out.println(sparkConf.get("es.update.script"));
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Properties prop = ESMysqlSpark.getMysqlConf3();
        Dataset<Row> productOrder = spark.read().jdbc(prop.getProperty("url"), "tb_order", prop)
                .select("user_id", "order_no", "order_type", "cdate").where("cdate is not null")
                .filter((Row row) -> {
                    DateUtil dateUtil = new DateUtil();
                    Date orderDate = row.getTimestamp(3);
                    Date currentDate = new Date();
                    String productOrderType = row.getString(2);
                    return dateUtil.daysBetween(orderDate, currentDate) == 1 && productOrderType.equals("3");
                }).na().drop();
        productOrder.show(10);

        JavaRDD<RestaurantUser> resultRDD = productOrder.toJavaRDD().map((Row row) -> {
            RestaurantUser restaurantUser = new RestaurantUser();
            restaurantUser.setId(row.get(0).toString() + "*");
            restaurantUser.setUserId(row.get(0).toString());
            restaurantUser.setLimousineUsageNum(1L);
            return restaurantUser;
        });

        SQLContext context = new SQLContext(spark);
        Dataset ds = context.createDataFrame(resultRDD, RestaurantUser.class);
        EsSparkSQL.saveToEs(ds,"cip/cip");
        spark.stop();

    }

}
