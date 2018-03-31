package com.adatafun.dp.service;

import com.adatafun.dp.conf.ESMysqlSpark;
import com.adatafun.dp.model.DmpIndex;
import com.adatafun.dp.util.DateUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.Properties;

/**
 * ProductUsageFrequency.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/5.
 */
public class ProductUsageFrequency {

    public static void main(String[] args){

        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();

        Dataset<Row> productOrder = spark.read().jdbc(prop.getProperty("url"), "tb_order", prop)
                .select("user_id", "order_no", "order_type", "cdate").where("cdate is not null")
                .filter((Row row) -> {
                    DateUtil dateUtil = new DateUtil();
                    Date orderDate = row.getTimestamp(3);
                    Date currentDate = new Date();
                    return dateUtil.daysBetween(orderDate, currentDate) <= 180;
                }).na().drop()
                .groupBy("user_id", "order_type").count();
        productOrder.show(10);

        JavaRDD<DmpIndex> resultRDD = productOrder.toJavaRDD()
                .map((Row row) -> {
                    DmpIndex result = new DmpIndex();
                    String productOrderType = row.getString(1);
                    Long productOrderCount = row.getLong(2);
                    switch (productOrderType) {
                        case "3":
                            if (productOrderCount >= 3) {
                                result.setLimousineUsage("礼宾车高使用率");
                            } else if (productOrderCount >= 1) {
                                result.setLimousineUsage("礼宾车一般使用率");
                            } else {
                                result.setLimousineUsage("从不使用礼宾车");
                            }
                            break;
                        case "10":
                            if (productOrderCount >= 5) {
                                result.setRestaurantUsage("机场餐厅高使用率");
                            } else if (productOrderCount >= 1) {
                                result.setRestaurantUsage("机场餐厅一般使用率");
                            } else {
                                result.setRestaurantUsage("从不使用机场餐厅");
                            }
                            break;
                        case "11":
                            if (productOrderCount >= 3) {
                                result.setParkingUsage("代客泊车高使用率");
                            } else if (productOrderCount >= 1) {
                                result.setParkingUsage("代客泊车一般使用率");
                            } else {
                                result.setParkingUsage("从不使用代客泊车");
                            }
                            break;
                        case "4":
                            if (productOrderCount >= 3) {
                                result.setVvipUsage("要客通高使用率");
                            } else if (productOrderCount >= 1) {
                                result.setVvipUsage("要客通一般使用率");
                            } else {
                                result.setVvipUsage("从不使用要客通");
                            }
                            break;
                        case "7":
                            if (productOrderCount >= 3) {
                                result.setCipUsage("CIP高使用率");
                            } else if (productOrderCount >= 1) {
                                result.setCipUsage("CIP一般使用率");
                            } else {
                                result.setCipUsage("从不使用CIP");
                            }
                            break;
                        default:
                            break;
                    }
                    result.setId(row.get(0).toString() + "*");
                    result.setLongTengId(row.get(0).toString());
                    DateTime dateTime = new DateTime();
                    result.setUpdateTime(dateTime.toString());
                    return result;
                });

        SQLContext context = new SQLContext(spark);
        Dataset ds = context.createDataFrame(resultRDD, DmpIndex.class);
        EsSparkSQL.saveToEs(ds,"dmp-user/dmp-user");

    }

}
