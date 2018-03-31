package com.adatafun.dp.service;

import com.adatafun.dp.conf.ESMysqlSpark;
import com.adatafun.dp.model.DmpIndex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import org.joda.time.DateTime;
import scala.Tuple2;

import java.util.Properties;

/**
 * CreditCardTypeTest.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/16.
 */
public class CreditCardTypeTest {

    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();

        Dataset<Row> bindRecord = spark.read()
                .jdbc(prop.getProperty("url"),"tb_bindrecord", "id", 1, 13722414, 200, prop)
                .withColumnRenamed("dragoncode", "dragon_code")
                .select("user_id", "dragon_code").na().drop();
        bindRecord.show(10);

        Dataset<Row> dragonCard = spark.read()
                .jdbc(prop.getProperty("url"),"tbldragoncard","dragoncardid", 1, 2978724611L, 400, prop)
                .select("agentid", "dragoncode", "balancetype").na().drop()
                .filter("agentid != -1").repartition(400);
        dragonCard.show(10);

        Dataset<Row> agentInfo = spark.read().jdbc(prop.getProperty("url"),"tblagentinfo",prop)
                .select("agentid","agentname", "guestclassname").na().drop(new String[]{"agentid", "agentname"});
        agentInfo.show(10);

        Dataset<Row> bdJoin = bindRecord.join(dragonCard, bindRecord.col("dragon_code").equalTo(dragonCard.col("dragoncode")))
                .select("user_id", "agentid", "dragon_code", "balancetype");
        bdJoin.show(10);
        Dataset<Row> abdJoin = agentInfo.join(bdJoin, agentInfo.col("agentid").equalTo(bdJoin.col("agentid")))
                .select("user_id", "dragon_code", "agentname", "guestclassname", "balancetype");
        abdJoin.show(10);

        JavaRDD<Tuple2<Long, String>> tupleRDD = abdJoin.toJavaRDD().map((Row row) -> {
            Tuple2<Long, String> tuple2;
            Long userId = row.getLong(0);
            String dragonCode = row.getString(1);
            String agentName = row.getString(2);
            String guestClassName;
            if (row.isNullAt(3)) {
                guestClassName = "无";
            } else {
                guestClassName = row.getString(3);
            }
            Integer balanceType = row.getInt(4);
            switch (agentName) {
                case "中国平安VIP俱乐部":
                case "平安VIP":
                    switch (guestClassName) {
                        case "铂金":
                            tuple2 = new Tuple2<>(userId, "铂金级");
                            break;
                        case "非VIP":
                            tuple2 = new Tuple2<>(userId, "非VIP及其他");
                            break;
                        case "黑钻":
                            tuple2 = new Tuple2<>(userId, "黑钻级");
                            break;
                        case "黄金":
                            tuple2 = new Tuple2<>(userId, "黄金级");
                            break;
                        case "钻石":
                            tuple2 = new Tuple2<>(userId, "钻石级");
                            break;
                        default:
                            tuple2 = new Tuple2<>(userId, "非VIP及其他");
                            break;
                    }
                    break;
                case "广发银行信用卡中心":
                    if (!guestClassName.equals("臻尚白金卡") && !dragonCode.substring(0, 7).equals("8908102")) {
                        tuple2 = new Tuple2<>(userId, "大白金");
                    } else if (!guestClassName.equals("臻尚白金卡") && dragonCode.substring(0, 7).equals("8908102")) {
                        tuple2 = new Tuple2<>(userId, "无限卡");
                    } else if (guestClassName.equals("臻尚白金卡") && !dragonCode.substring(0, 7).equals("8908102")) {
                        tuple2 = new Tuple2<>(userId, "小白金");
                    } else {
                        tuple2 = new Tuple2<>(userId, "非VIP及其他");
                    }
                    break;
                case "中信银行信用卡中心":
                case "信用卡中心2012":
                    if (balanceType == 0) {
                        tuple2 = new Tuple2<>(userId, "大白金");
                    } else if (balanceType == 2) {
                        tuple2 = new Tuple2<>(userId, "无限卡");
                    } else {
                        tuple2 = new Tuple2<>(userId, "非VIP及其他");
                    }
                    break;
                case "上海农商银行":
                case "中国光大银行信用卡中心":
                case "中国银行":
                case "交通银行太平洋白金信用卡":
                case "交通银行太平洋白金信用卡-新增会籍":
                case "交行原供应商转换会籍":
                case "交行线上对接无限制点卡":
                case "交行线上对接点卡":
                case "交通银行太平洋白金信用卡-新增会籍（交行呼转开卡）":
                case "世界白金鑫卡":
                case "上海农商行白金商务卡":
                case "光大白金卡":
                case "光大二期客户":
                case "光大一期换卡":
                case "光大银行":
                case "三年白金卡":
                case "五年5点卡":
                case "五年8点卡":
                case "五年白金卡":
                case "阳光存贷合一白金卡":
                case "光大白金卡（电子）":
                case "中银白金卡":
                    tuple2 = new Tuple2<>(userId, "大白金");
                    break;
                case "长城美运卡":
                    tuple2 = new Tuple2<>(userId, "顶级卡");
                    break;
                case "阳光存贷合一尊尚卡":
                case "致尚卡":
                    tuple2 = new Tuple2<>(userId, "钻石卡");
                    break;
                default:
                    tuple2 = new Tuple2<>(userId, "非VIP及其他");
                    break;
            }
            return tuple2;
        });
        System.out.println(tupleRDD.take(10));

        JavaRDD<DmpIndex> javaRDD = tupleRDD.distinct()
                .groupBy((Tuple2<Long, String> tuple2) -> tuple2._1)
                .map((Tuple2<Long, Iterable<Tuple2<Long, String>>> tuple2) -> {
                    DmpIndex dmpIndex = new DmpIndex();
                    Iterable<Tuple2<Long, String>> iterable = tuple2._2;
                    StringBuilder stringBuilder = new StringBuilder();
                    for (Tuple2<Long, String> tuple21 : iterable) {
                        stringBuilder.append(tuple21._2).append(",");
                    }
                    dmpIndex.setId(tuple2._1.toString() + "*");
                    dmpIndex.setLongTengId(tuple2._1.toString());
                    dmpIndex.setCardType(stringBuilder.delete(stringBuilder.length()-1, stringBuilder.length()).toString().split(","));
                    DateTime dateTime = new DateTime();
                    dmpIndex.setUpdateTime(dateTime.toString());
                    return dmpIndex;
                });

        SQLContext context = new SQLContext(spark);
        Dataset ds = context.createDataFrame(javaRDD, DmpIndex.class);
        EsSparkSQL.saveToEs(ds,"dmp-user/dmp-user");
    }

}
