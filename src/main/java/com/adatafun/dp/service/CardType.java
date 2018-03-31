package com.adatafun.dp.service;

import com.adatafun.dp.conf.ESMysqlSpark;
import com.adatafun.dp.model.RestaurantUser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.*;

import java.lang.Boolean;
import java.lang.Long;
import java.util.*;

/**
 * CardType.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/5.
 */
public class CardType {

    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();

        Dataset<Row> df = spark.read().jdbc(prop.getProperty("url"),"tbldragoncard", prop);
        df.createOrReplaceTempView("tbldragoncard");
        Dataset<Row> dataset = df.sqlContext().sql("select agentid, dragoncode, createtime, balancetype from tbldragoncard where agentid != -1")
                .repartition(100).withColumnRenamed("dragoncode", "dragon_code_2").na().drop();
        dataset.show(10);

        Dataset<Row> bindRecord = spark.read()
                .jdbc(prop.getProperty("url"),"tb_bindrecord", "id", 1, 3686000, 10, prop)
                .withColumnRenamed("dragoncode", "dragon_code_1")
                .select("user_id", "dragon_code_1").na().drop();
        bindRecord.show(10);
        Dataset<Row> dragonCard = spark.read()
                .jdbc(prop.getProperty("url"),"tbldragoncard","dragoncardid", 1, 29725601, 10, prop)
                .withColumnRenamed("dragoncode", "dragon_code_2").select("agentid", "dragon_code_2", "balancetype").na().drop()
                .filter("dragoncardid < 29725601").filter("agentid != -1");
        dragonCard.show(10);
        Dataset<Row> agentInfo = spark.read().jdbc(prop.getProperty("url"),"tblagentinfo",prop)
                .select("agentid","agentname", "guestclassname").na().drop(new String[]{"agentid"});
        agentInfo.show(10);

        JavaPairRDD<String, Long> firstRDD = bindRecord.toJavaRDD().mapToPair(new PairFunction<Row, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(1), row.getLong(0));//<d,u>
            }
        });
        JavaPairRDD<String, Tuple2<Long, Integer>> secondRDD = dragonCard.toJavaRDD().mapToPair(new PairFunction<Row, String, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Long, Integer>> call(Row row) throws Exception {
                Integer balanceType;
                if (row.isNullAt(2)) {
                    balanceType = -1;
                } else {
                    balanceType = row.getInt(2);
                }
                Tuple2<Long, Integer> tuple2 = new Tuple2<>(row.getLong(0), balanceType);
                return new Tuple2<>(row.getString(1), tuple2);//<d,<a,b>>
            }
        });

        JavaPairRDD<String, Tuple2<Long, Tuple2<Long, Integer>>> joinRDD = firstRDD.join(secondRDD);
        JavaRDD<Tuple4<Long, String, Long, Integer>> forthRDD = joinRDD
                .map(new Function<Tuple2<String, Tuple2<Long, Tuple2<Long, Integer>>>, Tuple4<Long, String, Long, Integer>>() {
                    @Override
                    public Tuple4<Long, String, Long, Integer> call(Tuple2<String, Tuple2<Long, Tuple2<Long, Integer>>> param) throws Exception {
                        return new Tuple4<>(param._2._2._1, param._1, param._2._1, param._2._2._2);//<a,d,u,b>
                    }
                });
        JavaRDD<Tuple4<Long, String, Long, Integer>> forthRDDNotNull = forthRDD.filter(new Function<Tuple4<Long, String, Long, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple4<Long, String, Long, Integer> tuple4) throws Exception {
                return !tuple4._1().toString().isEmpty();
            }
        });
        JavaRDD<Tuple2<Long, String>> forthRDDNull = forthRDD.filter(new Function<Tuple4<Long, String, Long, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple4<Long, String, Long, Integer> tuple4) throws Exception {
                return !tuple4._1().toString().isEmpty();
            }
        }).map(new Function<Tuple4<Long, String, Long, Integer>, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> call(Tuple4<Long, String, Long, Integer> tuple4) throws Exception {
                return new Tuple2<>(tuple4._3(), "非VIP及其他");
            }
        });

        JavaPairRDD<Long, Tuple3<String, Long, Integer>> fifthRDD = forthRDDNotNull
                .mapToPair(new PairFunction<Tuple4<Long, String, Long, Integer>, Long, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple2<Long, Tuple3<String, Long, Integer>> call(Tuple4<Long, String, Long, Integer> tuple4) throws Exception {
                Tuple3<String, Long, Integer> tuple3 = new Tuple3<>(tuple4._2(), tuple4._3(), tuple4._4());
                return new Tuple2<>(tuple4._1(), tuple3);//<a,<d,u,b>>
            }
        });
        JavaPairRDD<Long, Tuple2<String, String>> thirdRDD = agentInfo.toJavaRDD().mapToPair(new PairFunction<Row, Long, Tuple2<String, String>>() {
            @Override
            public Tuple2<Long, Tuple2<String, String>> call(Row row) throws Exception {
                String guestClassName;
                if (row.isNullAt(2)) {
                    guestClassName = "无";
                } else {
                    guestClassName = row.getString(2);
                }
                Tuple2<String, String> tuple2 = new Tuple2<>(row.getString(1), guestClassName);
                return new Tuple2<>(row.getLong(0), tuple2);//<a,<ae,g>>
            }
        });

        JavaPairRDD<Long, Tuple2<Tuple3<String, Long, Integer>, Tuple2<String, String>>> sixthRDD = fifthRDD.join(thirdRDD);
        JavaRDD<Tuple5<Long, String, String, Integer, String>> tuple5JavaRDD = sixthRDD
                .map(new Function<Tuple2<Long, Tuple2<Tuple3<String, Long, Integer>, Tuple2<String, String>>>, Tuple5<Long, String, String, Integer, String>>() {
            @Override
            public Tuple5<Long, String, String, Integer, String> call(Tuple2<Long, Tuple2<Tuple3<String, Long, Integer>, Tuple2<String, String>>> param) throws Exception {
                return new Tuple5<>(param._2._1._2(), param._2._2._1, param._2._1._1(), param._2._1._3(), param._2._2._2);//<u,ae,d,b,g>
            }
        });

        JavaRDD<Tuple2<Long, String>> tupleRDD = tuple5JavaRDD.map((Tuple5<Long, String, String, Integer, String> tuple5) -> {
            Tuple2<Long, String> tuple2 = null;
            Long userId = tuple5._1();
            String agentName = tuple5._2();
            String dragonCode = tuple5._3();
            Integer balanceType = tuple5._4();
            String guestClassName = tuple5._5();
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
        }).union(forthRDDNull);

        JavaPairRDD<Long, Iterable<Tuple2<Long, String>>> rdd = tupleRDD.groupBy(new Function<Tuple2<Long, String>, Long>() {
            @Override
            public Long call(Tuple2<Long, String> tuple2) throws Exception {
                return tuple2._1;
            }
        });
        JavaRDD<RestaurantUser> javaRDD = rdd.map(new Function<Tuple2<Long, Iterable<Tuple2<Long, String>>>, RestaurantUser>() {
            @Override
            public RestaurantUser call(Tuple2<Long, Iterable<Tuple2<Long, String>>> tuple2) throws Exception {
                RestaurantUser restaurantUser = new RestaurantUser();
                Iterable<Tuple2<Long, String>> iterable = tuple2._2;
                StringBuilder stringBuilder = new StringBuilder();
                for (Tuple2<Long, String> tuple21 : iterable) {
                    stringBuilder.append(tuple21._2).append(",");
                }
                restaurantUser.setId(tuple2._1.toString());
                restaurantUser.setUserId(tuple2._1.toString());
                restaurantUser.setCardType(stringBuilder.delete(stringBuilder.length()-1, stringBuilder.length()).toString().split(","));
                return restaurantUser;
            }
        });

        SQLContext context = new SQLContext(spark);
        Dataset ds = context.createDataFrame(javaRDD, RestaurantUser.class);
//            ds.toJavaRDD().saveAsTextFile("C:/spark-test-data/result");
        EsSparkSQL.saveToEs(ds,"cip/cip");
    }

}
