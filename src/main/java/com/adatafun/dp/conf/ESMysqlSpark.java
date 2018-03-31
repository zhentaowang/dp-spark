package com.adatafun.dp.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * ESMysqlSpark.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/2.
 */
public class ESMysqlSpark {
    public static SparkSession getSession(){
        SparkConf conf = new SparkConf().setAppName("testEs").setMaster("local[*]").set("spark.local.dir", "C:/spark-test-data");
//        SparkConf conf = new SparkConf().setAppName("dmpProcess").setMaster("spark://192.168.1.131:7077");
        Properties props = ApplicationProperty.getInstance().getProperty();
        conf.set("es.index.auto.create",props.getProperty("es.index.auto.create"));
        conf.set("es.nodes",props.getProperty("es.nodes"));
        conf.set("es.port",props.getProperty("es.port"));
        conf.set("es.nodes.wan.only",props.getProperty("es.nodes.wan.only"));
        conf.set("es.mapping.id",props.getProperty("es.mapping.id"));
        conf.set("es.write.operation",props.getProperty("es.write.operation"));
        conf.set("spark.sql.crossJoin.enabled","true");
//        conf.set("spark.default.parallelism", "500");
//        conf.set("spark.executor.memory", "6g");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        return SparkSession.builder().config(conf).getOrCreate();
    }

    public static SparkConf getSparkConf(){
        Properties props = ApplicationProperty.getInstance().getProperty();
        String appName = props.getProperty("spark.appName");
        String master = props.getProperty("spark.master");
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.local.dir", "C:/spark-test-data");
//        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        conf.set("es.index.auto.create",props.getProperty("es.index.auto.create"));
        conf.set("es.nodes",props.getProperty("es.nodes"));
        conf.set("es.port",props.getProperty("es.port"));
        conf.set("es.nodes.wan.only",props.getProperty("es.nodes.wan.only"));
        conf.set("es.mapping.id",props.getProperty("es.mapping.id"));
        conf.set("es.write.operation",props.getProperty("es.write.operation"));
        conf.set("spark.sql.crossJoin.enabled","true");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("es.update.script", props.getProperty("es.update.script"));
        return conf;
    }

    public static Properties getMysqlConf(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl"));
        propMysql.setProperty("user",appConf.getProperty("user"));
        propMysql.setProperty("password",appConf.getProperty("password"));
        propMysql.setProperty("driver","com.mysql.cj.jdbc.Driver");
        return propMysql;
    }

    public static Properties getMysqlConf2(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl2"));
        propMysql.setProperty("user",appConf.getProperty("user2"));
        propMysql.setProperty("password",appConf.getProperty("password2"));
        propMysql.setProperty("driver","com.mysql.cj.jdbc.Driver");
        return propMysql;
    }

    public static Properties getMysqlConf3(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl3"));
        propMysql.setProperty("user",appConf.getProperty("user3"));
        propMysql.setProperty("password",appConf.getProperty("password3"));
        propMysql.setProperty("driver","com.mysql.cj.jdbc.Driver");
        propMysql.setProperty("serverTimezone","Asia/Shanghai");
        return propMysql;
    }
}
