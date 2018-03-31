package com.adatafun.dp.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * ApplicationProperty.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/2.
 */
public class ApplicationProperty {

    private final Properties prop = new Properties();
    private static ApplicationProperty instance = null;

    private ApplicationProperty(){}

    public static synchronized ApplicationProperty getInstance(){
        if (instance == null){
            instance = new ApplicationProperty();
            return instance;
        } else {
            return instance;
        }
    }

    public Properties getProperty(){
        try {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
            this.prop.load(in);
            in.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        return this.prop;
    }

    public static void main(String[] args){
        ApplicationProperty appConfig = ApplicationProperty.getInstance();
        Properties prop = appConfig.getProperty();
        System.out.println(prop.getProperty("spark.appName"));
    }

}
