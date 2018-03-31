package com.adatafun.dp.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.lang3.time.DateUtils.parseDate;

/**
 * DateUtil.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/1/19.
 */
public class DateUtil {

    public int getDateSpace(String date1, String date2) throws ParseException {

        Calendar calst = Calendar.getInstance();
        Calendar caled = Calendar.getInstance();

        calst.setTime(parseDate(date1, "yyyyMMdd"));
        caled.setTime(parseDate(date2, "yyyyMMdd"));

        //设置时间为0时
        calst.set(Calendar.HOUR_OF_DAY, 0);
        calst.set(Calendar.MINUTE, 0);
        calst.set(Calendar.SECOND, 0);
        caled.set(Calendar.HOUR_OF_DAY, 0);
        caled.set(Calendar.MINUTE, 0);
        caled.set(Calendar.SECOND, 0);
        //得到两个日期相差的天数
        int days = ((int)(caled.getTime().getTime()/1000)-(int)(calst.getTime().getTime()/1000))/3600/24;

        return days;
    }

    public int daysBetween(Date early, Date late) {

        Calendar calst = Calendar.getInstance();
        Calendar caled = Calendar.getInstance();
        calst.setTime(early);
        caled.setTime(late);
        //设置时间为0时
        calst.set(Calendar.HOUR_OF_DAY, 0);
        calst.set(Calendar.MINUTE, 0);
        calst.set(Calendar.SECOND, 0);
        caled.set(Calendar.HOUR_OF_DAY, 0);
        caled.set(Calendar.MINUTE, 0);
        caled.set(Calendar.SECOND, 0);
        //得到两个日期相差的天数
        int days = ((int) (caled.getTime().getTime() / 1000) - (int) (calst
                .getTime().getTime() / 1000)) / 3600 / 24;

        return days;
    }

    /**
     * @param args 入参
     */
    public static void main(String[] args) throws Exception {
        DateUtil f = new DateUtil();
        Date date = new Date();//取时间

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date sdd = formatter.parse("2011-03-27 02:30:00");
        System.out.println(f.daysBetween(sdd, date));
        String dateString = formatter.format(date);

        System.out.println(date);

        int i = 4;
        switch(i){
            case 0:
            case 1:
                System.out.println("1");
                break;
            case 2:
            case 3:
                System.out.println("3");
                break;
            default:
                System.out.println("default");
                break;
        }
    }

}
