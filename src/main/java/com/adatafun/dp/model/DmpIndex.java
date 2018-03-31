package com.adatafun.dp.model;

import org.joda.time.DateTime;

import java.util.Date;

/**
 * DmpIndex.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/16.
 */
public class DmpIndex {

    private String id;
    private String longTengId;
    private String limousineUsage;
    private String restaurantUsage;
    private String parkingUsage;
    private String vvipUsage;
    private String cipUsage;
    private String[] cardType;
    private String updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLongTengId() {
        return longTengId;
    }

    public void setLongTengId(String longTengId) {
        this.longTengId = longTengId;
    }

    public String getLimousineUsage() {
        return limousineUsage;
    }

    public void setLimousineUsage(String limousineUsage) {
        this.limousineUsage = limousineUsage;
    }

    public String getRestaurantUsage() {
        return restaurantUsage;
    }

    public void setRestaurantUsage(String restaurantUsage) {
        this.restaurantUsage = restaurantUsage;
    }

    public String getParkingUsage() {
        return parkingUsage;
    }

    public void setParkingUsage(String parkingUsage) {
        this.parkingUsage = parkingUsage;
    }

    public String getVvipUsage() {
        return vvipUsage;
    }

    public void setVvipUsage(String vvipUsage) {
        this.vvipUsage = vvipUsage;
    }

    public String getCipUsage() {
        return cipUsage;
    }

    public void setCipUsage(String cipUsage) {
        this.cipUsage = cipUsage;
    }

    public String[] getCardType() {
        return cardType;
    }

    public void setCardType(String[] cardType) {
        this.cardType = cardType;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
