package com.adatafun.dp.model;

import java.io.Serializable;
import java.util.Date;

/**
 * RestaurantUser.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/2.
 */
public class RestaurantUser implements Serializable{

    private String id;
    private String userId;
    private String restaurantCode;
    private Float averageOrderAmount;
    private String limousineUsage;
    private Long limousineUsageNum;
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

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRestaurantCode() {
        return restaurantCode;
    }

    public void setRestaurantCode(String restaurantCode) {
        this.restaurantCode = restaurantCode;
    }

    public Float getAverageOrderAmount() {
        return averageOrderAmount;
    }

    public void setAverageOrderAmount(Float averageOrderAmount) {
        this.averageOrderAmount = averageOrderAmount;
    }

    public String getLimousineUsage() {
        return limousineUsage;
    }

    public void setLimousineUsage(String limousineUsage) {
        this.limousineUsage = limousineUsage;
    }

    public Long getLimousineUsageNum() {
        return limousineUsageNum;
    }

    public void setLimousineUsageNum(Long limousineUsageNum) {
        this.limousineUsageNum = limousineUsageNum;
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
