package com.mysqla.Mysqlread;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlWritbale implements DBWritable {
    private int id;
    private String  goodsName;
    private String  goodsType;
    private int goodsCount;
    private double goodsPrice;

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        System.out.println("读写数据库");
        preparedStatement.setInt(1,this.getId());
        preparedStatement.setString(2,this.getGoodsName());
        preparedStatement.setString(3,this.getGoodsType());
        preparedStatement.setInt(4,this.getGoodsCount());
        preparedStatement.setDouble(5,this.getGoodsPrice());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        System.out.println("--=-=-=-=-=-=-=-=-=");
        this.id=resultSet.getInt(1);
        this.goodsName=resultSet.getString(2);
        this.goodsType=resultSet.getString(3);
        this.goodsCount=resultSet.getInt(4);
        this.goodsPrice=resultSet.getDouble(5);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }

    public String getGoodsType() {
        return goodsType;
    }

    public void setGoodsType(String goodsType) {
        this.goodsType = goodsType;
    }

    public int getGoodsCount() {
        return goodsCount;
    }

    public void setGoodsCount(int goodsCount) {
        this.goodsCount = goodsCount;
    }

    public double getGoodsPrice() {
        return goodsPrice;
    }

    public void setGoodsPrice(double goodsPrice) {
        this.goodsPrice = goodsPrice;
    }

    @Override
    public String toString() {
        return  this.id + "\t" + this.goodsName + "\t" + this.goodsType + "\t" + this.goodsCount + "\t" + this.goodsPrice;
    }
}