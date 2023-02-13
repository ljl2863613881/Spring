package com.wordcount.zuoye.shu1;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlWritable implements DBWritable {
    private int id;
    private String goodsName;
    private String goodsType;
    private int goodsCount;
    private double goodsPrice;
    //无参数构造方法、set get方法，toString方法


    public MySqlWritable() {
    }

    @Override
    public String toString() {
        return this.id+"\t"+this.goodsName+"\t"+this.goodsType+"\t"+this.goodsCount+"\t"+this.goodsPrice;
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

    //写入数据到MySQL中方法  参数为用来执行SQL语句的Statement对象
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        System.out.println("写入数据到MySQL中方法");
        preparedStatement.setInt(1, this.id);
        preparedStatement.setString(2, this.goodsName);
        preparedStatement.setString(3, this.goodsType);
        preparedStatement.setInt(4, this.goodsCount);
        preparedStatement.setDouble(5, this.goodsPrice);

    }
    //从执行查询语句后返回的ResultSet对象中读取数据
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        System.out.println("从执行查询语句后返回的ResultSet对象中读取数据");
        this.id = resultSet.getInt(1);
        this.goodsName = resultSet.getString(2);
        this.goodsType = resultSet.getString(3);
        this.goodsCount = resultSet.getInt(4);
        this.goodsPrice = resultSet.getDouble(5);
    }
}