package com.mysqla.demo1;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlWritable implements DBWritable {
    private int id;
    private String country;
    private String lastupdate;
    //无参数构造方法、set get方法，toString方法


    public MySqlWritable() {
    }

    @Override
    public String toString() {
        return this.id +"\t"+this.country+"\t"+this.lastupdate;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getLastupdate() {
        return lastupdate;
    }

    public void setLastupdate(String lastupdate) {
        this.lastupdate = lastupdate;
    }

    //写入数据到MySQL中方法  参数为用来执行SQL语句的Statement对象
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        System.out.println("写入数据到MySQL中方法");
        preparedStatement.setInt(1, this.id);
        preparedStatement.setString(2, this.country);
        preparedStatement.setString(3, this.lastupdate);

    }
    //从执行查询语句后返回的ResultSet对象中读取数据
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        System.out.println("从执行查询语句后返回的ResultSet对象中读取数据");
        this.id = resultSet.getInt(1);
        this.country = resultSet.getString(2);
        this.lastupdate = resultSet.getString(3);
    }
}