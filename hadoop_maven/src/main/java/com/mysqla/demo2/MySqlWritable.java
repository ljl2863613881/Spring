package com.mysqla.demo2;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlWritable implements DBWritable {
    private int id;
    private String word;
    private int count;
    //无参数构造方法、set get方法，toString方法


    public MySqlWritable() {
    }

    @Override
    public String toString() {
        return "MySqlWritable{" +
                "id=" + id +
                ", word='" + word + '\'' +
                ", count='" + count + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int     count) {
        this.count = count;
    }

    //写入数据到MySQL中方法  参数为用来执行SQL语句的Statement对象
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        System.out.println("写入数据到MySQL中方法");
        preparedStatement.setString(1, this.word);
        preparedStatement.setInt(2, this.count);

    }
    //从执行查询语句后返回的ResultSet对象中读取数据
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        System.out.println("从执行查询语句后返回的ResultSet对象中读取数据");
        this.word = resultSet.getString(1);
        this.count = resultSet.getInt(2);
    }
}