package com.作业.dome1.写入;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlWritable implements DBWritable {
    private int vehicleID;
    private String plateNo;
    private String model;
    private String type;
    private int  lineID;
    private int driverID;


    public MySqlWritable() {
    }

    @Override
    public String toString() {
        return "MySqlWritable{" +
                "vehicleID=" + vehicleID +
                ", plateNo='" + plateNo + '\'' +
                ", model='" + model + '\'' +
                ", type='" + type + '\'' +
                ", lineID=" + lineID +
                ", driverID=" + driverID +
                '}';
    }

    public int getVehicleID() {
        return vehicleID;
    }

    public void setVehicleID(int vehicleID) {
        this.vehicleID = vehicleID;
    }

    public String getPlateNo() {
        return plateNo;
    }

    public void setPlateNo(String plateNo) {
        this.plateNo = plateNo;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLineID() {
        return lineID;
    }

    public void setLineID(int lineID) {
        this.lineID = lineID;
    }

    public int getDriverID() {
        return driverID;
    }

    public void setDriverID(int driverID) {
        this.driverID = driverID;
    }

    //写入数据到MySQL中方法  参数为用来执行SQL语句的Statement对象
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        System.out.println("写入");
        preparedStatement.setInt(1,this.vehicleID);
        preparedStatement.setString(2,this.plateNo);
        preparedStatement.setString(3,this.model);
        preparedStatement.setString(4,this.type);
        preparedStatement.setInt(5,this.lineID);
        preparedStatement.setInt(6,this.driverID);


    }
    //从执行查询语句后返回的ResultSet对象中读取数据
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        System.out.println("读取");
        this.plateNo=resultSet.getString(1);
        this.model=resultSet.getString(2);
        this.type=resultSet.getString(3);
        this.lineID=resultSet.getInt(4);
        this.driverID=resultSet.getInt(5);
    }
}
