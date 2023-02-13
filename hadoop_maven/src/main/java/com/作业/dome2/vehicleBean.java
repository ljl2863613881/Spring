package com.作业.dome2;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class vehicleBean implements DBWritable, WritableComparable<vehicleBean> {
    private String vehicleID;
    private String plateID;
    private String type ;
    private String model;
    private int lineID;
    private String driveID = "null";

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.vehicleID);
        preparedStatement.setString(2, this.plateID);
        preparedStatement.setString(3, this.type);
        preparedStatement.setString(4,this.model);
        preparedStatement.setInt(5, this.lineID);
        preparedStatement.setString(6, this.driveID);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }

    @Override
    public String toString() {
        return  vehicleID +
                " " + plateID +
                " " + type +
                " " + model +
                " " + lineID +
                " " + driveID
                ;
    }
    public String getVehicleID() {
        return vehicleID;
    }

    public void setVehicleID(String vehicleID) {
        this.vehicleID = vehicleID;
    }



    public String getPlateID() {
        return plateID;
    }

    public void setPlateID(String plateID) {
        this.plateID = plateID;
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

    public String getDriveID() {
        return driveID;
    }





    public void setDriveID(String driveID) {
        this.driveID = driveID;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.vehicleID);
        dataOutput.writeUTF(this.plateID);
        dataOutput.writeUTF(this.model);
        dataOutput.writeUTF(this.type);

        dataOutput.writeInt(this.lineID);
        dataOutput.writeUTF(this.driveID);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vehicleID = dataInput.readUTF();
        this.plateID = dataInput.readUTF();
        this.model = dataInput.readUTF();
        this.type = dataInput.readUTF();
        this.lineID = dataInput.readInt();
        this.driveID = dataInput.readUTF();
    }

    @Override
    public int compareTo(vehicleBean o) {
        return   this.lineID > o.getLineID() ? -1 : 1;
    }
}
