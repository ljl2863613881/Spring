package com.作业.dome2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class vehicleMapper extends Mapper<LongWritable, Text, Text, vehicleBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println(line);
        String[] types = line.split(" ");
        String type = types[4];

        vehicleBean mysqlWritable = new vehicleBean();
        mysqlWritable.setVehicleID(types[0]);//文件第一个，下标为0
        mysqlWritable.setPlateID(types[1]);//文件第二个，下标为1
        mysqlWritable.setModel(types[2]);//文件第三个，下标为2
        mysqlWritable.setType(types[3]);//文件第四个，下标为3
        mysqlWritable.setLineID(Integer.parseInt(types[4]));//文件第五个，下标为4
        if (types.length == 6) {//判断如果这行的数据是否等于6
            mysqlWritable.setDriveID(types[5]);//有6条数据，变写出文件第6个数据
        }
        context.write(new Text(type), mysqlWritable );//写入
    }
}

