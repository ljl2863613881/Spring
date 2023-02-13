package com.mapred.flow3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    /**
     *
     *
     *
     * */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //当前总流量都是240可能是不同电话，所以这个情况可要考虑进去
        for (Text phone: values
             ) {
            context.write(phone, key);

        }


        //context.write(电话, FlowBean);
    }
}
