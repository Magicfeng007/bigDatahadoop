package com.magic.hadoop.app.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Magicfeng007
 * @Description:
 * @Date: Created in 2018-04-15---下午7:06
 */
public class MaxTemperatureMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
    private IntWritable temper = new IntWritable();
    private IntWritable years = new IntWritable();
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String year = line.toString().substring(15,19);
        String temp = line.toString().substring(87,92);
        temper.set(Integer.parseInt(temp));
        years.set(Integer.parseInt(year));
        context.write(years,temper);
    }
}
