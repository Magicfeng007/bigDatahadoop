package com.magic.hadoop.app.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: Magicfeng007
 * @Description: 获取NCDC每年最高的温度
 * @Date: Created in 2018-04-15---下午6:17
 */
public class MaxTemperature {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf,"MaxTemperature");
            job.setJarByClass(MaxTemperature.class);
            job.setMapperClass(MaxTemperatureMapper.class);
            job.setReducerClass(MaxTemperatureReducer.class);
            job.setCombinerClass(MaxTemperatureReducer.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }



    }
}
