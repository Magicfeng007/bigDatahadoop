package com.magic.hadoop.app.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;

/**
 * @Author: Magicfeng007
 * @Description: 使用FileSystem来读取hadoop hdfs 中的文件并输出
 * 使用命令执行：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT.jar com.magic.hadoop.app.fs.FileSystemCat hdfs://magic:8082/user/magic/core-site.xml
 * @Date: Created in 2018-04-22---下午5:05
 */
public class FileSystemCat {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        InputStream in = null;
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            in = fileSystem.open(new Path(args[0]));
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            if(in != null){
                IOUtils.closeStream(in);
            }
        }
    }
}
