package com.magic.hadoop.app.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;

/**
 * @Author: Magicfeng007
 * @Description: 获取文件元数据
 *执行命令：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT.jar com.magic.hadoop.app.fs.ShowFileStatusTest hdfs://magic:8082/user/magic/core-site.xml
 * @Date: Created in 2018-04-22---下午8:14
 */
public class ShowFileStatusTest {
    public static void main(String[] args) throws Exception{
        String dstFile = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(dstFile));
        System.out.println("fileStatus.getBlockSize():" + fileStatus.getBlockSize());
        System.out.println("fileStatus.getOwner():" + fileStatus.getOwner());
        System.out.println("fileStatus.getGroup():" + fileStatus.getGroup());
        System.out.println("fileStatus.getAccessTime():" + fileStatus.getAccessTime());
        System.out.println("fileStatus.getLen():" + fileStatus.getLen());
        System.out.println("fileStatus.getReplication():" + fileStatus.getReplication());

    }
}
