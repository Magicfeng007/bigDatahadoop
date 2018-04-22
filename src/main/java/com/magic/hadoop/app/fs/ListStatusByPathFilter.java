package com.magic.hadoop.app.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * @Author: Magicfeng007
 * @Description: 通过正则表达式列出文件
 * 执行命令：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT.jar com.magic.hadoop.app.fs.ListStatus hdfs://magic:8082/ hdfs://magic:8082/user hdfs://magic:8082/user/magic
 * @Date: Created in 2018-04-22---下午8:37
 */
public class ListStatusByPathFilter {
    public static void main(String[] args) throws Exception{
        if(args!=null && args.length>0){
            Path[] paths = new Path[args.length-1];
            for(int i = 1;i<args.length;i++){
                paths[i-1] = new Path(args[i]);
            }
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            RegexExcludePathFilter pathFilter = new RegexExcludePathFilter(args[0]);
//            FileStatus[] fileStatuses = fileSystem.listStatus(paths,pathFilter);
            FileStatus[] fileStatuses = fileSystem.globStatus(paths[0],pathFilter);

            for (FileStatus fileStatus :
                    fileStatuses) {
                System.out.println("fileStatus.getPath():" + fileStatus.getPath());
                System.out.println("fileStatus.getBlockSize():" + fileStatus.getBlockSize());
                System.out.println("fileStatus.getOwner():" + fileStatus.getOwner());
                System.out.println("fileStatus.getGroup():" + fileStatus.getGroup());
                System.out.println("fileStatus.getAccessTime():" + fileStatus.getAccessTime());
                System.out.println("fileStatus.getLen():" + fileStatus.getLen());
                System.out.println("fileStatus.getReplication():" + fileStatus.getReplication());
            }

            Path[] listedPaths = FileUtil.stat2Paths(fileStatuses);
            for (Path p:listedPaths) {
                System.out.println(p);
            }
        }


    }
}
