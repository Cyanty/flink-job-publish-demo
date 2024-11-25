package com.job.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * @author lcy
 */
public class HdfsUtils {
    private final static Logger logger = LoggerFactory.getLogger(HdfsUtils.class);

    /**
     * 上传文件到hdfs
     * @param localFilePath 本地文件夹/文件路径
     * @param hdfsFilePath hdfs目标文件夹路径
     * @param conf hadoop配置
     * @param isOverwrite 是否覆写
     */
    public static void uploadFilesToHdfs(String localFilePath,
                                         String hdfsFilePath,
                                         Configuration conf,
                                         boolean isOverwrite) throws IOException {
        File localDirOrFile = new File(localFilePath);
        if (localDirOrFile.isDirectory()) {
            File[] files = localDirOrFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        uploadFile(file.getAbsolutePath(), hdfsFilePath + "/" + file.getName(), conf, isOverwrite);
                    }
                }
            }
        } else {
            if (localDirOrFile.isFile()){
                String fileName = localDirOrFile.getName();
                uploadFile(localDirOrFile.getAbsolutePath(), hdfsFilePath + "/" + fileName, conf, isOverwrite);
            }
        }
    }

    private static void uploadFile(String localFilePath,
                                   String hdfsFilePath,
                                   Configuration conf,
                                   boolean isOverwrite) throws IOException {
        String hdfsUri = conf.get("fs.defaultFS");
        try (FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf)) {
            Path localPath = new Path(localFilePath);
            Path hdfsPath = new Path(hdfsFilePath);
            if (isOverwrite){
                fs.copyFromLocalFile(false, true, localPath, hdfsPath);
                logger.info("Path:{} Overwrite Upload complete.", hdfsPath);
                return;
            }
            if (fs.exists(hdfsPath)) { // 检查文件是否存在
                logger.info("File already exists in HDFS. Skipping upload.");
            } else {
                fs.copyFromLocalFile(localPath, hdfsPath);
                logger.info("Path:{} Upload complete.", hdfsPath);
            }
        }
    }

    public static String findFileInHdfs(String hdfsFilePath, Configuration conf) throws IOException {
        String hdfsUri = conf.get("fs.defaultFS");
        try (FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf)) {
            Path hdfsPath = new Path(hdfsFilePath);
            FileStatus[] statuses = fs.listStatus(hdfsPath); // 获取当前路径下的所有文件和目录
            for (FileStatus status : statuses) {
                // 如果是文件并且文件名匹配
                if (status.isFile() && status.getPath().getName().matches("flink-dist.*\\.jar")) {
                    return status.getPath().toString();
                }
                // 如果是目录，递归查找
                else if (status.isDirectory()) {
                    String flinkDistJar = findFileInHdfs(status.getPath().toString().replace(hdfsUri,""), conf);
                    if (flinkDistJar != null){
                        return flinkDistJar;
                    }
                }
            }
        }
        return null;
    }

    public static void deleteFile(String hdfsFilePath, Configuration conf) throws IOException {
        String hdfsUri = conf.get("fs.defaultFS");
        try (FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf)) {
            Path hdfsPath = new Path(hdfsFilePath);
            if (fs.exists(hdfsPath)) {
                boolean deleted = fs.delete(hdfsPath, false); // false表示不递归删除
                if (deleted) {
                    logger.info("文件 " + hdfsFilePath + " 已删除");
                } else {
                    logger.error("文件 " + hdfsFilePath + " 删除失败");
                }
            } else {
                logger.info("文件 " + hdfsFilePath + " 不存在");
            }
        }
    }


}
