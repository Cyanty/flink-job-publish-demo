package com.job.config;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.URL;
import java.util.Objects;

/**
 * @author lcy
 */
public class ConfigBeanClazz {
    public final static String CORE_SITE = "conf/core-site.xml";
    public final static String YARN_SITE = "conf/yarn-site.xml";
    public final static String HDFS_SITE = "conf/hdfs-site.xml";
    public final static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    public static String flinkConfPath;
    public static YarnClient yarnClient = YarnClient.createYarnClient();
    public static YarnConfiguration yarnConfiguration;

    static {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.addResource(CORE_SITE);
        conf.addResource(YARN_SITE);
        conf.addResource(HDFS_SITE);

        URL flinkConfUrl = ConfigBeanClazz.class.getClassLoader().getResource("flink-conf/");
        Objects.requireNonNull(flinkConfUrl, "flink conf path is null");
        flinkConfPath = flinkConfUrl.getPath();

        yarnConfiguration = new YarnConfiguration(conf);
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

    }

}
