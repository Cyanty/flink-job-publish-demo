package com.job.handler;

import com.job.config.ConfigBeanClazz;
import com.job.domain.ApplicationClientEntity;
import com.job.domain.FlinkDeploymentMode;
import com.job.domain.PerJobClientEntity;
import com.job.domain.PrepareEntity;
import com.job.publish.YarnClientAware;
import com.job.publish.impl.ApplicationClient;
import com.job.publish.impl.PerJobClient;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * @author lcy
 * @description: 创建客户端，建议将flink安装包下的 lib/(包含flink-dist*.jar)和plugins/文件夹 上传到hdfs，作为依赖的文件夹引用
 */
public class YarnClientFactory {
    /**
     *
     * @param publishType  flink on yarn任务提交类型
     * @param localUserJar  本地jar包路径
     * @param localUserJarLib  本地jar包依赖lib/路径
     * @param hdfsUserJar  hdfs上传jar包路径
     * @param hdfsUserJarLib  hdfs上传jar包依赖lib/路径
     * @param flinkDistJarPath  flink-dist_*.jar文件路径
     * @param applicationName  任务名称
     * @param entryPointClassName  任务运行主类
     * @return  提交任务客户端
     * @throws IOException
     */
    public static YarnClientAware getYarnClient(String publishType,
                                                String localUserJar,
                                                String localUserJarLib,
                                                String hdfsUserJar,
                                                String hdfsUserJarLib,
                                                String flinkDistJarPath,
                                                String applicationName,
                                                String entryPointClassName) throws IOException {
        PrepareEntity prepareEntity = prepare(localUserJar, localUserJarLib, hdfsUserJar, hdfsUserJarLib, flinkDistJarPath);
        YarnClientAware yarnClient = null;
        if (publishType.equals(FlinkDeploymentMode.PER_JOB.getMode())){
            yarnClient = new PerJobClient(
                    prepareEntity.getPerJobClientEntity().getFlinkDistJar(),
                    prepareEntity.getPerJobClientEntity().getUserJar(),
                    prepareEntity.getPerJobClientEntity().getUserJarLib(),
                    applicationName,
                    entryPointClassName);
        }
        if (publishType.equals(FlinkDeploymentMode.APPLICATION.getMode())){
            yarnClient = new ApplicationClient(
                    prepareEntity.getApplicationClientEntity().getFlinkDistJar(),
                    prepareEntity.getApplicationClientEntity().getUserJar(),
                    prepareEntity.getApplicationClientEntity().getUserJarLib(),
                    applicationName,
                    entryPointClassName);
        }
        return Objects.requireNonNull(yarnClient);
    }

    public static PrepareEntity prepare(String localUserJar,
                                        String localUserJarLib,
                                        String hdfsUserJar,
                                        String hdfsUserJarLib,
                                        String flinkDistJarPath) throws IOException {
        String hdfsUri = ConfigBeanClazz.conf.get("fs.defaultFS");
        String userJarFileName = new File(localUserJar).getName();
        HdfsUtils.uploadFilesToHdfs(
                localUserJar,
                hdfsUserJar,
                ConfigBeanClazz.conf,
                true);
        HdfsUtils.uploadFilesToHdfs(
                localUserJarLib,
                hdfsUserJarLib,
                ConfigBeanClazz.conf,
                false);
        String flinkDistJar = HdfsUtils.findFileInHdfs(flinkDistJarPath, ConfigBeanClazz.conf);
        Objects.requireNonNull(flinkDistJar, "FlinkDistJar is not found.");

        PrepareEntity prepareEntity = new PrepareEntity();
        prepareEntity.setPerJobClientEntity(
                new PerJobClientEntity(
                        localUserJar,
                        hdfsUri + hdfsUserJarLib,
                        flinkDistJar));
        prepareEntity.setApplicationClientEntity(
                new ApplicationClientEntity(
                        hdfsUri + hdfsUserJar + "/" + userJarFileName,
                        hdfsUri + hdfsUserJarLib,
                        flinkDistJar));

        return prepareEntity;
    }
}
