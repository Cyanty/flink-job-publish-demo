package com.job.publish.impl;

import com.job.config.ConfigBeanClazz;
import com.job.publish.YarnClientAware;
import com.job.handler.Common;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.*;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

/**
 * @author lcy
 */
public class PerJobClient implements YarnClientAware {
    private static final Logger logger = LoggerFactory.getLogger(PerJobClient.class);
    private final String flinkDistJar;
    private final String userJar;
    private final String userJarLib;
    private final String applicationName;
    private final String entryPointClassName;

    /**
     * @author lcy
     * @description yarn per-job模式提交任务
     * @param flinkDistJar flinkDistJar hdfs路径
     * @param userJar 本地jar路径
     * @param userJarLib hdfs lib文件夹路径
     * @param applicationName flink作业名称
     * @param entryPointClassName 主类名
     */
    public PerJobClient(String flinkDistJar, String userJar, String userJarLib, String applicationName, String entryPointClassName) {
        this.flinkDistJar = flinkDistJar;
        this.userJar = userJar;
        this.userJarLib = userJarLib;
        this.applicationName = applicationName;
        this.entryPointClassName = entryPointClassName;
    }

    public void doSubmit() {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(ConfigBeanClazz.flinkConfPath);
        flinkConfig
                .set(DeploymentOptions.TARGET, YarnDeploymentTarget.PER_JOB.getName())
                .set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(userJarLib)) // 可以将flinkDistJar放在指定的lib/路径下
                // 可选参数配置
                .set(JobManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1024", MemorySize.MemoryUnit.MEGA_BYTES))
                .set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1024", MemorySize.MemoryUnit.MEGA_BYTES));
        logger.info("|Effective submit configuration: " + flinkConfig +"|");

        DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
        ClusterClientFactory<ApplicationId> clientFactory = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);

        PackagedProgram packagedProgram = null;
        ClusterClient<ApplicationId> clusterClient = null;
        YarnClusterDescriptor clusterDescriptor = null;
        try {
            clusterDescriptor = new YarnClusterDescriptor(
                    flinkConfig,
                    ConfigBeanClazz.yarnConfiguration,
                    ConfigBeanClazz.yarnClient,
                    YarnClientYarnClusterInformationRetriever.create(ConfigBeanClazz.yarnClient),
                    false);
            clusterDescriptor.setLocalJarPath(new Path(flinkDistJar));
//            clusterDescriptor.addShipFiles(Collections.singletonList(new File(userJarLib))); // 如需传递本地lib文件夹，开启此设置，则userJarLib需为本地路径

            ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(flinkConfig);
            logger.info("|<<specification>> " + clusterSpecification + "|");

            packagedProgram = PackagedProgram.newBuilder()
                    .setJarFile(new File(userJar))
                    .setEntryPointClassName(entryPointClassName)
                    .build();
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
                    packagedProgram,
                    flinkConfig,
                    1,
                    false);
            logger.info("|<<applicationId>> jobGraph getJobID: " + jobGraph.getJobID().toString() + "|");

            ClusterClientProvider<ApplicationId> clusterClientProvider = Common.invokeDeployInternal(
                    clusterDescriptor,
                    clusterSpecification,
                    applicationName,
                    YarnJobClusterEntrypoint.class.getName(),
                    jobGraph,
                    true
            );
//            ClusterClientProvider<ApplicationId> clusterClientProvider =
//                    clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true);

            clusterClient = clusterClientProvider.getClusterClient();
            ApplicationId applicationId = clusterClient.getClusterId();
            String webInterfaceURL = clusterClient.getWebInterfaceURL();
            logger.info("\n|-------------------------<<applicationId>>------------------------|\n" +
                        "|Flink Job Started: applicationId: " + applicationId + "  |\n" +
                        "|Flink Job Web Url: " + webInterfaceURL + "                    |\n" +
                        "|__________________________________________________________________|");
        } catch (ProgramInvocationException  | IllegalAccessException |
                 NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }finally {
            Common.close(packagedProgram);
            Common.close(clusterClient);
            Common.close(clusterDescriptor);
        }
    }

    @Override
    public void doCancel() {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(ConfigBeanClazz.flinkConfPath);
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.PER_JOB.getName());
        try (YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                flinkConfig,
                ConfigBeanClazz.yarnConfiguration,
                ConfigBeanClazz.yarnClient,
                YarnClientYarnClusterInformationRetriever.create(ConfigBeanClazz.yarnClient),
                false)) {
            ApplicationId applicationId = Common.getJobApplicationId(applicationName);
            clusterDescriptor.killCluster(applicationId);
            logger.info("\n| <<Cancel Flink job>>: " + applicationId.toString() + " success! |");
        }catch (FlinkException e){
            logger.error("\n| <<Cancel Flink job>>: error |\n", e);
        } catch (IOException | YarnException e) {
            throw new RuntimeException(e);
        }
    }

}
