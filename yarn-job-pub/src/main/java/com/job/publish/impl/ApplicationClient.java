package com.job.publish.impl;

import com.job.config.ConfigBeanClazz;
import com.job.publish.YarnClientAware;
import com.job.handler.Common;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;

/**
 * @author lcy
 */
public class ApplicationClient implements YarnClientAware {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationClient.class);
    private final String flinkDistJar;
    private final String userJar;
    private final String userJarLib;
    private final String applicationName;
    private final String entryPointClassName;

    /**
     * @author lcy
     * @description yarn application模式提交任务
     * @param flinkDistJar flinkDistJar hdfs路径
     * @param userJar hdfs jar路径
     * @param userJarLib hdfs lib文件夹路径
     * @param applicationName flink作业名称
     * @param entryPointClassName 主类名
     */
    public ApplicationClient(String flinkDistJar, String userJar, String userJarLib, String applicationName, String entryPointClassName) {
        this.flinkDistJar = flinkDistJar;
        this.userJar = userJar;
        this.userJarLib = userJarLib;
        this.applicationName = applicationName;
        this.entryPointClassName = entryPointClassName;
    }

    @Override
    public void doSubmit() {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(ConfigBeanClazz.flinkConfPath);
        flinkConfig
                .set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName())
                .set(PipelineOptions.JARS, Collections.singletonList(userJar))
                .set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(userJarLib)) // 可以将flinkDistJar放在指定的lib/路径下
                .set(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJar)
                .set(YarnConfigOptions.APPLICATION_NAME, applicationName)
                // 可选参数配置
                .set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES))
                .set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));
        logger.info("|Effective submit configuration: " + flinkConfig + "|");

        DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
        ClusterClientFactory<ApplicationId> clientFactory = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);
        ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(flinkConfig);
        logger.info("|<<specification>> " + clusterSpecification + "|");

        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(new String[]{"-c", entryPointClassName}, entryPointClassName);

        YarnClusterDescriptor clusterDescriptor = null;
        ClusterClient<ApplicationId> clusterClient = null;
        try {
            clusterDescriptor = new YarnClusterDescriptor(
                    flinkConfig,
                    ConfigBeanClazz.yarnConfiguration,
                    ConfigBeanClazz.yarnClient,
                    YarnClientYarnClusterInformationRetriever.create(ConfigBeanClazz.yarnClient),
                    false);

            ClusterClientProvider<ApplicationId> clusterClientProvider = clusterDescriptor.deployApplicationCluster(
                    clusterSpecification,
                    applicationConfiguration);

            clusterClient = clusterClientProvider.getClusterClient();
            ApplicationId applicationId = clusterClient.getClusterId();
            String webInterfaceURL = clusterClient.getWebInterfaceURL();
            logger.info("\n|-------------------------<<applicationId>>------------------------|\n" +
                        "|Flink Job Started: applicationId: " + applicationId + "  |\n" +
                        "|Flink Job Web Url: " + webInterfaceURL + "                    |\n" +
                        "|__________________________________________________________________|");
        } catch (ClusterDeploymentException e) {
            throw new RuntimeException(e);
        } finally {
            Common.close(clusterClient);
            Common.close(clusterDescriptor);
        }
    }

    @Override
    public void doCancel() {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(ConfigBeanClazz.flinkConfPath);
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
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
