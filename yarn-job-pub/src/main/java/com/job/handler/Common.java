package com.job.handler;

import com.job.config.ConfigBeanClazz;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Array;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * @author lcy
 */
public class Common {
    private static final Logger logger = LoggerFactory.getLogger(Common.class);

    private static Method DeployInternalMethod() throws NoSuchMethodException {
        Class<?> clazz = YarnClusterDescriptor.class;
        Method deployInternal = clazz.getDeclaredMethod(
                "deployInternal",
                ClusterSpecification.class,
                String.class,
                String.class,
                JobGraph.class,
                boolean.class);
        deployInternal.setAccessible(true);
        return deployInternal;
    }

    public static ClusterClientProvider<ApplicationId> invokeDeployInternal(
            YarnClusterDescriptor clusterDescriptor,
            ClusterSpecification clusterSpecification,
            String applicationName,
            String yarnClusterEntrypoint,
            JobGraph jobGraph,
            boolean detached) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method deployInternalMethod = DeployInternalMethod();
        return (ClusterClientProvider<ApplicationId>) deployInternalMethod.invoke(
                clusterDescriptor,
                clusterSpecification,
                applicationName,
                yarnClusterEntrypoint,
                jobGraph,
                detached);
    }

    public static ApplicationId getJobApplicationId(String applicationName) throws IOException, YarnException {
        ArrayList<ApplicationId> applicationIds = new ArrayList<>();
        // 定义过滤器，只获取 RUNNING 和 ACCEPTED 状态的应用程序
        EnumSet<YarnApplicationState> appStates = EnumSet.of(YarnApplicationState.RUNNING, YarnApplicationState.ACCEPTED);
        List<ApplicationReport> applications = ConfigBeanClazz.yarnClient.getApplications(appStates);
        for (ApplicationReport appReport : applications) {
            if (appReport.getName().equals(applicationName)) {
                logger.info("|<<applicationId>> Get applicationId: " + appReport.getApplicationId() + " |");
                applicationIds.add(appReport.getApplicationId());
            }
        }
        if (applicationIds.size() == 1){
            return applicationIds.get(0);
        }

        throw new RuntimeException("|<<applicationId>> applicationName: " + applicationName + " Not found or multiple exist |");
    }

    public static void close(AutoCloseable closeable) {
        if (closeable != null){
            try {
                closeable.close();
            }catch (Exception e) {
                logger.error("Error closing resource:" + closeable, e);
            }
        }
    }

}
