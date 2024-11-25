import com.job.config.ConfigBeanClazz;
import com.job.handler.Common;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author lcy
 */

public class RetrieveTest {
    @Test
    public void retrieveTest() throws Exception {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(ConfigBeanClazz.flinkConfPath);
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                flinkConfig,
                ConfigBeanClazz.yarnConfiguration,
                ConfigBeanClazz.yarnClient,
                YarnClientYarnClusterInformationRetriever.create(ConfigBeanClazz.yarnClient),
                false);
        ClusterClientProvider<ApplicationId> clientProvider = clusterDescriptor.retrieve(
                ApplicationId.fromString("application_1722825415835_5680"));
        ClusterClient<ApplicationId> clusterClient = clientProvider.getClusterClient();
        CompletableFuture<Collection<JobStatusMessage>> collectionCompletableFuture = clusterClient.listJobs();
        Collection<JobStatusMessage> jobStatusMessages = collectionCompletableFuture.join();
        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            System.out.println(jobStatusMessage.getJobId());
            System.out.println(jobStatusMessage.getJobName());
            System.out.println(jobStatusMessage.getJobState());
            System.out.println(jobStatusMessage.getStartTime());
        }
        clusterDescriptor.close();
    }


}
