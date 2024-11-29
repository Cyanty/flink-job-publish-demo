package com.job;

import com.job.domain.FlinkDeploymentMode;
import com.job.handler.YarnClientFactory;
import com.job.publish.YarnClientAware;

import java.io.IOException;

/**
 * @author lcy
 */
public class Main {
    public static void main(String[] args) throws IOException {
        String publishType = FlinkDeploymentMode.PER_JOB.getMode();
        String flinkDistJarPath = "/flink-job";
        String localUserJar = "\\flink-job-example\\target\\flink-job-example-1.0-SNAPSHOT.jar";
        String localUserJarLib = "\\flink-job-example\\target\\lib";
        String hdfsUserJar = "/flink-job";
        String hdfsUserJarLib = "/flink-job/lib";
        String applicationName = publishType + "-demo";
        String entryPointClassName = "com.example.SimpleDemoRun";

        YarnClientAware yarnClient = YarnClientFactory.getYarnClient(
                publishType, localUserJar, localUserJarLib, hdfsUserJar, hdfsUserJarLib,
                flinkDistJarPath, applicationName, entryPointClassName);

        yarnClient.doSubmit(); // 提交flink job

//        yarnClient.doCancel(); // 取消flink job

    }
}
