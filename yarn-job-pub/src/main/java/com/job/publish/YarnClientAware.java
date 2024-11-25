package com.job.publish;


public interface YarnClientAware {

    /**
     * 提交flink on yarn作业
     */
    void doSubmit();

    /**
     * 仅支持单作业名(作业名唯一)的任务取消
     */
    void doCancel();

}
