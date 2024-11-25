package com.job.domain;

public enum FlinkDeploymentMode {
    PER_JOB("yarn-per-job"),
    SESSION("yarn-session"),
    APPLICATION("yarn-application");

    private final String mode;

    FlinkDeploymentMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return this.mode;
    }
}

