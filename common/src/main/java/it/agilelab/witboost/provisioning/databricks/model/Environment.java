package it.agilelab.witboost.provisioning.databricks.model;

public enum Environment {
    PRODUCTION("production"),
    QA("qa"),
    DEVELOPMENT("development");

    private final String value;

    Environment(String value) {
        this.value = value;
    }

    public String getValue() {
        return value.toLowerCase();
    }
}
