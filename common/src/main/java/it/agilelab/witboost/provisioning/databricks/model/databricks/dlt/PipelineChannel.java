package it.agilelab.witboost.provisioning.databricks.model.databricks.dlt;

/**
 * Enum representing different types of SKUs (Stock Keeping Units).
 */
public enum PipelineChannel {
    PREVIEW("preview"),
    CURRENT("current");

    private final String value;

    PipelineChannel(String value) {
        this.value = value;
    }

    public String getValue() {
        return value.toLowerCase();
    }
}
