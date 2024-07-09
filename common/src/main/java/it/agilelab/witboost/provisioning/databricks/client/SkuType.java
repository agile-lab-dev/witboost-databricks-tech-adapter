package it.agilelab.witboost.provisioning.databricks.client;

/**
 * Enum representing different types of SKUs (Stock Keeping Units).
 */
public enum SkuType {
    TRIAL("trial"),
    STANDARD("standard"),
    PREMIUM("premium");

    private final String value;

    SkuType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value.toLowerCase();
    }
}
