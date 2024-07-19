package it.agilelab.witboost.provisioning.databricks.model.databricks.dlt;

/**
 * Enum representing different types of SKUs (Stock Keeping Units).
 */
public enum ProductEdition {
    ADVANCED("advanced"),
    CORE("core"),
    PRO("pro");

    private final String value;

    ProductEdition(String value) {
        this.value = value;
    }

    public String getValue() {
        return value.toLowerCase();
    }
}
