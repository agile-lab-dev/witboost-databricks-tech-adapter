package it.agilelab.witboost.provisioning.databricks.model.databricks.job;

public enum GitReferenceType {
    BRANCH,
    TAG;

    public static GitReferenceType fromString(String referenceType) {
        for (GitReferenceType type : GitReferenceType.values()) {
            if (type.name().equalsIgnoreCase(referenceType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown reference type: " + referenceType);
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
