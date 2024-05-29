package it.agilelab.witboost.provisioning.databricks.model.databricks;

public enum GitProvider {
    GITLAB;

    public static GitProvider fromString(String gitProvider) {
        for (GitProvider type : GitProvider.values()) {
            if (type.name().equalsIgnoreCase(gitProvider)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown gitProvider: " + gitProvider);
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
