package it.agilelab.witboost.provisioning.databricks.model.databricks.object;

import com.databricks.sdk.service.catalog.SecurableType;

public interface DBObject {
    String fullyQualifiedName();

    SecurableType getSecurableType();
}
