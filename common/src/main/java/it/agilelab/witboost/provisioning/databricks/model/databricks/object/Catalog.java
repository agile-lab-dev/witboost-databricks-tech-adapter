package it.agilelab.witboost.provisioning.databricks.model.databricks.object;

import com.databricks.sdk.service.catalog.SecurableType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Catalog implements DBObject {
    private String catalogName;

    @Override
    public String fullyQualifiedName() {
        return catalogName;
    }

    @Override
    public SecurableType getSecurableType() {
        return SecurableType.CATALOG;
    }
}
