package it.agilelab.witboost.provisioning.databricks.model.databricks.object;

import com.databricks.sdk.service.catalog.SecurableType;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class View implements DBObject {
    private String catalogName;
    private String schemaName;
    private String viewName;

    @Override
    public String fullyQualifiedName() {
        return catalogName + "." + schemaName + "." + viewName;
    }

    @Override
    public SecurableType getSecurableType() {
        return SecurableType.TABLE;
    }
}
