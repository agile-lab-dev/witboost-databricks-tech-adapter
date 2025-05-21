package it.agilelab.witboost.provisioning.databricks.model.databricks.outputport;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksComponentSpecific;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabricksOutputPortSpecific extends DatabricksComponentSpecific {

    // Can be null when the workspace creation is disabled
    @Nullable
    private String metastore;

    @NotBlank
    private String catalogName;

    @NotBlank
    private String schemaName;

    @NotBlank
    private String tableName;

    @NotBlank
    private String sqlWarehouseName;

    @NotBlank
    private String workspaceOP;

    @NotBlank
    private String catalogNameOP;

    @NotBlank
    private String schemaNameOP;

    @NotBlank
    private String viewNameOP;
}
