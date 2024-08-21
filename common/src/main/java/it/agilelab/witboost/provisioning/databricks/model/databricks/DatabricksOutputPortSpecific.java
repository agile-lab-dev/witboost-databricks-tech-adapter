package it.agilelab.witboost.provisioning.databricks.model.databricks;

import it.agilelab.witboost.provisioning.databricks.model.Specific;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DatabricksOutputPortSpecific extends Specific {

    @NotNull
    private String workspace;

    @NotNull
    private String catalogName;

    @NotNull
    private String schemaName;

    @NotNull
    private String tableName;

    @NotNull
    private String sqlWarehouseName;

    @NotNull
    private String workspaceOP;

    @NotNull
    private String catalogNameOP;

    @NotNull
    private String schemaNameOP;

    @NotNull
    private String viewNameOP;
}
