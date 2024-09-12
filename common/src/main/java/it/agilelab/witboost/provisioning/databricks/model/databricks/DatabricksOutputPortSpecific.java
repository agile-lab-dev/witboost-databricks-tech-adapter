package it.agilelab.witboost.provisioning.databricks.model.databricks;

import it.agilelab.witboost.provisioning.databricks.model.Specific;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DatabricksOutputPortSpecific extends Specific {

    @NotBlank
    private String workspace;

    @NotBlank
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
