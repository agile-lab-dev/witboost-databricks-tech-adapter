package it.agilelab.witboost.provisioning.databricks.model.databricks;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabricksWorkspaceInfo {
    private String name;
    private String id;
    private String databricksHost;
    private String azureResourceId;
    private String azureResourceUrl;
    private ProvisioningState provisioningState;

    /**
     * Whether the tech adapter should manage the workspace (that is, upsert it, manage users/groups and permissions, etc.)
     */
    private boolean isManaged;

    public DatabricksWorkspaceInfo(
            String name,
            String id,
            String databricksHost,
            String azureResourceId,
            String azureResourceUrl,
            ProvisioningState provisioningState) {
        this.name = name;
        this.id = id;
        this.databricksHost = databricksHost;
        this.azureResourceId = azureResourceId;
        this.azureResourceUrl = azureResourceUrl;
        this.provisioningState = provisioningState;
        this.isManaged = true;
    }

    public DatabricksWorkspaceInfo(
            String databricksHost,
            String id,
            String azureResourceId,
            String azureResourceUrl,
            ProvisioningState provisioningState) {
        this.name = databricksHost;
        this.id = id;
        this.databricksHost = databricksHost;
        this.azureResourceId = azureResourceId;
        this.azureResourceUrl = azureResourceUrl;
        this.provisioningState = provisioningState;
        this.isManaged = false;
    }
}
