package it.agilelab.witboost.provisioning.databricks.model.databricks;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DatabricksWorkspaceInfo {
    private String name;
    private String id;
    private String url;
    private String azureResourceId;

    public DatabricksWorkspaceInfo(String name, String id, String url, String azureResourceId) {
        this.name = name;
        this.id = id;
        this.url = url;
        this.azureResourceId = azureResourceId;
    }
}
