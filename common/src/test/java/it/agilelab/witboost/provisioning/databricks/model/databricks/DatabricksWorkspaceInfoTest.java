package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class DatabricksWorkspaceInfoTest {

    private DatabricksWorkspaceInfo workspaceInfo;

    @BeforeEach
    public void setUp() {
        workspaceInfo = new DatabricksWorkspaceInfo(
                "WorkspaceName", "WorkspaceId", "http://workspace.url", "ResourceId", "resourceUrl");
    }

    @Test
    public void testConstructorAndGetters() {
        assertThat(workspaceInfo.getName()).isEqualTo("WorkspaceName");
        assertThat(workspaceInfo.getId()).isEqualTo("WorkspaceId");
        assertThat(workspaceInfo.getDatabricksHost()).isEqualTo("http://workspace.url");
        assertThat(workspaceInfo.getAzureResourceId()).isEqualTo("ResourceId");
    }

    @Test
    public void testSetters() {
        workspaceInfo.setName("NewWorkspaceName");
        workspaceInfo.setId("NewWorkspaceId");
        workspaceInfo.setDatabricksHost("http://new.workspace.url");
        workspaceInfo.setAzureResourceId("NewResourceId");

        assertThat(workspaceInfo.getName()).isEqualTo("NewWorkspaceName");
        assertThat(workspaceInfo.getId()).isEqualTo("NewWorkspaceId");
        assertThat(workspaceInfo.getDatabricksHost()).isEqualTo("http://new.workspace.url");
        assertThat(workspaceInfo.getAzureResourceId()).isEqualTo("NewResourceId");
    }
}
