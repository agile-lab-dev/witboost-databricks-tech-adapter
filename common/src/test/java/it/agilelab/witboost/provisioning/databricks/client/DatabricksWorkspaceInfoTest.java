package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
public class DatabricksWorkspaceInfoTest {

    @Test
    public void testDatabricksWorkspaceInfo() {
        String name = "WorkspaceName";
        String id = "123";
        String url = "http://example.com";
        String resourceId = "abc";
        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(name, id, url, resourceId);

        assertEquals(name, databricksWorkspaceInfo.getName());
        assertEquals(id, databricksWorkspaceInfo.getId());
        assertEquals(url, databricksWorkspaceInfo.getUrl());
    }

    @Test
    public void testSetters() {
        DatabricksWorkspaceInfo workspaceInfo =
                new DatabricksWorkspaceInfo("workspace", "123", "https://example.com", "abc");
        String newName = "newWorkspace";
        String newId = "456";
        String newUrl = "https://newexample.com";

        workspaceInfo.setName(newName);
        workspaceInfo.setId(newId);
        workspaceInfo.setUrl(newUrl);

        assertEquals(newName, workspaceInfo.getName());
        assertEquals(newId, workspaceInfo.getId());
        assertEquals(newUrl, workspaceInfo.getUrl());
    }
}
