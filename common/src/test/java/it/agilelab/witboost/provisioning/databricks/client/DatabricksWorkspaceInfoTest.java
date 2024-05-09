package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksWorkspaceInfoTest {

    @Test
    public void testDatabricksWorkspaceInfo() {
        String name = "WorkspaceName";
        String id = "123";
        String url = "http://example.com";
        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(name, id, url);

        assertEquals(name, databricksWorkspaceInfo.getName());
        assertEquals(id, databricksWorkspaceInfo.getId());
        assertEquals(url, databricksWorkspaceInfo.getUrl());
    }

    @Test
    public void testSetters() {
        DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo("workspace", "123", "https://example.com");
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

    @Test
    public void testEquality() {

        DatabricksWorkspaceInfo workspaceInfo1 = new DatabricksWorkspaceInfo("workspace", "123", "https://example.com");
        DatabricksWorkspaceInfo workspaceInfo2 = new DatabricksWorkspaceInfo("workspace", "123", "https://example.com");
        DatabricksWorkspaceInfo workspaceInfo3 =
                new DatabricksWorkspaceInfo("anotherWorkspace", "456", "https://anotherexample.com");

        assertEquals(workspaceInfo1, workspaceInfo2); // Two workspace objects with same attributes should be equal
        assertNotEquals(
                workspaceInfo1, workspaceInfo3); // Two workspace objects with different attributes should not be equal
    }

    @Test
    public void testToString() {

        DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo("workspace", "123", "https://example.com");
        assertEquals(
                "DatabricksWorkspaceInfo{name='workspace', id='123', url='https://example.com'}",
                workspaceInfo.toString());
    }
}
