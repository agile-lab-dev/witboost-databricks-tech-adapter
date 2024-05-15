package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import org.junit.jupiter.api.Test;

public class AzureDatabricksManagerBeanTest {

    @Test
    public void testGetObject() {

        AzureDatabricksManager mockedManager = mock(AzureDatabricksManager.class);
        AzureDatabricksManagerBean managerBean = new AzureDatabricksManagerBean();
        managerBean.setAzureDatabricksManager(mockedManager);
        assertSame(mockedManager, managerBean.getObject());
    }

    @Test
    public void testGetObjectType() {
        AzureDatabricksManagerBean managerBean = new AzureDatabricksManagerBean();
        assertEquals(AzureDatabricksManager.class, managerBean.getObjectType());
    }

    @Test
    public void testIsSingleton() {
        AzureDatabricksManagerBean managerBean = new AzureDatabricksManagerBean();
        assertTrue(managerBean.isSingleton());
    }

    @Test
    public void testSetAzureDatabricksManager() {
        AzureDatabricksManagerBean managerBean = new AzureDatabricksManagerBean();
        AzureDatabricksManager mockedManager = mock(AzureDatabricksManager.class);
        managerBean.setAzureDatabricksManager(mockedManager);
        assertSame(mockedManager, managerBean.getObject());
    }
}
