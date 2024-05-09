package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import org.junit.jupiter.api.Test;

public class AzureDatabricksManagerBeanTest {

    @Test
    public void testGetObject() {

        AzureDatabricksManager mockedManager = mock(AzureDatabricksManager.class);
        AzureDatabricksManagerBeanBean managerBean = new AzureDatabricksManagerBeanBean();
        managerBean.setAzureDatabricksManager(mockedManager);
        assertSame(mockedManager, managerBean.getObject());
    }

    @Test
    public void testGetObjectType() {
        AzureDatabricksManagerBeanBean managerBean = new AzureDatabricksManagerBeanBean();
        assertEquals(AzureDatabricksManager.class, managerBean.getObjectType());
    }

    @Test
    public void testIsSingleton() {
        AzureDatabricksManagerBeanBean managerBean = new AzureDatabricksManagerBeanBean();
        assertTrue(managerBean.isSingleton());
    }

    @Test
    public void testSetAzureDatabricksManager() {
        AzureDatabricksManagerBeanBean managerBean = new AzureDatabricksManagerBeanBean();
        AzureDatabricksManager mockedManager = mock(AzureDatabricksManager.class);
        managerBean.setAzureDatabricksManager(mockedManager);
        assertSame(mockedManager, managerBean.getObject());
    }
}
