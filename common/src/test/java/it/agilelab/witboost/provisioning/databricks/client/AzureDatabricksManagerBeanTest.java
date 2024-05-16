package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import it.agilelab.witboost.provisioning.databricks.bean.AzureDatabricksManagerBean;
import org.junit.jupiter.api.Test;

public class AzureDatabricksManagerBeanTest {

    @Test
    public void testGetObject() {

        AzureDatabricksManager mockedManager = mock(AzureDatabricksManager.class);
        AzureDatabricksManagerBean managerBean = new AzureDatabricksManagerBean();
        managerBean.setAzureDatabricksManager(mockedManager);
        assertSame(mockedManager, managerBean.getObject());
    }

    //    @Test
    //    void testGetObject_CreatesNewInstanceWhenManagerIsNull() {
    //        AzureProfile mockProfile = mock(AzureProfile.class);
    //
    //        when(mockProfile.getEnvironment()).thenReturn(AzureEnvironment.AZURE);
    //        when(mockProfile.getSubscriptionId()).thenReturn("123");
    //
    //        String val = mockProfile.getSubscriptionId();
    //        System.out.println(val);
    //
    //        DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();
    //        DefaultAzureCredentialBuilder mockCredentialBuilder = mock(DefaultAzureCredentialBuilder.class);
    //        when(mockCredentialBuilder.authorityHost(any())).thenReturn(mockCredentialBuilder);
    //
    //        AzureDatabricksManager mockedManager = mock(AzureDatabricksManager.class);
    //
    //
    //
    //
    //        when(mockedManager.authenticate(credential, mockProfile)).thenReturn(mockedManager);
    //
    //        AzureDatabricksManagerBean managerBean = new AzureDatabricksManagerBean();
    //        managerBean.setAzureDatabricksManager(null);
    //
    //        AzureDatabricksManager result = managerBean.getObject();
    //
    //        verify(mockCredentialBuilder, times(1)).authorityHost(any());
    //        verify(mockCredentialBuilder, times(1)).build();
    //        assertNotNull(result);
    //        assertSame(mockedManager, result);
    //    }

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
