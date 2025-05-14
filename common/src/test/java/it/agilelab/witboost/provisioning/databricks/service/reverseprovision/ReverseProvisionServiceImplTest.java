package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.CatalogInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class ReverseProvisionServiceImplTest {

    @Autowired
    private ReverseProvisionServiceImpl reverseProvisionServiceImpl;

    @MockBean
    private OutputPortReverseProvisionHandler outputPortReverseProvisionHandler;

    @MockBean
    private WorkflowReverseProvisionHandler workflowReverseProvisionHandler;

    private CatalogInfo catalogInfo;

    @BeforeEach
    public void setUp() {
        catalogInfo = new CatalogInfo();
        CatalogInfo.Spec.Mesh mesh = new CatalogInfo.Spec.Mesh();
        mesh.setName("componentName");
        CatalogInfo.Spec spec = new CatalogInfo.Spec();
        spec.setMesh(mesh);
        catalogInfo.setSpec(spec);
    }

    @Test
    public void testReverseProvisionServiceImplOutputPortTemplateId_SUCCESS() {

        ReverseProvisioningRequest request =
                new ReverseProvisioningRequest("urn:dmb:utm:databricks-outputport-template:0.0.0", "qa");
        request.setCatalogInfo(catalogInfo);

        when(outputPortReverseProvisionHandler.reverseProvision(request))
                .thenReturn(new ReverseProvisioningStatus(ReverseProvisioningStatus.StatusEnum.COMPLETED, null));

        ReverseProvisioningStatus status = reverseProvisionServiceImpl.runReverseProvisioning(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, status.getStatus());
    }

    @Test
    public void testReverseProvisionServiceImplOutputPortTemplateId_FAILED_WrongUseCaseTemplateId() {

        ReverseProvisioningRequest request = new ReverseProvisioningRequest("urn:dmb:utm:wrong-template:0.0.0", "qa");
        request.setCatalogInfo(catalogInfo);

        ReverseProvisioningStatus status = reverseProvisionServiceImpl.runReverseProvisioning(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "The useCaseTemplateId 'urn:dmb:utm:wrong-template' of the component is not supported by this Tech Adapter",
                status.getLogs().get(0).getMessage());
    }
}
