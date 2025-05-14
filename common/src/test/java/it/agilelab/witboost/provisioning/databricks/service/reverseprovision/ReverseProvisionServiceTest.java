package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.CatalogInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class ReverseProvisionServiceTest {
    @MockBean
    private WorkflowReverseProvisionHandler workflowReverseProvisionHandler;

    @MockBean
    private OutputPortReverseProvisionHandler outputPortReverseProvisionHandler;

    @Autowired
    private ReverseProvisionServiceImpl reverseProvisionServiceImpl;

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
    public void testReverseProvisionServiceImplWorkloadTemplateId_SUCCESS() {

        ReverseProvisioningRequest request =
                new ReverseProvisioningRequest("urn:dmb:utm:databricks-workload-workflow-template:0.0.0", "qa");
        request.setCatalogInfo(catalogInfo);

        when(workflowReverseProvisionHandler.reverseProvision(request))
                .thenReturn(right(new LinkedHashMap<Object, Object>()));

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

    @Test
    public void testReverseProvisionServiceImplWorkflowTemplateId_FAILED() {
        ReverseProvisioningRequest request =
                new ReverseProvisioningRequest("urn:dmb:utm:databricks-workload-workflow-template:0.0.0", "qa");
        request.setCatalogInfo(catalogInfo);

        String errorMessage = "Provisioning error xyz";
        Problem problem = new Problem(errorMessage);
        FailedOperation failedOperation = new FailedOperation(Collections.singletonList(problem));
        when(workflowReverseProvisionHandler.reverseProvision(request)).thenReturn(Either.left(failedOperation));

        ReverseProvisioningStatus status = reverseProvisionServiceImpl.runReverseProvisioning(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());

        assert (status.getLogs().get(0).getMessage().contains(errorMessage));
    }
}
