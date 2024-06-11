package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProvisionServiceTest {
    @Mock
    private ValidationService validationService;

    @Mock
    private WorkloadHandler workloadHandler;

    @InjectMocks
    private ProvisionServiceImpl provisionService;

    @Test
    public void testValidateOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<Specific>(null, null, false)));
        var expectedRes = new ValidationResult(true);

        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testValidateError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));
        var expectedRes = new ValidationResult(false).error(new ValidationError(List.of("error")));

        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testProvisionValidationError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.provision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testProvisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("unsupported");
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, workload, false)));
        String expectedDesc = "The kind 'unsupported' of the component is not supported by this Specific Provisioner";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.provision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testProvisionWorkspaceOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workloadHandler.provisionWorkload(provisionRequest)).thenReturn(right("workloadId"));

        var info = Map.of("path", "workloadId");
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), info).publicInfo(info));

        var actualRes = provisionService.provision(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testUnprovisionValidationError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.unprovision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUnprovisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> Workload = new Workload<>();
        Workload.setKind("unsupported");
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, Workload, false)));
        String expectedDesc = "The kind 'unsupported' of the component is not supported by this Specific Provisioner";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.unprovision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUnprovisionWorkloadOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> Workload = new Workload<>();
        Workload.setKind("workload");
        var provisionRequest = new ProvisionRequest<>(null, Workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workloadHandler.unprovisionWorkload(provisionRequest)).thenReturn(right(null));
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        var actualRes = provisionService.unprovision(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }
}
