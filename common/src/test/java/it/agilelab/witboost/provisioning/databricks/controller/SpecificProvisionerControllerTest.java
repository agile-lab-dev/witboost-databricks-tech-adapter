package it.agilelab.witboost.provisioning.databricks.controller;

import static org.mockito.Mockito.when;

import it.agilelab.witboost.provisioning.databricks.api.ApiServiceImpl;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import java.util.Collections;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@ExtendWith(MockitoExtension.class)
public class SpecificProvisionerControllerTest {

    @Mock
    private ApiServiceImpl service;

    @InjectMocks
    private SpecificProvisionerController specificProvisionerController;

    @Autowired
    public SpecificProvisionerControllerTest() {}

    @Test
    void testValidateOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        when(service.validate(provisioningRequest)).thenReturn(new ValidationResult(true));

        ResponseEntity<ValidationResult> actualRes = specificProvisionerController.validate(provisioningRequest);

        Assertions.assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        Assertions.assertTrue(Objects.requireNonNull(actualRes.getBody()).getValid());
    }

    @Test
    void testValidateHasError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        String expectedError = "Validation error";
        when(service.validate(provisioningRequest))
                .thenReturn(new ValidationResult(false)
                        .error(new ValidationError(Collections.singletonList(expectedError))));

        ResponseEntity<ValidationResult> actualRes = specificProvisionerController.validate(provisioningRequest);

        Assertions.assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        Assertions.assertFalse(Objects.requireNonNull(actualRes.getBody()).getValid());
        Assertions.assertEquals(1, actualRes.getBody().getError().getErrors().size());
        actualRes.getBody().getError().getErrors().forEach(p -> Assertions.assertEquals(expectedError, p));
    }

    @Test
    void testProvisionOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        when(service.provision(provisioningRequest))
                .thenReturn(new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, ""));

        ResponseEntity<ProvisioningStatus> actualRes = specificProvisionerController.provision(provisioningRequest);

        Assertions.assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        Assertions.assertEquals(
                ProvisioningStatus.StatusEnum.COMPLETED,
                Objects.requireNonNull(actualRes.getBody()).getStatus());
    }

    @Test
    void testProvisionHasError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(service.provision(provisioningRequest))
                .thenThrow(new SpecificProvisionerValidationException(failedOperation));

        var ex = Assertions.assertThrows(
                SpecificProvisionerValidationException.class,
                () -> specificProvisionerController.provision(provisioningRequest));
        Assertions.assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    void testUnprovisionOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        when(service.unprovision(provisioningRequest))
                .thenReturn(new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, ""));

        ResponseEntity<ProvisioningStatus> actualRes = specificProvisionerController.unprovision(provisioningRequest);

        Assertions.assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        Assertions.assertEquals(
                ProvisioningStatus.StatusEnum.COMPLETED,
                Objects.requireNonNull(actualRes.getBody()).getStatus());
    }

    @Test
    void testUnprovisionHasError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(service.unprovision(provisioningRequest))
                .thenThrow(new SpecificProvisionerValidationException(failedOperation));

        var ex = Assertions.assertThrows(
                SpecificProvisionerValidationException.class,
                () -> specificProvisionerController.unprovision(provisioningRequest));
        Assertions.assertEquals(failedOperation, ex.getFailedOperation());
    }
}
