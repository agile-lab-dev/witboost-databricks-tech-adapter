package it.agilelab.witboost.provisioning.databricks.controller;

import static org.mockito.Mockito.when;

import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.provision.ProvisionService;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@ExtendWith(MockitoExtension.class)
public class SpecificProvisionerControllerTest {

    @Mock
    private ProvisionService service;

    @InjectMocks
    private SpecificProvisionerController specificProvisionerController;

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
        String token = UUID.randomUUID().toString();
        when(service.provision(provisioningRequest)).thenReturn(token);
        var actualRes = specificProvisionerController.provision(provisioningRequest);
        Assertions.assertEquals(token, actualRes.getBody());
        Assertions.assertEquals(HttpStatusCode.valueOf(202), actualRes.getStatusCode());
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
        String token = UUID.randomUUID().toString();
        when(service.unprovision(provisioningRequest)).thenReturn(token);

        var actualRes = specificProvisionerController.unprovision(provisioningRequest);

        Assertions.assertEquals(token, actualRes.getBody());
        Assertions.assertEquals(HttpStatusCode.valueOf(202), actualRes.getStatusCode());
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

    @Test
    void testGetStatus() {
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        String token = UUID.randomUUID().toString();
        when(service.getStatus(token))
                .thenReturn(new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "this is the result"));

        ResponseEntity<ProvisioningStatus> actualRes = specificProvisionerController.getStatus(token);

        Assertions.assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        Assertions.assertEquals(
                ProvisioningStatus.StatusEnum.COMPLETED, actualRes.getBody().getStatus());
        Assertions.assertEquals("this is the result", actualRes.getBody().getResult());
    }
}
