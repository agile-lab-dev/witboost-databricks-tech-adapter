package it.agilelab.witboost.provisioning.databricks.service.validation;

import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.openapi.model.DescriptorKind;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.util.ResourceUtils;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ValidationServiceTest {

    private final ValidationService service = new ValidationServiceImpl();

    @Test
    public void testValidateOutputPortFailureToImplement() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        var failedOperation = new FailedOperation(Collections.singletonList(
                new Problem("Implement the validation for output port based on your requirements!")));

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertEquals(failedOperation, actualResult.getLeft());
    }

    @Test
    public void testValidateStorageFailureToImplement() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        var failedOperation = new FailedOperation(Collections.singletonList(
                new Problem("Implement the validation for storage area based on your requirements!")));

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertEquals(failedOperation, actualResult.getLeft());
    }

    @Test
    public void testValidateWorkloadFailureToImplement() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        var failedOperation = new FailedOperation(Collections.singletonList(
                new Problem("Implement the validation for workload based on your requirements!")));

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertEquals(failedOperation, actualResult.getLeft());
    }

    @Test
    public void testValidateWrongDescriptorKind() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.DATAPRODUCT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The descriptorKind field is not valid. Expected: 'COMPONENT_DESCRIPTOR', Actual: 'DATAPRODUCT_DESCRIPTOR'";

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertTrue(actualResult.isLeft());
        Assertions.assertEquals(1, actualResult.getLeft().problems().size());
        actualResult.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongDescriptorFormat() {
        String ymlDescriptor = "an_invalid_descriptor";
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Failed to deserialize the Yaml Descriptor. Details: ";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertTrue(p.description().startsWith(expectedDesc));
            Assertions.assertTrue(p.cause().isPresent());
        });
    }

    @Test
    public void testValidateMissingComponentIdToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/pr_descriptor_storage_missing_componentIdToProvision.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Component with ID null not found in the Descriptor";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateMissingComponentToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage_missing_component.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage not found in the Descriptor";

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertTrue(actualResult.isLeft());
        Assertions.assertEquals(1, actualResult.getLeft().problems().size());
        actualResult.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateMissingComponentKindToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage_missing_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component Kind not found for the component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongComponentKindToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage_wrong_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The kind 'wrong' of the component to provision is not supported by this Specific Provisioner";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }
}
