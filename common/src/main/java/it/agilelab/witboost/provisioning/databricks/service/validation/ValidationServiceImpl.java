package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.Component;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.DescriptorKind;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.parser.Parser;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ValidationServiceImpl implements ValidationService {

    private final String OUTPUTPORT_KIND = "outputport";
    private final String WORKLOAD_KIND = "workload";
    private static final Logger logger = LoggerFactory.getLogger(ValidationServiceImpl.class);
    private final Map<String, Class<? extends Specific>> kindToSpecificClass =
            Map.of(WORKLOAD_KIND, DatabricksWorkloadSpecific.class);

    @Override
    public Either<FailedOperation, ProvisionRequest<? extends Specific>> validate(
            ProvisioningRequest provisioningRequest) {

        logger.info("Starting Descriptor validation");
        logger.info("Checking Descriptor Kind equals COMPONENT_DESCRIPTOR");

        if (!DescriptorKind.COMPONENT_DESCRIPTOR.equals(provisioningRequest.getDescriptorKind())) {
            String errorMessage = String.format(
                    "The descriptorKind field is not valid. Expected: '%s', Actual: '%s'",
                    DescriptorKind.COMPONENT_DESCRIPTOR, provisioningRequest.getDescriptorKind());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        logger.info("Parsing Descriptor");
        var eitherDescriptor = Parser.parseDescriptor(provisioningRequest.getDescriptor());
        if (eitherDescriptor.isLeft()) return left(eitherDescriptor.getLeft());
        var descriptor = eitherDescriptor.get();

        var componentId = descriptor.getComponentIdToProvision();

        logger.info("Checking component to provision {} is in the descriptor", componentId);
        var optionalComponentToProvision = descriptor.getDataProduct().getComponentToProvision(componentId);

        if (optionalComponentToProvision.isEmpty()) {
            String errorMessage = String.format("Component with ID %s not found in the Descriptor", componentId);
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        var componentToProvisionAsJson = optionalComponentToProvision.get();

        logger.info("Getting component kind for component to provision {}", componentId);
        var optionalComponentKindToProvision = descriptor.getDataProduct().getComponentKindToProvision(componentId);
        if (optionalComponentKindToProvision.isEmpty()) {
            String errorMessage = String.format("Component Kind not found for the component with ID %s", componentId);
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        var componentKindToProvision = optionalComponentKindToProvision.get();
        Component<? extends Specific> componentToProvision;
        switch (componentKindToProvision) {
            case WORKLOAD_KIND:
                var workloadClass = kindToSpecificClass.get(WORKLOAD_KIND);
                logger.info("Parsing Workload Component");
                var eitherWorkloadToProvision = Parser.parseComponent(componentToProvisionAsJson, workloadClass);
                if (eitherWorkloadToProvision.isLeft()) return left(eitherWorkloadToProvision.getLeft());
                componentToProvision = eitherWorkloadToProvision.get();
                var workloadValidation = WorkloadValidation.validate(componentToProvision);
                if (workloadValidation.isLeft()) return left(workloadValidation.getLeft());
                break;
            default:
                String errorMessage = String.format(
                        "The kind '%s' of the component to provision is not supported by this Specific Provisioner",
                        componentKindToProvision);
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        return right(new ProvisionRequest<>(
                descriptor.getDataProduct(), componentToProvision, provisioningRequest.getRemoveData()));
    }
}
