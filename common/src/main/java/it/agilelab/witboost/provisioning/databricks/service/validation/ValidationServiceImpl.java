package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.core.ApiClient;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.Component;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.DescriptorKind;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.parser.Parser;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ValidationServiceImpl implements ValidationService {

    private final String OUTPUTPORT_KIND = "outputport";
    private final String WORKLOAD_KIND = "workload";
    private static final Logger logger = LoggerFactory.getLogger(ValidationServiceImpl.class);

    private final Map<String, List<Class<? extends Specific>>> kindToSpecificClasses = new HashMap<>();
    private final Function<ApiClientConfigParams, ApiClient> apiClientFactory;
    private final WorkspaceHandler workspaceHandler;
    private final MiscConfig miscConfig;

    public ValidationServiceImpl(
            Function<ApiClientConfigParams, ApiClient> apiClientFactory,
            MiscConfig miscConfig,
            WorkspaceHandler workspaceHandler) {
        this.apiClientFactory = apiClientFactory;
        this.miscConfig = miscConfig;
        this.workspaceHandler = workspaceHandler;

        List<Class<? extends Specific>> classes = new ArrayList<>();
        classes.add(DatabricksJobWorkloadSpecific.class);
        classes.add(DatabricksDLTWorkloadSpecific.class);

        kindToSpecificClasses.put(WORKLOAD_KIND, classes);
        kindToSpecificClasses.put(OUTPUTPORT_KIND, List.of(DatabricksOutputPortSpecific.class));
    }

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

        JsonNode componentToProvisionAsJson = optionalComponentToProvision.get();

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
                logger.info("Parsing Workload Component");
                var eitherWorkloadToProvision = parseComponent(componentToProvisionAsJson);
                if (eitherWorkloadToProvision.isLeft()) return left(eitherWorkloadToProvision.getLeft());
                componentToProvision = eitherWorkloadToProvision.get();
                var workloadValidation = WorkloadValidation.validate(componentToProvision);
                if (workloadValidation.isLeft()) return left(workloadValidation.getLeft());
                break;
            case OUTPUTPORT_KIND:
                String environment = descriptor.getDataProduct().getEnvironment();

                var outputPortClass = kindToSpecificClasses.get(OUTPUTPORT_KIND).get(0); // List of 1 element
                var eitherOutputPortToValidate = Parser.parseComponent(componentToProvisionAsJson, outputPortClass);
                if (eitherOutputPortToValidate.isLeft()) return left(eitherOutputPortToValidate.getLeft());
                componentToProvision = eitherOutputPortToValidate.get();

                logger.info(
                        "Parsing Output Port Component {} in {} environment",
                        componentToProvision.getName(),
                        environment);

                var outputPortValidator = new OutputPortValidation(miscConfig, workspaceHandler, apiClientFactory);
                var outputPortValidation = outputPortValidator.validate(
                        (OutputPort<DatabricksOutputPortSpecific>) componentToProvision, environment);
                if (outputPortValidation.isLeft()) return left(outputPortValidation.getLeft());

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

    private Either<FailedOperation, Component<? extends Specific>> parseComponent(JsonNode componentToProvisionAsJson) {
        Component<? extends Specific> componentToProvision;
        var workloadClasses = kindToSpecificClasses.get(WORKLOAD_KIND);

        List<Problem> problems = new ArrayList<>();

        for (Class<? extends Specific> workloadClass : workloadClasses) {
            var eitherWorkloadToProvision = Parser.parseComponent(componentToProvisionAsJson, workloadClass);
            if (eitherWorkloadToProvision.isRight()) {
                componentToProvision = eitherWorkloadToProvision.get();
                return right(componentToProvision);
            }

            var problemsList = eitherWorkloadToProvision.getLeft().problems();
            if (problemsList != null) for (Problem prob : problemsList) problems.add(prob);
        }

        if (!problems.isEmpty()) {
            logger.error(
                    "An error occurred while parsing the component. Please try again and if the error persists contact the platform team. Errors:");
            for (Problem problem : problems) {
                logger.error("Problem: {}", problem.description());
            }
        } else {
            logger.error("An error occurred while parsing the component but no specific problems were found.");
        }

        return left(new FailedOperation(problems));
    }
}
