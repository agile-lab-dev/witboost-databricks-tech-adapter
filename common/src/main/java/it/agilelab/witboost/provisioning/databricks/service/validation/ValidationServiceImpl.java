package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.core.ApiClient;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.ApiClientConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.config.WorkloadTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.Component;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
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

    private Map<String, List<Class<? extends Specific>>> kindToSpecificClasses = new HashMap<>();
    private Function<ApiClientConfig.ApiClientConfigParams, ApiClient> apiClientFactory;
    private WorkspaceHandler workspaceHandler;
    private MiscConfig miscConfig;
    private WorkloadTemplatesConfig workloadTemplatesConfig;

    public ValidationServiceImpl(
            Function<ApiClientConfig.ApiClientConfigParams, ApiClient> apiClientFactory,
            MiscConfig miscConfig,
            WorkspaceHandler workspaceHandler,
            WorkloadTemplatesConfig workloadTemplatesConfig) {
        this.apiClientFactory = apiClientFactory;
        this.miscConfig = miscConfig;
        this.workspaceHandler = workspaceHandler;
        this.workloadTemplatesConfig = workloadTemplatesConfig;

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
                logger.info(String.format("Parsing Workload Component [id %s]", componentId));
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

        String useCaseTemplateId =
                getUseCaseTemplateId(String.valueOf(componentToProvisionAsJson.get("useCaseTemplateId")));

        Either<FailedOperation, ? extends Component<? extends Specific>> eitherWorkloadToProvision;

        if (workloadTemplatesConfig.getJob().contains(useCaseTemplateId)) {
            eitherWorkloadToProvision =
                    Parser.parseComponent(componentToProvisionAsJson, DatabricksJobWorkloadSpecific.class);
        } else if (workloadTemplatesConfig.getWorkflow().contains(useCaseTemplateId)) {
            eitherWorkloadToProvision =
                    Parser.parseComponent(componentToProvisionAsJson, DatabricksWorkflowWorkloadSpecific.class);
        } else if (workloadTemplatesConfig.getDlt().contains(useCaseTemplateId)) {
            eitherWorkloadToProvision =
                    Parser.parseComponent(componentToProvisionAsJson, DatabricksDLTWorkloadSpecific.class);
        } else {
            String errorMessage = String.format(
                    "An error occurred while parsing the component %s. Please try again and if the error persists contact the platform team. Details: unsupported use case template id.",
                    componentToProvisionAsJson.get("name"));
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        if (eitherWorkloadToProvision.isRight()) return right(eitherWorkloadToProvision.get());

        return left(eitherWorkloadToProvision.getLeft());
    }

    private static String getUseCaseTemplateId(String useCaseTemplateIdFull) {
        String[] parts = useCaseTemplateIdFull.split(":");
        String[] useCaseTemplateIdParts = Arrays.copyOfRange(parts, 0, parts.length - 1);
        String useCaseTemplateId = String.join(":", useCaseTemplateIdParts);
        String cleanUseCaseTemplateId = useCaseTemplateId.replace("\"", "").trim();
        return cleanUseCaseTemplateId;
    }
}
