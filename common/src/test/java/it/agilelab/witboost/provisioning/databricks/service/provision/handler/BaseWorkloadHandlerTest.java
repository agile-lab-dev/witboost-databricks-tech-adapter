package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * This test class contains only exception handling tests for the {@code BaseWorkloadHandler}.
 * The successful behavior and functionality are tested in the classes that extend this one.
 *
 * These tests focus on ensuring that the system reacts correctly in failure scenarios.
 */
@SpringBootTest
@ExtendWith(MockitoExtension.class)
@EnableConfigurationProperties
public class BaseWorkloadHandlerTest {

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    AzureAuthConfig azureAuthConfig;

    @Autowired
    DatabricksPermissionsConfig databricksPermissionsConfig;

    @Autowired
    GitCredentialsConfig gitCredentialsConfig;

    @Mock
    WorkspaceClient workspaceClient;

    @Mock
    WorkspaceAPI workspaceAPI;

    @MockBean
    private AccountClient accountClient;

    @MockBean
    private AzureResourceManager azureResourceManager;

    @Mock
    WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    private BaseWorkloadHandler baseWorkloadHandler;
    private DataProduct dataProduct;
    private DatabricksDLTWorkloadSpecific databricksDLTWorkloadSpecific;
    private DatabricksWorkflowWorkloadSpecific databricksWorkflowWorkloadSpecific;
    private DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);
    private String workspaceName = "testWorkspace";

    @BeforeEach
    public void setUp() {

        baseWorkloadHandler = new BaseWorkloadHandler(
                azureAuthConfig,
                gitCredentialsConfig,
                databricksPermissionsConfig,
                accountClient,
                workspaceLevelManagerFactory);
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
    }

    @Test
    public void createRepositoryWithPermissions_ExceptionJob() {

        Workload workload = new Workload<>();
        workload.setName("wrong_workload");
        DatabricksJobWorkloadSpecific specific = new DatabricksJobWorkloadSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific gitSpecific = new DatabricksJobWorkloadSpecific.JobGitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(gitSpecific);
        workload.setSpecific(specific);

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, Void> result = baseWorkloadHandler.createRepositoryWithPermissions(
                provisionRequest,
                workspaceClient,
                new DatabricksWorkspaceInfo("workspace", null, null, null, null, null),
                "owner",
                "devGroup");

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while creating repository https://github.com/repo.git in workspace for component wrong_workload.");
    }

    @Test
    public void createRepositoryWithPermissions_ExceptionDLT() {

        Workload workload = new Workload<>();
        workload.setName("wrong_workload");
        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        DatabricksWorkflowWorkloadSpecific.GitSpecific gitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(gitSpecific);
        workload.setSpecific(specific);

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, Void> result = baseWorkloadHandler.createRepositoryWithPermissions(
                provisionRequest,
                workspaceClient,
                new DatabricksWorkspaceInfo("workspace", null, null, null, null, null),
                "owner",
                "devGroup");

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while creating repository https://github.com/repo.git in workspace for component wrong_workload.");
    }

    @Test
    public void createRepositoryWithPermissions_ExceptionWorkflow() {

        Workload workload = new Workload<>();
        workload.setName("wrong_workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        DatabricksWorkflowWorkloadSpecific.GitSpecific gitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(gitSpecific);
        workload.setSpecific(specific);

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, Void> result = baseWorkloadHandler.createRepositoryWithPermissions(
                provisionRequest,
                workspaceClient,
                new DatabricksWorkspaceInfo("workspace", null, null, null, null, null),
                "owner",
                "devGroup");

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while creating repository https://github.com/repo.git in workspace for component wrong_workload.");
    }

    @Test
    public void mapUser_Exception() {

        Workload workload = new Workload<>();
        workload.setName("wrong_workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        DatabricksWorkflowWorkloadSpecific.GitSpecific gitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(gitSpecific);
        workload.setSpecific(specific);

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);

        Either<FailedOperation, Map<String, String>> result = baseWorkloadHandler.mapPrincipals(provisionRequest);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while mapping dpOwner and devGroup for component wrong_workload.");
    }
}
