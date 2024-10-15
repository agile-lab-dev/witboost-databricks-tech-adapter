package it.agilelab.witboost.provisioning.databricks.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.JobSettings;
import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.config.WorkloadTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.*;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.*;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@EnableConfigurationProperties
public class WorkloadValidationTest {

    @Autowired
    private WorkloadTemplatesConfig workloadTemplatesConfig;

    @Autowired
    WorkloadValidation workloadValidation;

    @Test
    public void testValidateJobOk() {
        DatabricksJobWorkloadSpecific workloadSpecific = new DatabricksJobWorkloadSpecific();
        workloadSpecific.setWorkspace("");
        workloadSpecific.setJobName("testJob");
        workloadSpecific.setDescription("description");

        JobGitSpecific jobGitSpecific = new JobGitSpecific();
        jobGitSpecific.setGitReference("master");
        jobGitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("https://git-url.com");
        jobGitSpecific.setGitRepoUrl("databricks/notebook");
        workloadSpecific.setGit(jobGitSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("* * * * *");
        schedulingSpecific.setJavaTimezoneId("UTC");
        workloadSpecific.setScheduling(schedulingSpecific);

        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("13.3.x-scala2.12");
        jobClusterSpecific.setNodeTypeId("Standard_DS3_v2");
        jobClusterSpecific.setNumWorkers(2L);
        workloadSpecific.setCluster(jobClusterSpecific);

        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setUseCaseTemplateId(Optional.of("urn:dmb:utm:databricks-workload-job-template:0.0.0"));
        workload.setSpecific(workloadSpecific);

        var actualRes = workloadValidation.validate(workload);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateDLTOk() {
        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");

        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("workspace");
        specific.setPipelineName("pipelineName");
        specific.setProductEdition(ProductEdition.CORE);
        specific.setContinuous(true);
        specific.setNotebooks(List.of("notebook1", "notebook2"));
        specific.setFiles(List.of("file1", "file2"));
        specific.setMetastore("metastore");
        specific.setCatalog("catalog");
        specific.setTarget("target");
        specific.setPhoton(true);
        List notifications = new ArrayList();
        notifications.add(new PipelineNotification("email@email.com", Collections.singletonList("on-update-test")));
        specific.setChannel(PipelineChannel.CURRENT);
        specific.setCluster(cluster);

        GitSpecific dltGitSpecific = new GitSpecific();
        dltGitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(dltGitSpecific);

        Workload<DatabricksDLTWorkloadSpecific> workload = new Workload<>();
        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setUseCaseTemplateId(Optional.of("urn:dmb:utm:databricks-workload-dlt-template:0.0.0"));
        workload.setSpecific(specific);

        var actualRes = workloadValidation.validate(workload);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWorkflowOk() {
        DatabricksWorkflowWorkloadSpecific workloadSpecific = new DatabricksWorkflowWorkloadSpecific();
        String workspaceName = "testWorkspace";
        DatabricksWorkflowWorkloadSpecific databricksWorkflowWorkloadSpecific =
                new DatabricksWorkflowWorkloadSpecific();
        databricksWorkflowWorkloadSpecific.setWorkspace(workspaceName);
        databricksWorkflowWorkloadSpecific.setRepoPath("this/is/a/repo");

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        databricksWorkflowWorkloadSpecific.setGit(gitSpecific);

        Job workflow = new Job();
        workflow.setSettings(new JobSettings().setName("workflowName"));
        databricksWorkflowWorkloadSpecific.setWorkflow(workflow);

        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workload");
        workload.setUseCaseTemplateId(Optional.of("urn:dmb:utm:databricks-workload-workflow-template:0.0.0"));
        workload.setSpecific(databricksWorkflowWorkloadSpecific);

        var actualRes = workloadValidation.validate(workload);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongType() {
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("op id");
        outputPort.setName("op name");
        String expectedDesc = "The component op name is not of type Workload";

        var actualRes = workloadValidation.validate(outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidate_NullUseCaseTemplateId() {
        DatabricksJobWorkloadSpecific workloadSpecific = new DatabricksJobWorkloadSpecific();
        workloadSpecific.setWorkspace("");
        workloadSpecific.setJobName("testJob");
        workloadSpecific.setDescription("description");

        JobGitSpecific jobGitSpecific = new JobGitSpecific();
        jobGitSpecific.setGitReference("master");
        jobGitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("https://git-url.com");
        jobGitSpecific.setGitRepoUrl("databricks/notebook");
        workloadSpecific.setGit(jobGitSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("* * * * *");
        schedulingSpecific.setJavaTimezoneId("UTC");
        workloadSpecific.setScheduling(schedulingSpecific);

        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("13.3.x-scala2.12");
        jobClusterSpecific.setNodeTypeId("Standard_DS3_v2");
        jobClusterSpecific.setNumWorkers(2L);
        workloadSpecific.setCluster(jobClusterSpecific);

        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setUseCaseTemplateId(Optional.empty());
        workload.setSpecific(workloadSpecific);

        var actualRes = workloadValidation.validate(workload);
        String expectedError = "useCaseTemplateId is mandatory to detect the workload kind (job or dlt pipeline)";

        assert actualRes.isLeft();
        assert actualRes.getLeft().problems().get(0).description().contains(expectedError);
    }

    @Test
    public void testValidate_WrongUseCaseTemplateId() {
        DatabricksJobWorkloadSpecific workloadSpecific = new DatabricksJobWorkloadSpecific();
        workloadSpecific.setWorkspace("");
        workloadSpecific.setJobName("testJob");
        workloadSpecific.setDescription("description");

        JobGitSpecific jobGitSpecific = new JobGitSpecific();
        jobGitSpecific.setGitReference("master");
        jobGitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("https://git-url.com");
        jobGitSpecific.setGitRepoUrl("databricks/notebook");
        workloadSpecific.setGit(jobGitSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("* * * * *");
        schedulingSpecific.setJavaTimezoneId("UTC");
        workloadSpecific.setScheduling(schedulingSpecific);

        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("13.3.x-scala2.12");
        jobClusterSpecific.setNodeTypeId("Standard_DS3_v2");
        jobClusterSpecific.setNumWorkers(2L);
        workloadSpecific.setCluster(jobClusterSpecific);

        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setUseCaseTemplateId(Optional.of("wrong"));
        workload.setSpecific(workloadSpecific);

        var actualRes = workloadValidation.validate(workload);
        String expectedError =
                "Optional[wrong] (component workload name) is not an accepted useCaseTemplateId for Databricks jobs or DLT pipelines.";

        assert actualRes.isLeft();
        assert actualRes.getLeft().problems().get(0).description().contains(expectedError);
    }

    @Test
    public void testValidateWrongJobSpecific() {
        Specific specific = new Specific();
        Workload<Specific> workload = new Workload<>();

        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setSpecific(specific);
        workload.setUseCaseTemplateId(Optional.of("urn:dmb:utm:databricks-workload-job-template:0.0.0"));

        String expectedDesc =
                "The specific section of the component workload name is not of type DatabricksJobWorkloadSpecific";

        var actualRes = workloadValidation.validate(workload);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongDltSpecific() {
        Specific specific = new Specific();
        Workload<Specific> workload = new Workload<>();

        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setSpecific(specific);
        workload.setUseCaseTemplateId(Optional.of("urn:dmb:utm:databricks-workload-dlt-template:0.0.0"));

        String expectedDesc =
                "The specific section of the component workload name is not of type DatabricksDLTWorkloadSpecific";

        var actualRes = workloadValidation.validate(workload);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongWorkflowSpecific() {
        Workload<Specific> workload = new Workload<>();
        workload.setName("workload");

        workload.setUseCaseTemplateId(Optional.of("urn:dmb:utm:databricks-workload-workflow-template:0.0.0"));

        var actualRes = workloadValidation.validate(workload);

        assert actualRes.isLeft();
        assert actualRes
                .getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "The specific section of the component workload is not of type DatabricksWorkflowWorkloadSpecific");
    }
}
