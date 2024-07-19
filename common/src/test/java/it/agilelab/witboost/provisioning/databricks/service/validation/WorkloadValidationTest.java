package it.agilelab.witboost.provisioning.databricks.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.config.TemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.PipelineChannel;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.ProductEdition;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.*;
import java.util.List;
import java.util.Optional;
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
    private TemplatesConfig templatesConfig;

    @Autowired
    WorkloadValidation workloadValidation;

    @Test
    public void testValidateJobOk() {
        DatabricksJobWorkloadSpecific workloadSpecific = new DatabricksJobWorkloadSpecific();
        workloadSpecific.setWorkspace("");
        workloadSpecific.setJobName("testJob");
        workloadSpecific.setDescription("description");

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("master");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("https://git-url.com");
        gitSpecific.setGitRepoUrl("databricks/notebook");
        workloadSpecific.setGit(gitSpecific);

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
        specific.setNotificationsMails(List.of("email1@example.com", "email2@example.com"));
        specific.setNotificationsAlerts(List.of("alert1", "alert2"));
        specific.setChannel(PipelineChannel.CURRENT);
        specific.setCluster(cluster);

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(gitSpecific);

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
    public void testValidateWrongType() {
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_storage");
        String expectedDesc = "The component my_id_storage is not of type Workload";

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

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("master");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("https://git-url.com");
        gitSpecific.setGitRepoUrl("databricks/notebook");
        workloadSpecific.setGit(gitSpecific);

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

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("master");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("https://git-url.com");
        gitSpecific.setGitRepoUrl("databricks/notebook");
        workloadSpecific.setGit(gitSpecific);

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
                "Optional[wrong] (component my_workload_id) is not an accepted useCaseTemplateId for Databricks jobs or DLT pipelines.";

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
                "The specific section of the component my_workload_id is not of type DatabricksJobWorkloadSpecific";

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
                "The specific section of the component my_workload_id is not of type DatabricksDLTWorkloadSpecific";

        var actualRes = workloadValidation.validate(workload);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }
}
