package it.agilelab.witboost.provisioning.databricks.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.*;
import org.junit.jupiter.api.Test;

public class WorkloadValidationTest {

    @Test
    public void testValidateOk() {
        DatabricksWorkloadSpecific workloadSpecific = new DatabricksWorkloadSpecific();
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

        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setClusterSparkVersion("13.3.x-scala2.12");
        clusterSpecific.setNodeTypeId("Standard_DS3_v2");
        clusterSpecific.setNumWorkers(2L);
        workloadSpecific.setCluster(clusterSpecific);

        Workload<DatabricksWorkloadSpecific> workload = new Workload<>();
        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setSpecific(workloadSpecific);

        var actualRes = WorkloadValidation.validate(workload);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongType() {
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_storage");
        String expectedDesc = "The component my_id_storage is not of type Workload";

        var actualRes = WorkloadValidation.validate(outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongSpecific() {
        Specific specific = new Specific();
        Workload<Specific> workload = new Workload<>();

        workload.setId("my_workload_id");
        workload.setName("workload name");
        workload.setDescription("workload desc");
        workload.setKind("workload");
        workload.setSpecific(specific);

        String expectedDesc =
                "The specific section of the component my_workload_id is not of type DatabricksWorkloadSpecific";

        var actualRes = WorkloadValidation.validate(workload);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }
}
