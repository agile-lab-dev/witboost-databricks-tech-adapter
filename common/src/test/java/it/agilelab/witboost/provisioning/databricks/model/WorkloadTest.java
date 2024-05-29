package it.agilelab.witboost.provisioning.databricks.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class WorkloadTest {

    private Workload<String> workload1;

    @BeforeEach
    public void setUp() {
        workload1 = createWorkload();
    }

    private Workload<String> createWorkload() {
        Workload<String> workload = new Workload<>();
        workload.setVersion("1.0");
        workload.setInfrastructureTemplateId("template-1");
        workload.setUseCaseTemplateId(Optional.of("usecase-template-1"));
        workload.setDependsOn(Arrays.asList("dep-1", "dep-2"));
        workload.setPlatform(Optional.of("platform"));
        workload.setTechnology(Optional.of("technology"));
        workload.setWorkloadType(Optional.of("TypeA"));
        workload.setConnectionType(Optional.of("ConnectionA"));
        workload.setTags(new ArrayList<>());
        workload.setReadsFrom(Arrays.asList("source-1", "source-2"));

        return workload;
    }

    @Test
    public void testToString() {
        String expectedToString = "Workload(version=1.0, infrastructureTemplateId=template-1, "
                + "useCaseTemplateId=Optional[usecase-template-1], dependsOn=[dep-1, dep-2], "
                + "platform=Optional[platform], technology=Optional[technology], "
                + "workloadType=Optional[TypeA], connectionType=Optional[ConnectionA], "
                + "tags=[], readsFrom=[source-1, source-2])";

        assertEquals(expectedToString, workload1.toString());
    }

    @Test
    public void testJsonIgnoreProperties() {
        assertTrue(Workload.class.isAnnotationPresent(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class));
        com.fasterxml.jackson.annotation.JsonIgnoreProperties propertiesAnnotation =
                Workload.class.getAnnotation(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class);
        assertTrue(propertiesAnnotation.ignoreUnknown());
    }

    @Test
    public void testGetters() {
        assertEquals("1.0", workload1.getVersion());
        assertEquals("template-1", workload1.getInfrastructureTemplateId());
        assertEquals(Optional.of("usecase-template-1"), workload1.getUseCaseTemplateId());
        assertEquals(Arrays.asList("dep-1", "dep-2"), workload1.getDependsOn());
        assertEquals(Optional.of("platform"), workload1.getPlatform());
        assertEquals(Optional.of("technology"), workload1.getTechnology());
        assertEquals(Optional.of("TypeA"), workload1.getWorkloadType());
        assertEquals(Optional.of("ConnectionA"), workload1.getConnectionType());
        assertEquals(new ArrayList<>(), workload1.getTags());
        assertEquals(Arrays.asList("source-1", "source-2"), workload1.getReadsFrom());
    }
}
