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
public class OutputPortTest {

    private OutputPort<String> outputPort1;
    private OutputPort<String> outputPort2;
    private OutputPort<String> outputPort3;

    @BeforeEach
    public void setUp() {
        outputPort1 = createOutputPort();
        outputPort2 = createOutputPort();
        outputPort3 = new OutputPort<>();

        // Setting different values for outputPort3
        outputPort3.setVersion("2.0");
        outputPort3.setOutputPortType("TypeB");
    }

    private OutputPort<String> createOutputPort() {
        OutputPort<String> outputPort = new OutputPort<>();
        outputPort.setVersion("1.0");
        outputPort.setInfrastructureTemplateId("template-1");
        outputPort.setUseCaseTemplateId(Optional.of("usecase-template-1"));
        outputPort.setDependsOn(Arrays.asList("dep-1", "dep-2"));
        outputPort.setPlatform(Optional.of("platform"));
        outputPort.setTechnology(Optional.of("technology"));
        outputPort.setOutputPortType("TypeA");
        outputPort.setCreationDate(Optional.of("2023-01-01"));
        outputPort.setStartDate(Optional.of("2023-01-02"));
        outputPort.setRetentionTime(Optional.of("P3Y6M4D"));
        outputPort.setProcessDescription(Optional.of("Description"));
        outputPort.setDataContract(null); // Set to null for simplicity
        outputPort.setDataSharingAgreement(null); // Set to null for simplicity
        outputPort.setTags(new ArrayList<>()); // Empty list of tags
        outputPort.setSampleData(Optional.empty()); // Empty Optional sample data
        outputPort.setSemanticLinking(Optional.empty()); // Empty Optional semantic linking

        return outputPort;
    }

    @Test
    public void testToString() {
        String expectedToString = "OutputPort(version=1.0, infrastructureTemplateId=template-1, "
                + "useCaseTemplateId=Optional[usecase-template-1], dependsOn=[dep-1, dep-2], "
                + "platform=Optional[platform], technology=Optional[technology], outputPortType=TypeA, "
                + "creationDate=Optional[2023-01-01], startDate=Optional[2023-01-02], retentionTime=Optional[P3Y6M4D], "
                + "processDescription=Optional[Description], dataContract=null, dataSharingAgreement=null, tags=[], "
                + "sampleData=Optional.empty, semanticLinking=Optional.empty)";

        assertEquals(expectedToString, outputPort1.toString());
    }

    @Test
    public void testJsonIgnoreProperties() {
        assertTrue(OutputPort.class.isAnnotationPresent(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class));
        com.fasterxml.jackson.annotation.JsonIgnoreProperties propertiesAnnotation =
                OutputPort.class.getAnnotation(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class);
        assertTrue(propertiesAnnotation.ignoreUnknown());
    }

    @Test
    public void testGetters() {
        assertEquals("1.0", outputPort1.getVersion());
        assertEquals("template-1", outputPort1.getInfrastructureTemplateId());
        assertEquals(Optional.of("usecase-template-1"), outputPort1.getUseCaseTemplateId());
        assertEquals(Arrays.asList("dep-1", "dep-2"), outputPort1.getDependsOn());
        assertEquals(Optional.of("platform"), outputPort1.getPlatform());
        assertEquals(Optional.of("technology"), outputPort1.getTechnology());
        assertEquals("TypeA", outputPort1.getOutputPortType());
        assertEquals(Optional.of("2023-01-01"), outputPort1.getCreationDate());
        assertEquals(Optional.of("2023-01-02"), outputPort1.getStartDate());
        assertEquals(Optional.of("P3Y6M4D"), outputPort1.getRetentionTime());
        assertEquals(Optional.of("Description"), outputPort1.getProcessDescription());
        assertEquals(new ArrayList<>(), outputPort1.getTags());
        assertEquals(Optional.empty(), outputPort1.getSampleData());
        assertEquals(Optional.empty(), outputPort1.getSemanticLinking());
    }
}
