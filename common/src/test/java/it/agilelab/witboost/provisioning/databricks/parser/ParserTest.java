package it.agilelab.witboost.provisioning.databricks.parser;

import com.fasterxml.jackson.databind.JsonNode;
import it.agilelab.witboost.provisioning.databricks.model.Descriptor;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.util.ResourceUtils;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParserTest {

    @Test
    void testParseStorageDescriptorOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage.yml");

        var actualResult = Parser.parseDescriptor(ymlDescriptor);

        Assertions.assertTrue(actualResult.isRight());
    }

    @Test
    void testParseOutputPortDescriptorOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport.yml");

        var actualResult = Parser.parseDescriptor(ymlDescriptor);

        Assertions.assertTrue(actualResult.isRight());
    }

    @Test
    void testParseWorkloadDescriptorOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_workload.yml");

        var actualResult = Parser.parseDescriptor(ymlDescriptor);

        Assertions.assertTrue(actualResult.isRight());
    }

    @Test
    public void testParseStorageDescriptorFail() {
        String invalidDescriptor = "an_invalid_descriptor";
        String expectedDesc = "Failed to deserialize the Yaml Descriptor. Details: ";

        var actualRes = Parser.parseDescriptor(invalidDescriptor);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertTrue(p.description().startsWith(expectedDesc));
            Assertions.assertTrue(p.cause().isPresent());
        });
    }

    @Test
    public void testParseStorageComponentOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage.yml");
        var eitherDescriptor = Parser.parseDescriptor(ymlDescriptor);
        Assertions.assertTrue(eitherDescriptor.isRight());
        Descriptor descriptor = eitherDescriptor.get();
        String componentIdToProvision = "urn:dmb:cmp:healthcare:vaccinations:0:storage";
        var optionalComponent = descriptor.getDataProduct().getComponentToProvision(componentIdToProvision);
        Assertions.assertTrue(optionalComponent.isDefined());
        JsonNode component = optionalComponent.get();

        var actualRes = Parser.parseComponent(component, Specific.class);
        Assertions.assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseOutputPortComponentOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport.yml");
        var eitherDescriptor = Parser.parseDescriptor(ymlDescriptor);
        Assertions.assertTrue(eitherDescriptor.isRight());
        Descriptor descriptor = eitherDescriptor.get();
        String componentIdToProvision = "urn:dmb:cmp:healthcare:vaccinations:0:hdfs-output-port";
        var optionalComponent = descriptor.getDataProduct().getComponentToProvision(componentIdToProvision);
        Assertions.assertTrue(optionalComponent.isDefined());
        JsonNode component = optionalComponent.get();

        var actualRes = Parser.parseComponent(component, Specific.class);

        Assertions.assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseWorkloadComponentOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_workload.yml");
        var eitherDescriptor = Parser.parseDescriptor(ymlDescriptor);
        Assertions.assertTrue(eitherDescriptor.isRight());
        Descriptor descriptor = eitherDescriptor.get();
        String componentIdToProvision = "urn:dmb:cmp:healthcare:dbt-provisioner:0:dbt-transformation-workload";
        var optionalComponent = descriptor.getDataProduct().getComponentToProvision(componentIdToProvision);
        Assertions.assertTrue(optionalComponent.isDefined());
        JsonNode component = optionalComponent.get();

        var actualRes = Parser.parseComponent(component, Specific.class);

        Assertions.assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseStorageComponentFail() {
        JsonNode node = null;
        String expectedDesc = "Failed to deserialize the component. Details: ";

        var actualRes = Parser.parseComponent(node, Specific.class);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertTrue(p.description().startsWith(expectedDesc));
            Assertions.assertTrue(p.cause().isPresent());
        });
    }
}
