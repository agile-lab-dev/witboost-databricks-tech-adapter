package it.agilelab.witboost.provisioning.databricks.parser;

import com.fasterxml.jackson.databind.JsonNode;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.Descriptor;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.util.ResourceUtils;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class ParserTest {

    @Test
    void testParseWorkloadDescriptorOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_workload.yml");

        var actualResult = Parser.parseDescriptor(ymlDescriptor);

        Assertions.assertTrue(actualResult.isRight());
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
}
