package it.agilelab.witboost.provisioning.databricks.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class DataProductTest {

    private DataProduct dataProduct1;

    @BeforeEach
    public void setUp() {
        dataProduct1 = createDataProduct();
    }

    private DataProduct createDataProduct() {
        DataProduct dataProduct = new DataProduct();
        dataProduct.setId("product-1");
        dataProduct.setName("Product 1");
        dataProduct.setFullyQualifiedName(Optional.of("org.example.product1"));
        dataProduct.setDescription("Description of Product 1");
        dataProduct.setKind("KindA");
        dataProduct.setDomain("Data Domain");
        dataProduct.setVersion("1.0");
        dataProduct.setEnvironment("Development");
        dataProduct.setDataProductOwner("Owner1");
        dataProduct.setDataProductOwnerDisplayName("Owner One");
        dataProduct.setEmail(Optional.of("owner1@example.com"));
        dataProduct.setDevGroup("Dev Group");
        dataProduct.setOwnerGroup("Owner Group");
        dataProduct.setInformationSLA(Optional.of("SLA information"));
        dataProduct.setStatus(Optional.of("Active"));
        dataProduct.setMaturity(Optional.of("Beta"));
        dataProduct.setBilling(Optional.empty()); // No billing information
        dataProduct.setTags(new ArrayList<>()); // Empty list of tags
        dataProduct.setComponents(new ArrayList<>()); // Empty list of components

        return dataProduct;
    }

    @Test
    public void testGetters() {
        assertEquals("product-1", dataProduct1.getId());
        assertEquals("Product 1", dataProduct1.getName());
        assertEquals(Optional.of("org.example.product1"), dataProduct1.getFullyQualifiedName());
        assertEquals("Description of Product 1", dataProduct1.getDescription());
        assertEquals("KindA", dataProduct1.getKind());
        assertEquals("Data Domain", dataProduct1.getDomain());
        assertEquals("1.0", dataProduct1.getVersion());
        assertEquals("Development", dataProduct1.getEnvironment());
        assertEquals("Owner1", dataProduct1.getDataProductOwner());
        assertEquals("Owner One", dataProduct1.getDataProductOwnerDisplayName());
        assertEquals(Optional.of("owner1@example.com"), dataProduct1.getEmail());
        assertEquals("Dev Group", dataProduct1.getDevGroup());
        assertEquals("Owner Group", dataProduct1.getOwnerGroup());
        assertEquals(Optional.of("SLA information"), dataProduct1.getInformationSLA());
        assertEquals(Optional.of("Active"), dataProduct1.getStatus());
        assertEquals(Optional.of("Beta"), dataProduct1.getMaturity());
        assertEquals(Optional.empty(), dataProduct1.getBilling());
        assertEquals(new ArrayList<>(), dataProduct1.getTags());
        assertEquals(null, dataProduct1.getSpecific()); // Assuming specific is null in this case
        assertEquals(new ArrayList<>(), dataProduct1.getComponents()); // Empty list of components
    }

    @Test
    public void testToString() {
        String expectedToString =
                "DataProduct(id=product-1, name=Product 1, fullyQualifiedName=Optional[org.example.product1], "
                        + "description=Description of Product 1, kind=KindA, domain=Data Domain, version=1.0, environment=Development, "
                        + "dataProductOwner=Owner1, dataProductOwnerDisplayName=Owner One, email=Optional[owner1@example.com], "
                        + "devGroup=Dev Group, ownerGroup=Owner Group, informationSLA=Optional[SLA information], status=Optional[Active], "
                        + "maturity=Optional[Beta], billing=Optional.empty, tags=[], specific=null, components=[])";

        assertEquals(expectedToString, dataProduct1.toString());
    }

    @Test
    public void testJsonIgnoreProperties() {
        assertTrue(DataProduct.class.isAnnotationPresent(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class));
        com.fasterxml.jackson.annotation.JsonIgnoreProperties propertiesAnnotation =
                DataProduct.class.getAnnotation(com.fasterxml.jackson.annotation.JsonIgnoreProperties.class);
        assertTrue(propertiesAnnotation.ignoreUnknown());
    }
}
