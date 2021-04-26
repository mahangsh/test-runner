import util.YAMLConfig;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.*;

public class ModuleTest {

    @Test
    public void ModuleTest() {
        Yaml yaml = new Yaml(new Constructor(YAMLConfig.class));
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yaml");
        YAMLConfig YAMLConfig = yaml.load(inputStream);
        assertNotNull(YAMLConfig);
        assertEquals("AdaptiveScale", YAMLConfig.getName());
        assertEquals("adaptivescale-178418", YAMLConfig.getProjectId());
        assertEquals("/Users/edlira/serviceaccount.json", YAMLConfig.getServiceAccount());
        assertEquals("/Users/edlira/log/dpp_test_harness.log", YAMLConfig.getLog());
        assertNotNull(YAMLConfig.getModules());
        assertEquals(10, YAMLConfig.getModules().size());
        assertTrue(YAMLConfig.getModules().containsKey("gcp"));
        assertTrue(YAMLConfig.getModules().containsKey("bigquery"));
        assertTrue(YAMLConfig.getModules().containsKey("gcs"));
        assertTrue(YAMLConfig.getModules().containsKey("pubsub"));
        assertTrue(YAMLConfig.getModules().containsKey("dataproc"));
        assertTrue(YAMLConfig.getModules().containsKey("bigtable"));
        assertTrue(YAMLConfig.getModules().containsKey("spanner"));
        assertTrue(YAMLConfig.getModules().containsKey("DICOM"));
        assertTrue(YAMLConfig.getModules().containsKey("videointelligence"));
        assertTrue(YAMLConfig.getModules().containsKey("cloudmonitoring"));
    }

    @Test
    public void LoadCorrectMap() {
        Yaml yaml = new Yaml();
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yaml");
        Map<String, Object> obj = yaml.load(inputStream);
        assertEquals("AdaptiveScale", obj.get("name"));
        assertEquals("adaptivescale-178418", obj.get("projectId"));
        assertEquals("/Users/edlira/serviceaccount.json", obj.get("serviceAccount"));
        assertEquals("/Users/edlira/log/dpp_test_harness.log", obj.get("log"));
    }

    @Test
    public void GenerateCorrectYAML() {
        YAMLConfig YAMLConfig = new YAMLConfig();
        YAMLConfig.setName("test");
        YAMLConfig.setProjectId("adaptivescale");
        YAMLConfig.setServiceAccount("/path/to/service");
        YAMLConfig.setLog("/path/to/log");
        Yaml yaml = new Yaml();
        String expectedYaml = "{log: /path/to/log, modules: null, name: test, projectId: adaptivescale, serviceAccount: /path/to/service}\n";
        assertEquals(expectedYaml, yaml.dumpAs(YAMLConfig, Tag.MAP, null));
    }
}
