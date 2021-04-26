import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class CloudMonitoringModuleTest {
    @Test
    public void CloudMonitoringModuleTest() {
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yaml");
        Yaml yaml = new Yaml(new Constructor(new HashMap<String,Object>().getClass()));
        HashMap<String,Object> data = yaml.load(inputStream);
        HashMap<String,Object> modules = (HashMap<String, Object>) data.get("modules");
        assertEquals("[{projectId=cloudmonitoring, metricType=test_metrics, serviceAccount=/Users/edlira/serviceaccount.json}]",modules.get("cloudmonitoring").toString());
    }
}
