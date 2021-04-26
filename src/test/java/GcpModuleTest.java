import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class GcpModuleTest {
    @Test
    public void GcpModuleTest() {
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yaml");
        Yaml yaml = new Yaml(new Constructor(new HashMap<String,Object>().getClass()));
        HashMap<String,Object> data = yaml.load(inputStream);
        HashMap<String,Object> modules = (HashMap<String, Object>) data.get("modules");
        assertEquals("[{projectId=gcpapi, description=GCP API, project_id=dpp_test_project, location=us_east_1a, service_account=/Users/edlira/serviceaccount.json}]",modules.get("gcp").toString());
    }
}
