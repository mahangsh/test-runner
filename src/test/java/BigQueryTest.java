import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class BigQueryTest {
    @Test
    public void BigQueryModuleTest() {
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config.yaml");
        Yaml yaml = new Yaml(new Constructor(new HashMap<String,Object>().getClass()));
        HashMap<String,Object> data = yaml.load(inputStream);
        HashMap<String,Object> modules = (HashMap<String, Object>) data.get("modules");
        assertEquals("[{projectId=bigquery, dataset=test_data, table=test_data, schema=[{column=City, type=STRING}, {column=State, type=STRING}, {column=Zip, type=STRING}, {column=Phone, type=STRING}], record=Austin, TX, 73301, 800-555-1234, foo@example.com, serviceAccount=/Users/edlira/serviceaccount.json}, {projectId=bigquery2, dataset=test_data, table=test_data, schema=[{column=City, type=STRING}, {column=State, type=STRING}, {column=Zip, type=STRING}, {column=Phone, type=STRING}], record=Austin, TX, 73301, 800-555-1234, foo@example.com, serviceAccount=/Users/edlira/serviceaccount.json}]",modules.get("bigquery").toString());
    }
}
