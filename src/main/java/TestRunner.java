import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import common.ModuleName;
import interfaces.ModuleInterface;
import org.reflections.Reflections;
import util.YAMLConfig;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestRunner {
    private static String yamlConfig = "src/main/resources/config.yaml";
    //Define the implemented modules here
    final static Map<String, String> map = new HashMap<String, String>();

    private static void loadModules() {
        final Reflections reflections = new Reflections("modules");
        Set<Class<?>> moduleFound = reflections.getTypesAnnotatedWith(ModuleName.class,true);
        for (Class<?> aClass : moduleFound) {
            ModuleName moduleName = aClass.getAnnotation(ModuleName.class);
            if(moduleName != null) {
                map.put(moduleName.value(), aClass.getCanonicalName());
            }
        }
    }

    public static Class<?> classByName(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            System.out.printf("Could not find class module %s.\n", className);
        }
        return null;
    }
    private static CollectionType initModule(String moduleName) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        for (String className : map.values()) {
            Class aClass = classByName(className);
            if (aClass.getName().equals(map.get(moduleName))) {
                System.out.println(moduleName);
                System.out.println(map.get(moduleName));
                return mapper.getTypeFactory().constructCollectionType(List.class,aClass);
            }
        }
        System.out.printf("Module %s not found. This section will be skipped.\n", moduleName);
        return null;
    }

    public static void main(String[] args) throws IOException {
        // load modules in modules package
        loadModules();
        //REMOVE:  This is here just for testing.
        String configInput = yamlConfig;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        YAMLConfig config = mapper.readValue(new File(configInput), YAMLConfig.class);
        //if the config file is supplied to the CMD then use that
        if (args.length > 0 && args[0] != null){
             config = mapper.readValue(new File(args[0]), YAMLConfig.class);
        };
        config.getModules().forEach((key, value)->{
            List<ModuleInterface> moduleInterfaces = null;
            try {
                CollectionType module = initModule(key);
                if(module == null){
                    return;
                }
                moduleInterfaces = mapper.convertValue(value, module);
                moduleInterfaces.forEach(moduleInterface -> {
                    try {
                        moduleInterface.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

