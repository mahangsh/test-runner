package modules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import common.GoogleModule;
import common.ModuleName;
import util.YAMLConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

@ModuleName("spanner")
public class Spanner extends GoogleModule {
    private String instanceId;
    private String databaseId;
    private String configId;

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    public String getConfigId() { return configId; }

    public void setConfigId(String configId) { this.configId = configId; }

    @Override
    public void run() throws IOException, InterruptedException {
        validate();
        put();
        get();
    }

    @Override
    public void validate() throws IOException {
        try {
            SpannerOptions spannerService  = getSpannerService();
            if (spannerService == null) {
                System.out.println("Failed to connect to resource. Invalid credentials.");
                System.exit(0);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public SpannerOptions getSpannerService() throws Exception {
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(getServiceAccount())) {
            credentials = GoogleCredentials.fromStream(serviceAccountStream);
            return SpannerOptions.newBuilder().setCredentials(credentials).build();
        } catch (IOException e) {
            throw new Exception("Invalid credentials.");
        }
    }
    @Override
    public void put() throws IOException, InterruptedException {
        FileInputStream fileInputStream = new FileInputStream(getServiceAccount());
        GoogleCredentials serviceAccountCredentials = GoogleCredentials.fromStream(fileInputStream);
        SpannerOptions options = SpannerOptions.newBuilder().setCredentials(serviceAccountCredentials).build();
        try (com.google.cloud.spanner.Spanner spanner = options.getService()) {
            DatabaseId db = DatabaseId.of(options.getProjectId(), "test-instance", "sakila");

            String clientProject = spanner.getOptions().getProjectId();
            if (!db.getInstanceId().getProject().equals(clientProject)) {
                System.err.println(
                        "Invalid project specified. Project in the database id should match the"
                                + "project name set in the environment variable GOOGLE_CLOUD_PROJECT. Expected: "
                                + clientProject);
            }

            DatabaseClient dbClient = spanner.getDatabaseClient(db);
            DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
            createTableWithDatatypes(dbAdminClient,db);
        }

        setTotalPutOperations(1);
    }

    public void createTableWithDatatypes(DatabaseAdminClient dbAdminClient, DatabaseId id) {
        OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
                dbAdminClient.updateDatabaseDdl(
                        id.getInstanceId().getInstance(),
                        id.getDatabase(),
                        Arrays.asList(
                                "CREATE TABLE Singers ("
                                        + "  SingerId   INT64 NOT NULL,"
                                        + "  FirstName  STRING(1024),"
                                        + "  LastName   STRING(1024),"
                                        + "  SingerInfo BYTES(MAX)"
                                        + ") PRIMARY KEY (SingerId)"),
                        null);
        try {
            op.get();
            System.out.println("Created Singers table in database: [" + id + "]");
        } catch (ExecutionException e) {
            throw (SpannerException) e.getCause();
        } catch (InterruptedException e) {
            throw SpannerExceptionFactory.propagateInterrupt(e);
        }
    }

    public void createInstance() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(getServiceAccount());
        GoogleCredentials serviceAccountCredentials = GoogleCredentials.fromStream(fileInputStream);
        com.google.cloud.spanner.Spanner spanner = SpannerOptions.newBuilder().setCredentials(serviceAccountCredentials)
                .setProjectId(getProjectId()).build().getService();
        InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();

        int nodeCount = 2;
        String displayName = "Descriptive name";

        InstanceInfo instanceInfo =
                InstanceInfo.newBuilder(InstanceId.of(getProjectId(), getInstanceId()))
                        .setInstanceConfigId(InstanceConfigId.of(getProjectId(), getConfigId()))
                        .setNodeCount(nodeCount)
                        .setDisplayName(displayName)
                        .build();
        OperationFuture<Instance, CreateInstanceMetadata> operation =
                instanceAdminClient.createInstance(instanceInfo);
        try {
            Instance instance = operation.get();
            System.out.printf("Instance %s was successfully created%n", instance.getId());
        } catch (ExecutionException e) {
            System.out.printf(
                    "Error: Creating instance %s failed with error message %s%n",
                    instanceInfo.getId(), e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Error: Waiting for createInstance operation to finish was interrupted");
        }
    }

    @Override
    public void get() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(getServiceAccount());
        GoogleCredentials serviceAccountCredentials = GoogleCredentials.fromStream(fileInputStream);
        SpannerOptions options = SpannerOptions.newBuilder().setCredentials(serviceAccountCredentials).build();
        try (com.google.cloud.spanner.Spanner spanner = options.getService()) {
            DatabaseId db = DatabaseId.of(options.getProjectId(),"test-instance","sakila");

            String clientProject = spanner.getOptions().getProjectId();
            if (!db.getInstanceId().getProject().equals(clientProject)) {
                System.err.println(
                        "Invalid project specified. Project in the database id should match the"
                                + "project name set in the environment variable GOOGLE_CLOUD_PROJECT. Expected: "
                                + clientProject);
            }

            DatabaseClient dbClient = spanner.getDatabaseClient(db);
            read(dbClient);
        }
        setTotalReadOperations(1);
    }

    public void read(DatabaseClient dbClient) throws IOException {
        try (ResultSet resultSet =
                     dbClient
                             .singleUse()
                             .read(
                                     "Singers",
                                     KeySet.all(),
                                     Arrays.asList("SingerId","FirstName"))) {
            while (resultSet.next()) {
                System.out.printf(
                        "%d %s\n", resultSet.getLong(0), resultSet.getString(2));
            }
        }
    }

    @Override
    public void check() {
        System.out.printf("Check: total %s records written vs total %s records read. Check %s.", getTotalPutOperations(), getTotalReadOperations(), (getTotalPutOperations()==getTotalReadOperations())?"passed":"failed \n");
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        YAMLConfig yamlConfig = mapper.readValue(new File("src/main/resources/config.yaml"), YAMLConfig.class);

        Spanner spanner =new Spanner();
        spanner.setProjectId(yamlConfig.getProjectId());
        spanner.setServiceAccount(yamlConfig.getServiceAccount());
        HashMap<String,Object> modules = yamlConfig.getModules();
        LinkedHashMap<String, Object> map = ((ArrayList<LinkedHashMap<String, Object>>) modules.get("spanner")).get(0);

        spanner.setConfigId((String)map.get("configId"));
        spanner.setInstanceId((String)map.get("instanceId"));

        spanner.run();
    }
}
