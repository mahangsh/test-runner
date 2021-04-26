package modules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import common.GoogleModule;
import common.ModuleName;
import entities.BigQuerySchemaField;
import util.YAMLConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

@ModuleName("bigquery")
public class BigQuery extends GoogleModule {
    private String dataset;
    private String table;
    private List<BigQuerySchemaField> schema;
    private String record;
    private long totalInserts = 0;
    private long totalReads = 0;
    public String module = "bigquery";


    public BigQuery() throws FileNotFoundException, IOException {
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<BigQuerySchemaField> getSchema() {
        return schema;
    }

    public void setSchema(List<BigQuerySchemaField> schema) {
        this.schema = schema;
    }

    public String getRecord() {
        return record;
    }

    public void setRecord(String record) {
        this.record = record;
    }


    @Override
    public String toString() {
        return new StringJoiner(", ", BigQuery.class.getSimpleName() + "[", "]")
                .add("projectId=" + getProjectId())
                .add("dataset=" + getDataset())
                .add("table=" + getTable())
                .add("schema=" + getSchema())
                .add("record=" + getRecord())
                .toString();
    }

    @Override
    public void run() throws Exception {
        System.out.println(getServiceAccount());
        validate();
        put();
        get();
        check();
        deleteDataset();
    }

    @Override
    public void validate() throws IOException {
        try {
            com.google.cloud.bigquery.BigQuery bigQueryService = getBigQueryService();
            if (bigQueryService == null) {
                System.out.println("Failed to connect to resource. Invalid credentials.");
                System.exit(0);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public com.google.cloud.bigquery.BigQuery getBigQueryService() throws Exception {
        File credentialsPath = new File(getServiceAccount());
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
          return BigQueryOptions.newBuilder()
                            .setCredentials(credentials)
                            .setProjectId(getProjectId())
                            .build()
                            .getService();
        } catch (IOException e) {
            throw new Exception("Invalid credentials.");
        }
    }

    //Create a dataset in order to be able to create a table in the subsequent step
    //This function calls the dataset. Insert API method.
    public void createDataset() throws Exception{
        try {
            com.google.cloud.bigquery.BigQuery bigquery = getBigQueryService();
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(getDataset()).build();

            Dataset newDataset = bigquery.create(datasetInfo);
            String newDatasetName = newDataset.getDatasetId().getDataset();
            System.out.println(newDatasetName + " created successfully");
        } catch (BigQueryException e) {
            System.out.println("Dataset was not created. \n" + e.toString());
        }
    }
    @Override
    public void put() throws Exception {
        com.google.cloud.bigquery.BigQuery bigQueryService = getBigQueryService();
        createDataset();
        ArrayList<Field> parsedSchemaFields=new ArrayList();
        for (BigQuerySchemaField schemaField : getSchema()) {
            String name = schemaField.getColumn();
            String typeString = schemaField.getType();
            StandardSQLTypeName standardSQLTypeName = StandardSQLTypeName.valueOf(typeString);
            parsedSchemaFields.add(Field.of(name, standardSQLTypeName));
        }

        try {
            TableId tableId = TableId.of(dataset,table);
            TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of(parsedSchemaFields));
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            bigQueryService.create(tableInfo);
            System.out.println("Table created successfully");
        } catch (BigQueryException e) {
            System.out.println("Table was not created. \n" + e.toString());
        }

        setTotalPutOperations(tableInsertRows(dataset,table,parsedSchemaFields));
    }

    public long tableInsertRows(
            String datasetName, String tableName, ArrayList<Field> parsedSchemaFields) throws Exception {
        long totalInserts = 0;
        String[] recordValues = record.split(",");
        Map<String, Object> rowContent = new HashMap<>();
        for (int i = 0; i < recordValues.length; i++) {
            String recordValue = recordValues[i];
            String name = parsedSchemaFields.get(i).getName();
            rowContent.put(name, recordValue);
        }
        com.google.cloud.bigquery.BigQuery bigQueryService = getBigQueryService();
        try {
            TableId tableId = TableId.of(datasetName, tableName);

            InsertAllResponse response =
                    bigQueryService.insertAll(
                            InsertAllRequest.newBuilder(tableId)
                                    .addRow(rowContent)
                                    .build());

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    System.out.println("Response error: \n" + entry.getValue());
                }
            }
            System.out.println("Rows successfully inserted into table");
            totalInserts = 1;
        } catch (BigQueryException e) {
            System.out.println("Insert operation not performed \n" + e.toString());
        }
        return totalInserts;
    }

    @Override
    public void get() throws Exception {
        setTotalReadOperations(getRows(dataset, table));
    }


    public long getRows(
            String datasetName, String tableName) throws Exception {
        long totalRowsRetrieved = 0;
        com.google.cloud.bigquery.BigQuery bigQuery = getBigQueryService();
        try {
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(
                            "Select * from "+datasetName+"."+tableName)
                            .setUseLegacySql(false)
                            .build();
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            queryJob = queryJob.waitFor();
            if (queryJob == null) {
                throw new RuntimeException("Job no longer exists");
            } else if (queryJob.getStatus().getError() != null) {
                throw new RuntimeException(queryJob.getStatus().getError().toString());
            }

            TableResult result = queryJob.getQueryResults();
            totalRowsRetrieved = result.getTotalRows();
            for (FieldValueList row : result.iterateAll()) {
                System.out.println(row);
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Insert operation not performed \n" + e.toString());
        }
        return totalRowsRetrieved;
    }

    public void deleteDataset() throws Exception {
        try {
            com.google.cloud.bigquery.BigQuery bigQuery = getBigQueryService();
            DatasetId datasetId = DatasetId.of(getProjectId(), getDataset());
            boolean success = bigQuery.delete(datasetId, com.google.cloud.bigquery.BigQuery.DatasetDeleteOption.deleteContents());
            if (success) {
                System.out.println("Dataset deleted successfully");
            } else {
                System.out.println("Dataset was not found");
            }
        } catch (BigQueryException e) {
            System.out.println("Dataset was not deleted. \n" + e.toString());
        }
    }

    @Override
    public void check() {
        System.out.printf("Check: total %s records written vs total %s records read. Check %s.", getTotalPutOperations(), getTotalReadOperations(), (getTotalPutOperations()==getTotalReadOperations())?"passed":"failed \n");
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        YAMLConfig yamlConfig = mapper.readValue(new File("src/main/resources/config.yaml"), YAMLConfig.class);
        BigQuery bigQuery=new BigQuery();

        bigQuery.setProjectId(yamlConfig.getProjectId());
        bigQuery.setServiceAccount(yamlConfig.getServiceAccount());
        HashMap<String,Object> modules = yamlConfig.getModules();
        LinkedHashMap<String, Object> map = ((ArrayList<LinkedHashMap<String, Object>>) modules.get("bigquery")).get(0);

        bigQuery.setDataset((String)map.get("dataset"));
        bigQuery.setTable((String)map.get("table"));
        ArrayList<LinkedHashMap<String,String>> schemaValue = (ArrayList<LinkedHashMap<String, String>>) map.get("schema");
        ArrayList<Field> parsedSchemaFields=new ArrayList();

        for (LinkedHashMap<String, String> schemaField : schemaValue) {
            String name = (String) schemaField.get("column");
            String typeString = (String) schemaField.get("type");
            StandardSQLTypeName standardSQLTypeName = StandardSQLTypeName.valueOf(typeString);
            parsedSchemaFields.add(Field.of(name, standardSQLTypeName));
        }

        Map<String, Object> rowContent = new HashMap<>();

        String record = (String) map.get("record");
        String[] recordValues = record.split(",");

        for (int i = 0; i < recordValues.length; i++) {
            String recordValue = recordValues[i];
            String name = parsedSchemaFields.get(i).getName();
            rowContent.put(name, recordValue);
        }

        bigQuery.run();
    }
}