package modules;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcare.Projects.Locations.Datasets.DicomStores;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
import com.google.api.services.healthcare.v1.model.DicomStore;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import common.GoogleModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class DICOM extends GoogleModule {
    private String regionId;
    private String datasetId;
    private String storeId;
    private static final String DATASET_NAME = "projects/%s/locations/%s/datasets/%s";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final NetHttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    @Override
    public void run() throws IOException {
        dicomStoreCreate();
    }

    @Override
    public void validate() throws IOException {

    }

    @Override
    public void put() throws IOException, InterruptedException {

        setTotalPutOperations(0);
    }

    @Override
    public void get() throws IOException {
        setTotalReadOperations(0);
    }

    public void dicomStoreCreate( ) throws IOException {
         String datasetName =
             String.format(DATASET_NAME, getProjectId(), getRegionId() , getDatasetId());

        // Initialize the client, which will be used to interact with the service.
        CloudHealthcare client = createClient();
        Map<String, String> labels = new HashMap<String, String>();
        labels.put("key1", "value1");
        labels.put("key2", "value2");
        DicomStore content = new DicomStore().setLabels(labels);

        // Create request and configure any parameters.
        DicomStores.Create request =
                client
                        .projects()
                        .locations()
                        .datasets()
                        .dicomStores()
                        .create(datasetName, content)
                        .setDicomStoreId(storeId);
        DicomStore response = request.execute();
        System.out.println("DICOM store created: " + response.toPrettyString());
    }

    private CloudHealthcare createClient() throws IOException {

        File credentialsPath = new File(getServiceAccount());
        GoogleCredentials credential;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credential = GoogleCredentials.fromStream(serviceAccountStream).getApplicationDefault()
                    .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
        }

        // Create a HttpRequestInitializer, which will provide a baseline configuration to all requests.
        HttpRequestInitializer requestInitializer =
                request -> {
                    new HttpCredentialsAdapter(credential).initialize(request);
                    request.setConnectTimeout(60000); // 1 minute connect timeout
                    request.setReadTimeout(60000); // 1 minute read timeout
                };

        // Build the client for interacting with the service.
        return new CloudHealthcare.Builder(HTTP_TRANSPORT, JSON_FACTORY, requestInitializer)
                .setApplicationName("app-name")
                .build();
    }

}
