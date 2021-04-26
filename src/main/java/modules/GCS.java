package modules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import common.GoogleModule;
import common.ModuleName;
import util.YAMLConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.StringJoiner;

@ModuleName("gcs")
public class GCS extends GoogleModule {
    private String bucketName;
    private String filePath;
    private String downloadFilePath;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) { this.filePath = filePath; }

    public String getDownloadFilePath() { return downloadFilePath; }

    public void setDownloadFilePath(String downloadFilePath) { this.downloadFilePath = downloadFilePath; }


    @Override
    public String toString() {
        return new StringJoiner(", ", GCS.class.getSimpleName() + "[", "]")
                .add("projectId=" + getProjectId())
                .add("bucketName=" + getBucketName())
                .add("filePath=" + getFilePath())
                .toString();
    }


    @Override
    public void run() throws Exception {
        System.out.println(getServiceAccount());
        isExistingObject();
        validate();
        put();
        get();
    }

    @Override
    public void validate() {
        try {
            Storage storageService = getStorageService();
            if (storageService == null) {
                System.out.println("Failed to connect to resource. Invalid credentials.");
                System.exit(0);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public Storage getStorageService() throws Exception {
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(getServiceAccount())) {
            credentials = GoogleCredentials.fromStream(serviceAccountStream);
            return StorageOptions.newBuilder().setCredentials(credentials).setProjectId(getProjectId()).build().getService();
        } catch (IOException e) {
            throw new Exception("Invalid credentials.");
        }
    }

    @Override
    public void put() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(getServiceAccount());
        GoogleCredentials serviceAccountCredentials = GoogleCredentials.fromStream(fileInputStream);
        Storage storage = StorageOptions.newBuilder().setCredentials(serviceAccountCredentials).setProjectId(getProjectId()).build().getService();
        BlobId blobId = BlobId.of(getBucketName(), String.valueOf(Paths.get(getFilePath()).getFileName()));
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        Blob blob = storage.create(blobInfo, Files.readAllBytes(Paths.get(getFilePath())));

        System.out.println(
                "File " + getFilePath() + " uploaded to bucket " + getBucketName());
        setTotalPutOperations(1);
    }

    @Override
    public void get() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(getServiceAccount());
        GoogleCredentials serviceAccountCredentials = GoogleCredentials.fromStream(fileInputStream);
        Storage storage = StorageOptions.newBuilder().setCredentials(serviceAccountCredentials).setProjectId(getProjectId()).build().getService();
        Bucket bucket = storage.get(getBucketName());
        Blob blob = bucket.get(getDownloadFilePath());
        Path path = Paths.get(getDownloadFilePath());
        blob.downloadTo(path);

        System.out.println(
                "File " + getDownloadFilePath() + " downloaded from bucket " + getBucketName());
        setTotalReadOperations(1);
    }

    private static String getFileChecksum(MessageDigest digest, File file) throws IOException
    {
        FileInputStream fis = new FileInputStream(file);

        byte[] byteArray = new byte[1024];
        int bytesCount = 0;

        while ((bytesCount = fis.read(byteArray)) != -1) {
            digest.update(byteArray, 0, bytesCount);
        };

        fis.close();

        byte[] bytes = digest.digest();

        StringBuilder sb = new StringBuilder();
        for (byte aByte : bytes) {
            sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    public void isExistingObject() throws Exception {
        Storage storage= getStorageService();
        Bucket bucket = storage.get(getBucketName());
//        Blob blob = bucket.get(getDownloadFilePath());
        boolean exists = bucket.exists();
        System.out.println("Bucket exists.");
    }

    @Override
    public void check() throws IOException, NoSuchAlgorithmException {
        File uploadFile = new File("/Users/edlira/Downloads/test_dpp.json");
        File downloadFile = new File("/Users/edlira/Downloads/test_dpp.json");
        MessageDigest md5Digest = MessageDigest.getInstance("MD5");
        String uploadChecksum = getFileChecksum(md5Digest, uploadFile);
        String downloadChecksum = getFileChecksum(md5Digest, downloadFile);
        if(uploadChecksum.equals(downloadChecksum)){
            System.out.println("Data written and data read is a match.");
        }
        else{
            System.out.println("Data written and data read is not a match.");
        }
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        YAMLConfig yamlConfig = mapper.readValue(new File("src/main/resources/config.yaml"), YAMLConfig.class);

        GCS gcsModule = new GCS();
        gcsModule.setProjectId(yamlConfig.getProjectId());
        gcsModule.setServiceAccount(yamlConfig.getServiceAccount());
        HashMap<String,Object> modules = yamlConfig.getModules();
        LinkedHashMap<String, Object> map = ((ArrayList<LinkedHashMap<String, Object>>) modules.get("gcs")).get(0);


        gcsModule.setBucketName((String)map.get("bucketName"));
        gcsModule.setFilePath((String)map.get("filePath"));
        gcsModule.setDownloadFilePath((String)map.get("downloadFilePath"));

        gcsModule.run();
    }
}
