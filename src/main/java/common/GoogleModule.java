package common;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class GoogleModule extends AbstractModule {

    private String projectId;
    private String serviceAccount;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getServiceAccount() { return serviceAccount; }

    public void setServiceAccount(String serviceAccount) { this.serviceAccount = serviceAccount; }

    @Override
    public void run() throws Exception {

    }

    @Override
    public void validate() throws IOException {

    }

    @Override
    public void put() throws Exception {

    }

    @Override
    public void get() throws Exception {

    }

    @Override
    public void check() throws IOException, NoSuchAlgorithmException {

    }
}
