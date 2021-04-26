package util;

import java.util.HashMap;
import java.util.StringJoiner;

public class YAMLConfig {
    private String name;
    private String projectId;
    private String serviceAccount;
    private String log;
    private HashMap<String,Object> modules;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public HashMap<String,Object>   getModules() {return modules; }

    public void setModules(HashMap<String,Object>   modules) {
        this.modules = modules;
    }

    @Override
    public String toString() {
        return new StringJoiner("", "----\n","")
                .add("name=" + name + "\n")
                .add("project_id=" + projectId + "\n")
                .add("service_account=" + serviceAccount + "\n")
                .add("log=" + log + "\n" + "\n")
                .add("modules:" + modules)
                .toString();
    }

}
