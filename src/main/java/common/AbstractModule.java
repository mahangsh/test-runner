package common;

import interfaces.ModuleInterface;

public abstract class AbstractModule implements ModuleInterface {
    private long totalPutOperations = 0;
    private long totalReadOperations = 0;

    public long getTotalPutOperations() {
        return totalPutOperations;
    }

    public void setTotalPutOperations(long totalPutOperations) {
        this.totalPutOperations = totalPutOperations;
    }

    public long getTotalReadOperations() {
        return totalReadOperations;
    }

    public void setTotalReadOperations(long totalReadOperations) {
        this.totalReadOperations = totalReadOperations;
    }

    @Override
    public void validate() throws Exception {
        System.out.println("Validation is not implemented in this module. Skipping");
    }

    @Override
    public void put() throws Exception {
        System.out.println("Validation is not implemented in this module. Skipping");
    }

    @Override
    public void check() throws Exception {
        System.out.printf("Check: total %s write operations vs total %s read operations. Check %s.\n", totalPutOperations,
                totalReadOperations, (totalPutOperations == totalReadOperations) ? "passed" : "failed");
    }

    @Override
    public void run() throws Exception {
        validate();
        put();
        get();
        check();
    }

//    public void authenticate() throws IOException {
//        System.out.println("Authenticating");
//    }
//    public void validate() throws IOException {
//        System.out.println("Validating service with service account");
//    }
//    public void put() throws IOException {
//        System.out.println("Writing data");
//    }
//    public void get() throws IOException {
//        System.out.println("Reading data");
//    }
//    public void assertion() throws IOException {
//        System.out.println("Comparing input with output");
//    }

//    public static <module> void runTest(String module ){
//        System.out.println(module);
//        System.out.println("Running Tests");
//    }

//    protected abstract void runTest();
}
