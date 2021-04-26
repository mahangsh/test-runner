package modules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import common.GoogleModule;
import common.ModuleName;
import util.YAMLConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@ModuleName("pubsub")
public class PubSub extends GoogleModule {
    private String topicId;
    private String subscriptionId;
    private String message;

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    public String getSubscriptionId() {return subscriptionId; }

    public void setSubscriptionId(String subscriptionId) { this.subscriptionId = subscriptionId; }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PubSub.class.getSimpleName() + "[", "]")
                .add("projectId=" + getProjectId())
                .add("topicId=" + getTopicId())
                .add("message=" + getMessage())
                .toString();
    }

    @Override
    public void run() throws Exception {
        validate();
        put();
        get();
        check();
    }

    @Override
    public void validate() {
        try {
           Publisher publisherService= getPubSubService();
            if (publisherService == null) {
                System.out.println("Failed to connect to resource. Invalid credentials.");
                System.exit(0);
            }
            publisherService.shutdown();
        }catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public Publisher getPubSubService() throws Exception {
        File credentialsPath = new File(getServiceAccount());
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
            TopicName topicName = TopicName.of(getProjectId(), topicId);
            return Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
        } catch (IOException e) {
            throw new Exception("Invalid credentials.");
        }
    }
    @Override
    public void put() throws Exception {
        publisherExample();
        setTotalPutOperations(1);
    }

    public void publisherExample() throws Exception {
        Publisher publisher = getPubSubService();
        try {
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            System.out.println("Published message ID: " + messageId);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

    @Override
    public void get() throws IOException {
        subscribeAsyncExample();
        setTotalReadOperations(1);
    }

    public void subscribeAsyncExample() throws IOException {
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(getProjectId(), subscriptionId);

        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    System.out.println("Id: " + message.getMessageId());
                    System.out.println("Data: " + message.getData().toStringUtf8());
                    consumer.ack();
                };
        File credentialsPath = new File(getServiceAccount());
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            subscriber.stopAsync();
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
        PubSub pubSub= new PubSub();

        pubSub.setProjectId(yamlConfig.getProjectId());
        pubSub.setServiceAccount(yamlConfig.getServiceAccount());
        HashMap<String,Object> modules = yamlConfig.getModules();
        LinkedHashMap<String, Object> map = ((ArrayList<LinkedHashMap<String, Object>>) modules.get("pubsub")).get(0);

        pubSub.setTopicId((String)map.get("topicId"));
        pubSub.setSubscriptionId((String)map.get("subscriptionId"));
        pubSub.setMessage((String)map.get("message"));

        pubSub.run();
    }
}
