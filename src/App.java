import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.client.mqttv3.*;

public class App {
    // Constants and Variables
    private static final ArrayList<String> memberList = new ArrayList<>();
    private static final String HEARTBEAT_TOPIC = "topic/heartbeat";
    private static final String WILL_TOPIC = "topic/will";
    private static final String BROKER = "tcp://localhost:1883";

    private static String leaderId = null;
    private static boolean electionInProgress = false; // TODO IDK if this is needed

    public static void main(String[] args) throws Exception {
        // Random client ID + Will message
        final String CLIENT_ID = String.valueOf(System.currentTimeMillis()).substring(9, 12);
        final String WILL_MESSAGE = CLIENT_ID + " died";

        // Create and connect MQTT client and it's options
        MqttClient client = new MqttClient(BROKER, CLIENT_ID);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setKeepAliveInterval(2);
        options.setWill(WILL_TOPIC, WILL_MESSAGE.getBytes(), 1, false);
        client.connect(options);
        System.out.println("Connected to broker with client ID: " + CLIENT_ID);

        // Start Receive heartbeat messages thread
        responses(client);

        // Start Heartbeat thread
        heartBeat(client);

        // Start Status Client Thread
        statusClient(client);
    }

    // Receive Thread
    private static void responses(MqttClient client) {
        //use BlockingQueue to get messages
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                // Subscribe to MQTT topics
                client.subscribe(HEARTBEAT_TOPIC, 1, (topic, message) -> {
                    messageQueue.offer(new String("Heartbeat: " + new String(message.getPayload())));
                });
            } catch (MqttException e) {
                System.out.println("Error in subscribe: " + e);
            }

            while (true) {
                try {
                    // Take message from queue
                    String response = messageQueue.take();
                    System.out.println(response);

                    // check and update memberList
                    if (response.startsWith("Heartbeat: ")) {
                        String clientId = response.substring(11, 14);
                        synchronized (memberList) {
                            if (!memberList.contains(clientId) && !memberList.contains(clientId + " (Dead)")) {
                                memberList.add(clientId);
                                memberList.sort(String::compareTo);
                            }
                        }
                    }

                    // Print current members and leader
                    synchronized (memberList) {
                        System.out.println("Current members: " + memberList);
                        System.out.println("Current leader: " + leaderId);
                    }
                } catch (Exception e) {
                    System.out.println("Error in message processing: " + e);
                }

                // Sleep the thread
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                    break;
                }
            }
        }).start();
    }

    // Heartbeat Thread
    private static void heartBeat(MqttClient client) {
        new Thread(() -> {
            while (true) {
                // Send heartbeat message
                sendMessage(client, HEARTBEAT_TOPIC, client.getClientId() + " is alive");

                // Sleep the thread
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                    break;
                }
            }
        }).start();
    }

    // Status Client Thread
    private static void statusClient(MqttClient client) {
        //use BlockingQueue to get messages
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                client.subscribe(WILL_TOPIC, 1, (topic, message) -> {
                messageQueue.offer(new String("Will: " + new String(message.getPayload())));
            });
            } catch (MqttException e) {
                System.out.println("Error in status check: " + e);
            }

            while (true) {
                try {
                    // Take message from queue
                    String response = messageQueue.take();
                    System.out.println(response);

                    //check and update memberList for dead clients
                    if (response.startsWith("Will: ")) {
                        String clientId = response.substring(6, 9);
                        synchronized (memberList) {
                            memberList.set(memberList.indexOf(clientId), clientId + " (Dead)");
                        }

                        // check if the leader is getting killed
                        if (clientId.equals(leaderId)) {
                            startElection(client);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error in status client: " + e);
                }

                // Sleep the thread
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                }
            }
        }).start();
    }
    
    // TODO Election
    private static void startElection(MqttClient client) {
        
    }

    // Message Sender
    private static void sendMessage(MqttClient client, String topic, String message) {
        try {
            MqttMessage mqttMessage = new MqttMessage(message.getBytes());
            mqttMessage.setQos(1);
            client.publish(topic, mqttMessage);
        } catch (MqttException e) {
            System.err.println("Error in Message Sender: " + e);
        }
    }
}
