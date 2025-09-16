import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.client.mqttv3.*;

public class App {
    // Constants and Variables
    private static final ArrayList<String> memberList = new ArrayList<>();
    private static final String HEARTBEAT_TOPIC = "topic/heartbeat";
    private static final String WILL_TOPIC = "topic/will";
    private static final String BOSS_ANNOUNCE_TOPIC = "topic/BossAnnounce";
    private static final String BROKER = "tcp://192.168.100.9:1883";
    private static final int BaseThreadsleep = 1000;

    private static volatile String leaderId = null;
    private static volatile boolean electionInProgress = false;

    public static void main(String[] args) throws Exception {
        // Random client ID + Will message
        final String CLIENT_ID = String.valueOf(System.currentTimeMillis()).substring(9, 12);
        final String WILL_MESSAGE = CLIENT_ID + " died";

        // Create and connect MQTT client and it's options
        MqttClient client = new MqttClient(BROKER, CLIENT_ID);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setKeepAliveInterval(10);
        options.setWill(WILL_TOPIC, WILL_MESSAGE.getBytes(), 1, false);
        client.connect(options);
        System.out.println("Connected to broker with client ID: " + CLIENT_ID);

        // Start Receive heartbeat messages thread
        responses(client);

        // Start Heartbeat thread
        heartBeat(client);

        // Start Will Client Thread
        willClient(client);

        // Start Boss Thread
        bossThread(client);

        // Start Status Thread
        statusThread(client);

        //start Validation Thread
        validationThread();
    }

    // response Thread
    private static void responses(MqttClient client) {
        //use BlockingQueue to get messages
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        try {
            // Subscribe to MQTT topics
            client.subscribe(HEARTBEAT_TOPIC, 1, (topic, message) -> {
                messageQueue.offer(new String("Heartbeat: " + new String(message.getPayload())));
            });
        } catch (MqttException e) {
            System.out.println("Error in subscribe: " + e);
        }
        new Thread(() -> {

            while (true) {
                try {
                    // Take message from queue
                    String response = messageQueue.take();
                    synchronized (System.out) {
                        System.out.println(response);
                    }

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
                } catch (Exception e) {
                    System.out.println("Error in message processing: " + e);
                }

                // Sleep the thread
                try {
                    Thread.sleep(BaseThreadsleep);
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
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                    break;
                }
            }
        }).start();
    }

    // Will Client Thread
    private static void willClient(MqttClient client) {
        //use BlockingQueue to get messages
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        try {
            client.subscribe(WILL_TOPIC, 1, (topic, message) -> {
            messageQueue.offer(new String("Will: " + new String(message.getPayload())));
        });
        } catch (MqttException e) {
            System.out.println("Error in status check: " + e);
        }
        new Thread(() -> {
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
                            if (clientId.equals(leaderId)) {
                                leaderId = null;
                                electionInProgress = false;
                                System.out.println("Leader " + clientId + " has died.");
                                clearRetain(client, BOSS_ANNOUNCE_TOPIC);
                            }
                        }

                    }
                } catch (Exception e) {
                    System.err.println("Error in status client: " + e);
                }

                // Sleep the thread
                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                }
            }
        }).start();
    }

    // Boss Thread
    private static void bossThread(MqttClient client) {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        try {
            // Subscribe to MQTT topics
            client.subscribe(BOSS_ANNOUNCE_TOPIC, 1, (topic, message) -> {
                messageQueue.offer(new String("BossAnnounce: " + new String(message.getPayload())));
            });
        } catch (MqttException e) {
            System.out.println("Error in subscribe: " + e);
        }
        new Thread(() -> {
            final long deadline = System.currentTimeMillis() + (BaseThreadsleep * 2L);
            try {
                while (System.currentTimeMillis() < deadline) {
                    String pre = messageQueue.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (pre != null && pre.startsWith("BossAnnounce: ")) {
                        String payload = pre.substring("BossAnnounce: ".length()).trim();
                        if (!payload.isEmpty() && !"null".equalsIgnoreCase(payload)) {
                            leaderId = payload;
                            System.out.println("Leader found on startup: " + leaderId);
                            break;
                        }
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            while (true) {
                // If no leader, start election
                    if (leaderId == null && !electionInProgress) {
                        System.out.println("No leader detected, starting election...");
                        synchronized (memberList) {
                        electionInProgress = true;

                        // Elect the lowest client ID as leader
                        for (String member : memberList) {
                            if (!member.contains(" (Dead)")) {
                                leaderId = member;
                                break;
                            }
                        }

                        //check if leaderId is still null
                        if (leaderId != null) {
                            sendBossAnnounce(client,leaderId);
                            System.out.println("New leader elected: " + leaderId);
                        } else {
                            System.out.println("No eligible members to elect as leader.");
                        }
                        electionInProgress = false;
                    }
                }

                // Check for boss announce messages
                try {
                    String response = messageQueue.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if(response == null) {
                        continue;
                    }
                    System.out.println(response);

                    // Update leaderId if a new leader is announced
                    if (response.startsWith("BossAnnounce: ")) {
                        String newLeaderId = response.substring("BossAnnounce: ".length()).trim();
                        if (leaderId != null && leaderId.equals(newLeaderId)){
                            continue;
                        }
                        synchronized (memberList) {
                            if (memberList.contains(newLeaderId) && !newLeaderId.contains(" (Dead)")) {
                                leaderId = newLeaderId;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error in boss announce processing: " + e);
                }

                // Sleep the thread
                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                    break;
                }
            }
        }).start();
    }

    // Status Thread
    private static void statusThread(MqttClient client) {
        new Thread(() -> {
            while (true) {
                // Print current members and leader
                synchronized (memberList) {
                    System.out.println("--- Status Report ---");
                    System.out.println("Current members: " + memberList);
                    System.out.println("Current leader: " + leaderId);
                    System.out.println("---------------------");
                }

                // Sleep the thread
                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                    break;
                }
            }
        }).start();
    }

    // Validation Thread
    private static void validationThread() {
        new Thread(() -> {
            try {
                Thread.sleep(BaseThreadsleep * 3L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println(e);
            }
            while (true) {
                boolean validLeader = false;
                synchronized (memberList) {
                    if (leaderId == null) {
                        break;
                        }
                    for (String member : memberList) {
                        if (member.contains(leaderId) && !member.contains(" (Dead)")) {
                            validLeader = true;
                            break;
                        }
                    }
                    if (!validLeader) {
                        leaderId = null;
                        electionInProgress = false;
                    }
                    System.out.println("Initial member list: " + memberList);
                }
                try {
                    Thread.sleep(BaseThreadsleep * 5L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println(e);
                    break;
                }
            }
        }).start();
    }
    
    // Boss Announce Sender
    private static void sendBossAnnounce(MqttClient client, String id) {
        try {
            MqttMessage msg = new MqttMessage(id == null ? new byte[0] : id.getBytes());
            msg.setQos(1);
            msg.setRetained(true);
            client.publish(BOSS_ANNOUNCE_TOPIC, msg);
        } catch (Exception ignore) { }
    }

    // Clear Retain Message
    private static void clearRetain(MqttClient client, String topic) {
        try {
            MqttMessage empty = new MqttMessage(new byte[0]);
            empty.setQos(1);
            empty.setRetained(true);
            client.publish(topic, empty);
        } catch (Exception ignore) { }
    }
    
    // Message Sender
    private static void sendMessage(MqttClient client, String topic, String message) {
        try {
            MqttMessage mqttMessage = new MqttMessage(message.getBytes());
            mqttMessage.setQos(1);
            client.publish(topic, mqttMessage);
        } catch (Exception e) {
        }
    }
}
