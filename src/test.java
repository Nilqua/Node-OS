import java.util.*;
import java.util.concurrent.*;
import org.eclipse.paho.client.mqttv3.*;

public class test {
    private static final ArrayList<String> memberList = new ArrayList<>();
    private static final String HEARTBEAT_TOPIC = "topic/heartbeat";
    private static final String WILL_TOPIC = "topic/will";
    private static final String ELECTION_TOPIC = "topic/election";
    private static final String COORDINATOR_TOPIC = "topic/coordinator";
    private static final String BROKER = "tcp://localhost:1883";

    private static String leaderId = null;
    private static String clientId;

    public static void main(String[] args) throws Exception {
        clientId = String.valueOf(System.currentTimeMillis()).substring(9, 12);
        String willMessage = clientId + " died";

        MqttClient client = new MqttClient(BROKER, clientId);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setKeepAliveInterval(2);
        options.setWill(WILL_TOPIC, willMessage.getBytes(), 1, true);
        client.connect(options);

        System.out.println("Connected to broker with client ID: " + clientId);

        // เริ่มฟัง heartbeat และ will
        receiveHeartbeats(client);
        statusListener(client);

        // เริ่มฟัง election และ coordinator message
        electionListener(client);
        coordinatorListener(client);

        // เริ่มส่ง heartbeat
        sendHeartbeat(client);

        // รอ 10 วินาที เพื่อรอ process ตัวอื่น join
        waitForProcessesThenElect(client);
    }

    private static void waitForProcessesThenElect(MqttClient client) {
        new Thread(() -> {
            try {
                System.out.println("Waiting 10 seconds for other processes to join...");
                Thread.sleep(10000);

                synchronized (memberList) {
                    long aliveCount = memberList.stream()
                        .filter(m -> !m.contains("(Dead)"))
                        .count();

                    if (aliveCount == 1) {
                        // ตัวเอง process เดียวในระบบ
                        leaderId = clientId;
                        sendMessage(client, COORDINATOR_TOPIC, leaderId);
                        System.out.println("Only one process, I am the leader: " + leaderId);
                    } else if (aliveCount > 1) {
                        // มากกว่า 1 process เริ่ม election
                        System.out.println("More than one process detected, starting election...");
                        startElection(client);
                    } else {
                        System.out.println("No alive processes found!");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void receiveHeartbeats(MqttClient client) {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                client.subscribe(HEARTBEAT_TOPIC, 1, (topic, message) -> {
                    messageQueue.offer(new String(message.getPayload()));
                });
            } catch (MqttException e) {
                e.printStackTrace();
            }

            while (true) {
                try {
                    String msg = messageQueue.take();
                    String id = msg.split(" ")[0];

                    synchronized (memberList) {
                        if (!memberList.contains(id) && !memberList.contains(id + " (Dead)")) {
                            memberList.add(id);
                            memberList.sort(String::compareTo);
                        }
                    }

                    synchronized (memberList) {
                        System.out.println("Members: " + memberList + " | Leader: " + leaderId);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void sendHeartbeat(MqttClient client) {
        new Thread(() -> {
            while (true) {
                sendMessage(client, HEARTBEAT_TOPIC, clientId + " is alive");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    private static void statusListener(MqttClient client) {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        new Thread(() -> {
            try {
                client.subscribe(WILL_TOPIC, 1, (topic, message) -> {
                    messageQueue.offer(new String(message.getPayload()));
                });
            } catch (MqttException e) {
                e.printStackTrace();
            }

            while (true) {
                try {
                    String msg = messageQueue.take();
                    String deadId = msg.split(" ")[0];

                    synchronized (memberList) {
                        int idx = memberList.indexOf(deadId);
                        if (idx != -1) {
                            memberList.set(idx, deadId + " (Dead)");
                        }
                    }

                    if (deadId.equals(leaderId)) {
                        System.out.println("Leader " + leaderId + " died! Starting election...");
                        startElection(client);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void electionListener(MqttClient client) {
        try {
            client.subscribe(ELECTION_TOPIC, 1, (topic, message) -> {
                String fromId = new String(message.getPayload());
                if (Integer.parseInt(clientId) > Integer.parseInt(fromId)) {
                    sendMessage(client, COORDINATOR_TOPIC, clientId);
                }
            });
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void coordinatorListener(MqttClient client) {
        try {
            client.subscribe(COORDINATOR_TOPIC, 1, (topic, message) -> {
                String newLeader = new String(message.getPayload());
                leaderId = newLeader;
                System.out.println("New leader announced: " + leaderId);
            });
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void startElection(MqttClient client) {
        System.out.println("Starting election...");
        sendMessage(client, ELECTION_TOPIC, clientId);

        // รอ coordinator message ถ้าไม่มี ให้ตัวเองเป็น leader
        new Thread(() -> {
            try {
                Thread.sleep(2000);

                synchronized (memberList) {
                    long aliveCount = memberList.stream()
                        .filter(m -> !m.contains("(Dead)")).count();

                    if (aliveCount == 1 || leaderId == null) {
                        leaderId = clientId;
                        sendMessage(client, COORDINATOR_TOPIC, leaderId);
                        System.out.println("I am the new leader: " + leaderId);
                    } else {
                        System.out.println("Waiting for coordinator message...");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void sendMessage(MqttClient client, String topic, String message) {
        try {
            client.publish(topic, new MqttMessage(message.getBytes()));
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}