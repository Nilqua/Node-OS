import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.*;

public class App {
    // เก็บรายชื่อสมาชิก (Client IDs)
    private static final ArrayList<String> memberList = new ArrayList<>();

    // กำหนด topic ต่าง ๆ ที่ใช้
    private static final String HEARTBEAT_TOPIC = "topic/heartbeat";
    private static final String WILL_TOPIC = "topic/will";
    private static final String BOSS_ANNOUNCE_TOPIC = "topic/BossAnnounce";

    // ที่อยู่ broker MQTT
    private static final String BROKER = "tcp://localhost:1883";

    // เวลา sleep พื้นฐานของ thread (ms)
    private static final int BaseThreadsleep = 3000;

    // ตัวแปรเก็บ leader ปัจจุบัน และสถานะการเลือกตั้ง
    private static volatile String leaderId = null;
    private static volatile boolean electionInProgress = false;

    public static void main(String[] args) throws Exception {
        // สร้าง clientID (ใช้เลข 3 หลักจากเวลา) + Will message
        final String CLIENT_ID = String.valueOf(System.currentTimeMillis()).substring(9, 12);
        final String WILL_MESSAGE = CLIENT_ID + " died";

        // สร้าง client และเชื่อมต่อกับ broker
        MqttClient client = new MqttClient(BROKER, CLIENT_ID);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setKeepAliveInterval(10);
        options.setWill(WILL_TOPIC, WILL_MESSAGE.getBytes(), 1, false);
        client.connect(options);
        System.out.println("Connected to broker with client ID: " + CLIENT_ID);

        // เริ่ม thread ต่าง ๆ
        responses(client); // รับ heartbeat จาก client อื่น
        heartBeat(client); // ส่ง heartbeat ของตัวเอง
        willClient(client); // ฟัง Will message จาก client ที่ตาย
        bossThread(client); // จัดการเลือก leader
        statusThread(client); // พิมพ์สถานะปัจจุบัน
        validationThread(); // ตรวจสอบความถูกต้องของ leader
    }

    // Thread สำหรับรับ heartbeat และอัปเดต member list
    private static void responses(MqttClient client) {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        try {
            client.subscribe(HEARTBEAT_TOPIC, 1, (topic, message) -> {
                messageQueue.offer("Heartbeat: " + new String(message.getPayload()));
            });
        } catch (MqttException e) {
            System.err.println("Error in subscribe: " + e);
        }
        new Thread(() -> {
            while (true) {
                try {
                    String response = messageQueue.take();
                    synchronized (System.out) {
                        System.out.println(response);
                    }
                    // เพิ่ม client เข้า memberList ถ้ายังไม่มี
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
                    System.err.println("Error in message processing: " + e);
                }
                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // Thread สำหรับส่ง heartbeat ของตัวเอง
    private static void heartBeat(MqttClient client) {
        new Thread(() -> {
            while (true) {
                sendMessage(client, HEARTBEAT_TOPIC, client.getClientId() + " is alive");
                try {
                    Thread.sleep(BaseThreadsleep + 5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // Thread สำหรับรับ Will message และ mark ว่า client ตาย
    private static void willClient(MqttClient client) {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
        try {
            client.subscribe(WILL_TOPIC, 1, (topic, message) -> {
                messageQueue.offer("Will: " + new String(message.getPayload()));
            });
        } catch (MqttException e) {
            System.err.println("Error in status check: " + e);
        }
        new Thread(() -> {
            while (true) {
                try {
                    String response = messageQueue.take();
                    System.out.println(response);

                    if (response.startsWith("Will: ")) {
                        String clientId = response.substring(6, 9);
                        synchronized (memberList) {
                            int idx = memberList.indexOf(clientId);
                            if (idx != -1) {
                                memberList.set(idx, clientId + " (Dead)");
                            } else if (!memberList.contains(clientId + " (Dead)")) {
                                memberList.add(clientId + " (Dead)");
                            }
                            // ถ้า leader ตาย ให้เคลียร์ leader ออก
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
                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // Thread สำหรับจัดการเลือก leader และฟัง BossAnnounce
    private static void bossThread(MqttClient client) {
        // ใช้ BossAnnounce เป็น COORDINATOR (retained)
        final String ELECTION_TOPIC = "topic/election"; // ELECTION:<id>
        final String ANSWER_TOPIC = "topic/answer"; // OK:<id>

        final int WAIT_HIGHER_MS = 2000; // เวลารอ OK จากไอดีที่สูงกว่า
        final int WAIT_COORD_MS = 3000; // เวลารอประกาศหัวหน้าหลังได้ OK

        BlockingQueue<String> bossQueue = new LinkedBlockingQueue<>(); // รับ BossAnnounce
        BlockingQueue<String> electionQueue = new LinkedBlockingQueue<>();
        BlockingQueue<String> answerQueue = new LinkedBlockingQueue<>();

        try {
            client.subscribe(BOSS_ANNOUNCE_TOPIC, 1,
                    (topic, message) -> bossQueue.offer("BossAnnounce: " + new String(message.getPayload())));
            client.subscribe(ELECTION_TOPIC, 1,
                    (topic, message) -> electionQueue.offer(new String(message.getPayload())));
            client.subscribe(ANSWER_TOPIC, 1, (topic, message) -> answerQueue.offer(new String(message.getPayload())));
        } catch (MqttException e) {
            System.err.println("Error in subscribe: " + e);
        }
        new Thread(() -> {
            // 1. ตอนเริ่ม: รอ leader จาก retained message
            final long deadline = System.currentTimeMillis() + (BaseThreadsleep * 2L);
            try {
                while (System.currentTimeMillis() < deadline) {
                    String pre = bossQueue.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS);
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
                // 2.ถ้าไม่มี leader ให้เริ่มเลือกตั้งใหม่
                if (leaderId == null && !electionInProgress) {
                    System.out.println("No leader detected, starting election...");

                    synchronized (memberList) {
                        electionInProgress = true;
                    }

                    // ส่ง ELECTION
                    sendMessage(client, ELECTION_TOPIC, "ELECTION:" + client.getClientId());
                    boolean higherIdExists = false;
                    long waitDeadline = System.currentTimeMillis() + WAIT_HIGHER_MS;
                    try {
                        while (System.currentTimeMillis() < waitDeadline) {
                            String response = answerQueue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                            if (response != null && response.startsWith("OK:")) {
                                String responderId = response.substring(3).trim();
                                if (responderId.compareTo(client.getClientId()) > 0) {
                                    higherIdExists = true;
                                }
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }

                    // ถ้ามีไอดีที่สูงกว่า รอ BossAnnounce
                    if (higherIdExists) {
                        long coordDeadline = System.currentTimeMillis() + WAIT_COORD_MS;
                        boolean sawLeader = false;
                        try {
                            while (System.currentTimeMillis() < coordDeadline) {
                                String response = bossQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                                if (response != null && response.startsWith("BossAnnounce: ")) {
                                    String newLeader = response.substring("BossAnnounce: ".length()).trim();
                                    if (!newLeader.isEmpty() && !"null".equalsIgnoreCase(newLeader)) {
                                        leaderId = newLeader;
                                        sawLeader = true;
                                        break;
                                    }
                                }
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }

                        if (!sawLeader) {
                            System.out.println("No leader announced, restarting election...");
                            synchronized (memberList) {
                                electionInProgress = false;
                            }
                            continue;
                        }
                    } else {
                        // ตัวเองมีไอดีสูงสุด → เป็น leader
                        leaderId = client.getClientId();
                        sendBossAnnounce(client, leaderId);
                        System.out.println("New leader elected: " + leaderId);
                    }

                    synchronized (memberList) {
                        electionInProgress = false;
                    }
                }

                // 3. ฟัง elction และตอบ Ok ถ้าไอดีสูงกว่า
                try {
                    String eMsg = electionQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (eMsg != null && eMsg.startsWith("ELECTION:")) {
                        String challenger = eMsg.substring("ELECTION:".length());
                        if (challenger.compareTo(client.getClientId()) < 0) {
                            sendMessage(client, ANSWER_TOPIC, "OK:" + client.getClientId());
                            if (!electionInProgress) {
                                electionInProgress = true;
                                leaderId = null;
                            }
                        }
                    }
                } catch (Exception ignore) {
                }

                // 4. ฟัง BossAnnounce
                try {
                    String bMsg = bossQueue.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (bMsg != null && bMsg.startsWith("BossAnnounce: ")) {
                        String newLeader = bMsg.substring("BossAnnounce: ".length()).trim();
                        if (newLeader != null && !newLeader.isEmpty()) {
                            synchronized (memberList) {
                                if (!newLeader.contains(" (Dead)")) {
                                    leaderId = newLeader;
                                }
                            }
                        }
                    }
                } catch (Exception ignore) {
                }

                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // Thread สำหรับแสดงสถานะสมาชิกและ leader
    private static void statusThread(MqttClient client) {
        new Thread(() -> {
            while (true) {
                synchronized (memberList) {
                    System.out.println("--- Status Report ---");
                    System.out.println("Current members: " + memberList);
                    System.out.println("Current leader: " + leaderId);
                    System.out.println("---------------------");
                }
                try {
                    Thread.sleep(BaseThreadsleep);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // Thread สำหรับตรวจสอบว่า leader ที่ถืออยู่ยังมีจริงหรือไม่
    private static void validationThread() {
        new Thread(() -> {
            try {
                Thread.sleep(BaseThreadsleep * 10L);
            } catch (InterruptedException e) {
            }
            while (true) {
                // อ่านสถานะ leader โดยไม่จับล็อก เพื่อไม่บล็อกเธรดอื่น
                String currentLeader = leaderId;
                if (currentLeader == null) {
                    try {
                        Thread.sleep(BaseThreadsleep * 2L);
                    } catch (InterruptedException e) {
                    }
                    continue;
                }

                boolean validLeader = false;
                synchronized (memberList) {
                    for (String member : memberList) {
                        if (member.contains(currentLeader) && !member.contains(" (Dead)")) {
                            validLeader = true;
                            break;
                        }
                    }
                    if (!validLeader) {
                        leaderId = null;
                        electionInProgress = false;
                        System.out.println("Leader is not valid reset to null");
                    }
                }
                try {
                    Thread.sleep(BaseThreadsleep * 5L);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // ส่ง BossAnnounce
    private static void sendBossAnnounce(MqttClient client, String id) {
        try {
            MqttMessage msg = new MqttMessage(id == null ? new byte[0] : id.getBytes());
            msg.setQos(1);
            msg.setRetained(true);
            client.publish(BOSS_ANNOUNCE_TOPIC, msg);
        } catch (Exception e) {
            System.err.println("sendBossAnnounce error: " + e);
        }
    }

    // ล้าง retained message ของ BossAnnounce
    private static void clearRetain(MqttClient client, String topic) {
        try {
            MqttMessage empty = new MqttMessage(new byte[0]);
            empty.setQos(1);
            empty.setRetained(true);
            client.publish(topic, empty);
        } catch (Exception e) {
            System.err.println("clearRetain error: " + e);
        }
    }

    // ฟังก์ชันส่งข้อความทั่วไป
    private static void sendMessage(MqttClient client, String topic, String message) {
        try {
            MqttMessage mqttMessage = new MqttMessage(message.getBytes());
            mqttMessage.setQos(1);
            client.publish(topic, mqttMessage);
        } catch (Exception e) {
            System.err.println("sendMessage error: " + e);
        }
    }
}
