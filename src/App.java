import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class App {
    // เก็บรายชื่อสมาชิก (Client IDs) + สถานะการเจอ
    private static final ArrayList<String> memberList = new ArrayList<>();
    private static final ConcurrentHashMap<String, Long> lastSeen = new ConcurrentHashMap<>();

    // กำหนด topic ต่าง ๆ ที่ใช้
    private static final String HEARTBEAT_TOPIC = "topic/heartbeat";
    private static final String BOSS_ANNOUNCE_TOPIC = "topic/BossAnnounce";

    // ที่อยู่ broker MQTT
    private static final String BROKER = "tcp://localhost:1883";

    // เวลา sleep พื้นฐานของ thread (ms)
    private static final int BaseThreadsleep = 3000;

    // ตัวแปรเก็บ leader ปัจจุบัน และสถานะการเลือกตั้ง
    private static volatile String leaderId = null;
    private static volatile boolean electionInProgress = false;

    public static void main(String[] args) throws Exception {
        // สร้าง clientID (ใช้เลข 3 หลักจากเวลา)
        final String CLIENT_ID = String.valueOf(System.currentTimeMillis()).substring(9, 12);

        // สร้าง client และเชื่อมต่อกับ broker
        MqttClient client = new MqttClient(BROKER, CLIENT_ID, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        client.connect(options);
        System.out.println("Connected to broker with client ID: " + CLIENT_ID);

        // เริ่ม thread ต่าง ๆ
        responses(client); // รับ heartbeat จาก client อื่น
        heartBeat(client); // ส่ง heartbeat ของตัวเอง
        timeOutThread(client); // จัดการ list สมาชิกที่ตาย
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
                    if (response.startsWith("Heartbeat: ")) {
                        String clientId = response.substring(11, 14);

                        // อัปเดตเวลาที่เจอล่าสุด
                        synchronized (lastSeen) {
                            lastSeen.put(clientId, System.currentTimeMillis());
                        }

                        // เพิ่ม clientId ลง memberList ถ้ายังไม่มี
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
            }
        }).start();
    }

    // Thread สำหรับส่ง heartbeat ของตัวเอง
    private static void heartBeat(MqttClient client) {
        new Thread(() -> {
            while (true) {
                sendMessage(client, HEARTBEAT_TOPIC, client.getClientId() + " is alive");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    // Thread สำหรับ mark ว่า client ตาย
    private static void timeOutThread(MqttClient client) {
        new Thread(() -> {
            while (true) {
                long now = System.currentTimeMillis();
                synchronized (lastSeen) {
                    for (Map.Entry<String, Long> entry : lastSeen.entrySet()) {
                        String clientId = entry.getKey();
                        long lastSeenTime = entry.getValue();
                        if (now - lastSeenTime > 20000) {
                            // ถ้าเกิน 20 วิ ให้ลบออกจาก lastSeen
                            lastSeen.remove(clientId);
                            // และเพิ่ม "(Dead)" ใน memberList
                            synchronized (memberList) {
                                int idx = memberList.indexOf(clientId);
                                if (idx != -1) {
                                    memberList.set(idx, clientId + " (Dead)");
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
                    }
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

        final int WAIT_HIGHER_MS = 5000; // เวลารอ OK จากไอดีที่สูงกว่า
        final int WAIT_COORD_MS = 7000; // เวลารอประกาศหัวหน้าหลังได้ OK

        BlockingQueue<String> bossQueue = new LinkedBlockingQueue<>(); // รับ BossAnnounce
        BlockingQueue<String> electionQueue = new LinkedBlockingQueue<>();
        BlockingQueue<String> answerQueue = new LinkedBlockingQueue<>();

        try {
            client.subscribe(BOSS_ANNOUNCE_TOPIC, 1,(topic, message) -> bossQueue.offer("BossAnnounce: " + new String(message.getPayload())));
            client.subscribe(ELECTION_TOPIC, 1,(topic, message) -> electionQueue.offer(new String(message.getPayload())));
            client.subscribe(ANSWER_TOPIC, 1, (topic, message) -> answerQueue.offer(new String(message.getPayload())));
        } catch (MqttException e) {
            System.err.println("Error in subscribe: " + e);
        }
        new Thread(() -> {
            // 1. ตอนเริ่ม: รอ leader จาก retained message
            final long deadline = System.currentTimeMillis() + (BaseThreadsleep * 2L);
            try {
                while (System.currentTimeMillis() < deadline) {
                    String oldBoss = bossQueue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (oldBoss != null && oldBoss.startsWith("BossAnnounce: ")) {
                        String oldBossIDString = oldBoss.substring("BossAnnounce: ".length()).trim();
                        if (!oldBossIDString.isEmpty() && !"null".equalsIgnoreCase(oldBossIDString)) {
                            leaderId = oldBossIDString;
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

                    // ตรวจว่ามีคนสูงกว่าไหม
                    boolean higherIdExists = false;
                    long waitDeadline = System.currentTimeMillis() + WAIT_HIGHER_MS;
                    try {
                        while (System.currentTimeMillis() < waitDeadline) {
                            String response = answerQueue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                            if (response != null && response.startsWith("OK:")) {
                                String responderId = response.substring(3).trim();
                                if (responderId.compareTo(client.getClientId()) > 0) {
                                    higherIdExists = true;
                                    System.out.println("Received OK from higher ID: " + responderId);
                                }
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }

                    // ถ้ามีไอดีที่สูงกว่า รอ BossAnnounce
                    if (higherIdExists) {
                        System.out.println("Higher ID exists, waiting for BossAnnounce...");
                        long coordDeadline = System.currentTimeMillis() + WAIT_COORD_MS;
                        boolean sawLeader = false;
                        try {
                            while (System.currentTimeMillis() < coordDeadline) {
                                String response = bossQueue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                                if (response != null && response.startsWith("BossAnnounce: ")) {
                                    String newLeader = response.substring("BossAnnounce: ".length()).trim();
                                    if (!newLeader.isEmpty() && !"null".equalsIgnoreCase(newLeader)) {
                                        leaderId = newLeader;
                                        sawLeader = true;
                                        System.out.println("New leader announced: " + leaderId);
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
                        if (client.getClientId().compareTo(challenger) > 0) {
                            synchronized(memberList){
                                if(memberList.contains(client.getClientId()+ " (Dead)")){continue;}
                                sendMessage(client, ANSWER_TOPIC, "OK:" + client.getClientId());
                            }
                            
                            if(client.getClientId().equals(leaderId)) {
                                sendBossAnnounce(client, leaderId);
                            } else {
                                electionInProgress = false;
                                leaderId = null;
                                sendMessage(client, ELECTION_TOPIC, "ELECTION:" + client.getClientId());                  
                            }
                        }
                    }
                } catch (Exception ignore) {
                }

                // 4. ฟัง BossAnnounce
                try {
                    String bMsg = bossQueue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
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
                    Thread.sleep(100);
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
                synchronized (System.out) {
                    System.out.println("--- Status Report ---");
                    System.out.println("This Client: " + client.getClientId());
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
                Thread.sleep(BaseThreadsleep * 5L);
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
