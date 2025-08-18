# Node-OS

Node-OS is a distributed system application that uses MQTT for communication between nodes. It simulates a network of clients that send heartbeat messages, monitor the status of other clients, and handle leader election in case of node failure.

## Features

- **Heartbeat Monitoring**: Clients send periodic heartbeat messages to indicate they are alive.
- **Status Monitoring**: Detects when a client goes offline and updates the member list accordingly.
- **Leader Election**: Initiates a leader election process when the current leader fails (to be implemented).
- **MQTT Communication**: Uses the Eclipse Paho MQTT library for message exchange.

## Project Structure
- `src/`: Contains the source code for the application.
- `bin/`: Contains compiled `.class` files.
- `lib/`: Contains external libraries used in the project.

## Prerequisites

- Java Development Kit (JDK) 8 or higher
- MQTT Broker (e.g., Mosquitto)
- Eclipse Paho MQTT library (`org.eclipse.paho.client.mqttv3-1.2.5.jar`)

## How to Run

1. **Set up the MQTT Broker**:
   - Install and start an MQTT broker (e.g., Mosquitto) on your local machine or a server.
   - Ensure the broker is running on `tcp://localhost:1883` or update the `BROKER` constant in `App.java` to match your broker's address.

2. **Compile the Code**:
   - Navigate to the `src/` directory and compile the Java files:
     ```bash
     javac -cp ../lib/org.eclipse.paho.client.mqttv3-1.2.5.jar App.java
     ```

3. **Run the Application**:
   - Execute the compiled `App` class:
     ```bash
     java -cp ../lib/org.eclipse.paho.client.mqttv3-1.2.5.jar:. App
     ```

4. **Observe the Output**:
   - The application will display logs for heartbeat messages, member list updates, and status changes.

## To-Do

- Implement the leader election algorithm in the `startElection` method.
- Add more robust error handling and logging.

## Dependencies

- [Eclipse Paho MQTT Client](https://www.eclipse.org/paho/)

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Author

- **Nilqua**