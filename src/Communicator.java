import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

    // Communicator class is responale for keeping the sockets and passoing the messages

public class Communicator {

    public synchronized void displayReceivedMessageInController(Socket connection, Integer localPort, String message) {
        System.out.println("[" + localPort + "] -> [" + connection.getLocalPort() + "] : " + message);
    }

    public synchronized void displayReceivedMessageInController(Socket connection, String message) {
        System.out.println("[" + connection.getPort() + "] -> [" + connection.getLocalPort() + "] : " + message);
    }

    public synchronized void displaySentMessage(Socket connection, String message) {
        System.out.println("[" + connection.getLocalPort() + "] -> [" + connection.getPort() + "] : " + message);
    }

    public synchronized void displayReceivedMessageInDstore(Socket connection, Integer localPort, String message) {
        System.out.println("[" + connection.getPort() + "] -> [" +  localPort + "] : " + message);
    }

    public synchronized void sendMessage(Socket connection , String message) {

        try {
            PrintWriter out = new PrintWriter(connection.getOutputStream());
            out.println(message); out.flush();
        } catch (IOException e) {
            System.out.println("error in sendMessage");
        }
    }

    public String receiveAnyMessage(String[] splittedMessage) throws IOException {

        return switch (splittedMessage[0]) {
            case"STORE", "REMOVE", "LOAD", "LIST" -> ("CLIENT");
            default -> ("NOT CLIENT");
        };

    }

    public synchronized void listenAndDisplayToTerminal(Socket connection) {
        // new thread for listening to the controllers messages
        new Thread(() -> {
            BufferedReader in = null;
            try {
                in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String line;

            // listens until decide who made the connection
            while(true) {
                try {
                    if ((line = in.readLine()) == null) break;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(line + " received");
            }
        }).start();
    }

}




