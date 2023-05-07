import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Controller {

    private Communicator communicator;
    private Index index;
    private ArrayList<Integer> dstorePortsList;
    private int R;

    public Controller (int cport, int R, int timeout, int rebalance_period) {

        this.R = R;

        // create needed objects
        this.communicator = new Communicator();
        this.index = new Index();
        this.dstorePortsList = new ArrayList<>();

        openDStoreConnection(cport);

    }

    public void openDStoreConnection(int cport) {
        // open server socket
        //communicator.openServerSocket(serverSocket,cport);
        // the for keeps the connection open
        for(;;){
            try (ServerSocket serverSocket = new ServerSocket(cport)) {
                System.out.println("waiting for connection");
                Socket connection = serverSocket.accept();
                new Thread(() -> {
                    try {
                        this.handleNewConnection(connection);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
                //connection.close();
            }catch(Exception e){System.out.println("error " + e);}
        }
    }

    public void handleNewConnection (Socket connection) throws IOException {

        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream())
        );

        String line;
        while((line = in.readLine()) != null) {
            String[] splittedMessage = line.split(" ");
            int port = Integer.parseInt(splittedMessage[1]);
            if (splittedMessage[0].equals("JOIN")) {
                handleDstoreConnection(connection,port);
                break;
            } else {
                if(dstorePortsList.size() >= R) {
                    if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")){
                        receiveClientMessage(splittedMessage, connection);
                        break;
                    }
                } else {
                    System.out.println("NOT ENOUGHT DSTORES");
                    communicator.sendMessage(connection,"NOT ENOUGHT DSTORES");
                    connection.close();
                }
            }
        }
    }

    public void receiveClientMessage (String[] splittedMessage,Socket connection) throws IOException {

        switch (splittedMessage[0]) {

            case "STORE":
                index.store(splittedMessage[1], Protocol.STORE_COMPLETE_TOKEN);
                handleStore(connection);
            case "LOAD":

            case "LIST":

            case "REMOVE":

            default:
                //return ("Error, it parssed : " + splittedMessage[0]);
        }

    }

    public void handleStore(Socket connection) throws IOException {

        StringBuilder storeToList = new StringBuilder();
        for (int i = 0; i < dstorePortsList.size(); i++) {
            if(i == dstorePortsList.size() - 1) {
                storeToList.append(dstorePortsList.get(i));
            } else {
                storeToList.append(dstorePortsList.get(i)).append(" ");
            }
        }
        communicator.sendMessage(connection,Protocol.STORE_TO_TOKEN + " " + storeToList);
        listenFromDstore(connection);
    }


    public void handleDstoreConnection(Socket connection, Integer port) throws IOException {

        System.out.println(port + " joined");
        dstorePortsList.add(port);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
        String line;
        // ?
        while((line = in.readLine()) != null) {
            //System.out.println(line + " is the second message received");
            if (line.equals(Protocol.STORE_ACK_TOKEN)) {
                index.updateIndexStatus(line.split(" ")[1],Protocol.STORE_COMPLETE_TOKEN);
                // TODO Check if all R Dstores have received the file
                // TODO Then send the message to the client that the connection is completed
            }
        }
        dstorePortsList.remove(port);
        System.out.println("Dstore " + port + " disconnected");
    }

    public synchronized void listenFromDstore(Socket connection) {
        // new thread for listening to the controllers messages
        new Thread(() -> {
            try {
                BufferedReader in = null;
                in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );
                String line;
                // TODO this looks strange
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                    if (line.split(" ")[0].equals(Protocol.STORE_ACK_TOKEN)) {
                        index.updateIndexStatus(line.split(" ")[1],Protocol.STORE_COMPLETE_TOKEN);
                    } else {
                        // TODO change this message
                        System.out.println("Error produced in listening if the file has been stored");
                    }
                }
            } catch (IOException e) {
                System.out.println("Error produced in listening if the file has been stored");
                throw new RuntimeException(e);
            }
        }).start();
    }

    public static void main(String[] args){
        try{
            // get parameters
            int cPort = Integer.parseInt(args[0]);
            int r = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalancePeriod = Integer.parseInt(args[3]);

            // creating the controller
            Controller controller = new Controller(cPort, r, timeout, rebalancePeriod);
        }
        catch(Exception e){
            System.out.println("Unable to create Controller.");
        }
    }
}
