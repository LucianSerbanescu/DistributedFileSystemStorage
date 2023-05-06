import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Controller {

    protected Communicator communicator;
    protected Index index;
    protected ArrayList<Integer> dstorePortsList;
    protected Integer dstoreCounter = 0;
    protected int R;

    public Controller (int cport, int R, int timeout, int rebalance_period) {

        this.R = R;

        // create needed objects
        communicator = new Communicator();
        index = new Index();
        dstorePortsList = new ArrayList<>();

        //TODO if there are not enought dstores, handle only dstores connections
        if (dstorePortsList.size() >= R ) {
            openDStoreConnection(cport);
        } else {

        }



    }

    public void openDStoreConnection(int cport) {
        // open server socket
        //communicator.openServerSocket(serverSocket,cport);
        try (ServerSocket serverSocket = new ServerSocket(cport)) {
            // the for keeps the connection open
            for(;;){
                try{
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
                }catch(Exception e){System.out.println("error "+e);}
            }
        }catch(Exception e){System.out.println("error "+e);}
    }

    public void handleNewConnection (Socket connection) throws IOException {

        // create
        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream())
        );
        String line;

        // listens until decide who made the connection
        while((line = in.readLine()) != null) {

            //System.out.println(line + " received");
            String[] splittedMessage = line.split(" ");

            //if it was a DStore, close this loop
            if (splittedMessage[0].equals("JOIN")) {
                int port = Integer.parseInt(splittedMessage[1]);
                handleDstoreSocket(connection,port);
                break;
            } else {
                // check if there are enough dstores
                if(dstoreCounter >= R) {
                    //if it was a Client close this loop
                    if (receivedMessage(splittedMessage).equals("CLIENT")){
                        // just wait
                        communicator.sleep();
                        receiveClientMessage(splittedMessage);
                        handleClientSocket(connection);
                        break;
                    }
                } else {
                    System.out.println("NOT ENOUGHT DSTORES");
                    communicator.sendMessage(connection,"NOT ENOUGHT DSTORES");
                }
            }
            //System.out.println(communicator.receiveMessage(line));
        }
    }

    public String receivedMessage (String[] splittedMessage) throws IOException {

        return switch (splittedMessage[0]) {
            case "STORE", "REMOVE", "LOAD", "LIST" -> ("CLIENT");
            default -> ("NOT CLIENT");
        };

    }


    public void receiveClientMessage (String[] splittedMessage) throws IOException {

//        String[] splittedMessage;
//        splittedMessage = rMessage.split(" ");
        // System.out.println("comes here");

        switch (splittedMessage[0]) {

            case "STORE":
                index.store(splittedMessage[1], "store in progress");
            case "LOAD":

            case "LIST":

            case "REMOVE":

            default:
                //return ("Error, it parssed : " + splittedMessage[0]);
        }

    }

    public void handleClientSocket(Socket connection) throws IOException {

        String storeToList = "";
        for (int i = 0; i < dstoreCounter; i++) {
            if(i == dstoreCounter - 1) {
                storeToList = storeToList + dstorePortsList.get(i);
            } else {
                storeToList = storeToList + dstorePortsList.get(i) + " ";
            }
        }
        communicator.sendMessage(connection,Protocol.STORE_TO_TOKEN + " " + storeToList);

    }

    public void handleDstoreSocket(Socket connection,Integer port) throws IOException {

        System.out.println(port + " joined");
        dstorePortsList.add(port);
        //System.out.println(dstorePortsList.get(0));
        dstoreCounter ++ ;

        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.getInputStream()));
        String line;
        while((line = in.readLine()) != null) {
            System.out.println(line + " is the second message received");
        }
        dstoreCounter -- ;
        System.out.println(port + " closed dstore");
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
