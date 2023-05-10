import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {

    // to not pass the argument in multiple functions, I just keep R as class variable
    private final int R;

    // timeout will be the same everywhere
    private final int timeout;

    // communicator send messages and display on terminal the received ones
    private final Communicator communicator;

    // keep track of file progress
    private ConcurrentHashMap<String,String> index;

    // keep ports of connected dstores
    private ArrayList<Integer> dstorePortsList;

    // keep all the filesizes
    private ConcurrentHashMap<String,String> fileSizeMap;

    // keep track of which dstore is each file
    private ConcurrentHashMap<String,ArrayList<Integer>> fileDistributionInDstoresMap;

    // keep the number of running threads
    private CountDownLatch storeLatch;
    private CountDownLatch removeLatch ;

    // keep a list with all the dstore connections
    private ArrayList<Socket> dstoresConnectionsList;


    // TODO : set timeouts everywhere

    public Controller (int cport, int R, int timeout, int rebalance_period) {

        this.R = R;
        this.timeout = timeout;

        this.communicator = new Communicator();

        this.index = new ConcurrentHashMap<>();
        this.fileSizeMap = new ConcurrentHashMap<>();
        this.dstorePortsList = new ArrayList<>();
        this.fileDistributionInDstoresMap = new ConcurrentHashMap<>();
        this.dstoresConnectionsList = new ArrayList<>();

        openServerSocket(cport);

    }

    public void openServerSocket(int cport) {

        System.out.println("*** CONTROLLER STARTED ***");
        // the Controller should always listen for new connections
        System.out.println("-> waiting for " + R + " dstores to join");
        for(;;){

            // listen to new connections forever
            try (ServerSocket serverSocket = new ServerSocket(cport)) {
                Socket connection = serverSocket.accept();
                // TODO : this might be helpful for more clients
                new Thread(() -> {
                    try {
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(connection.getInputStream())
                        );
                        handleNewConnection(connection,in);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
                // give a second to the dstore to connect
            } catch(Exception e)  {
                System.out.println("error in openServerSocket : " + e);
            }
        }

    }

    public void handleNewConnection(Socket connection,BufferedReader in) throws IOException, InterruptedException {
        String line;
        // start listening from one connection
        while((line = in.readLine()) != null) {
            communicator.displayReceivedMessage(connection,line);
            String[] splittedMessage = line.split(" ");
            if (splittedMessage[0].equals(Protocol.JOIN_TOKEN)) {
                Integer port = Integer.parseInt(splittedMessage[1]);
                dstorePortsList.add(port);
                // communicator.displayConnectionMessage(connection,line);
                System.out.println("-> [" + port + "] JOINED");
                if (dstorePortsList.size() >= R ) {
                    System.out.println("*** CLIENT REQUESTS OPEN ***");
                }
                new Thread(() -> {
                    try {
                        dstoresConnectionsList.add(connection);
                        // continue listening to the dstore messages
                        handleDstoreConnection(connection,port);

                        // handle dstore disconnection
                        System.out.println("-> [" + port + "] DISCONNECTED");
                        dstorePortsList.remove(port);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
                // stop listening to other messages from same connection
                break;
            } else {
                if(dstorePortsList.size() >= R) {
                    if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")){
                        storeLatch = new CountDownLatch(dstorePortsList.size());
                        removeLatch = new CountDownLatch(dstorePortsList.size());

                        new Thread(() -> {
                            try {
                                handleClientMessage(splittedMessage, connection);
                            } catch (IOException | InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }).start();
                        // stop listening to other messages from same connection
                        // break;
                    }
                } else if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")) {
                    System.out.println("-> ERROR : not enough dstores connected");
                    communicator.sendMessage(connection, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    connection.close();
                }
                System.out.println("-> CLIENT DISCONNECTED");
            }
        }
    }

    public void handleClientMessage(String[] splittedMessage, Socket connection) throws IOException, InterruptedException {

        switch (splittedMessage[0]) {

            case Protocol.STORE_TOKEN -> {
                if (index.containsKey(splittedMessage[1])) {
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    index.put(splittedMessage[1], "STORE_IN_PROGRESS");
                    fileSizeMap.put(splittedMessage[1],splittedMessage[2]);
                    handleStore(connection);
                    if (storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {

                        communicator.sendMessage(connection, Protocol.STORE_COMPLETE_TOKEN);
                    } else {
                        // TODO : remove all the file from index
                    }
                }
            }

            case Protocol.LOAD_TOKEN -> {
                // TODO : Handle ERROR_LOAD
                String filename = splittedMessage[1];
                // check if the file is in any dstore
                if (fileDistributionInDstoresMap.containsKey(filename)) {
                    ArrayList<Integer> dstoresFileList = fileDistributionInDstoresMap.get(filename);
                    //communicator.sendMessage(connection,Protocol.LOAD_FROM_TOKEN+ " " + dstoresFileList.get(0) + " " + fileSizeMap.get(filename));
                    communicator.sendMessage(connection, Protocol.LOAD_FROM_TOKEN+ " " + dstoresFileList.get(0) + " " + fileSizeMap.get(filename));

                } else {
                    // TODO : what if the index is different from where the files are stored
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }

            case Protocol.RELOAD_TOKEN -> {
                // TODO : listen for RELOAD if loading from previous dstore fails
                // TODO : send back the new port of another dstore

            }

            case Protocol.REMOVE_TOKEN -> {
                if (index.containsKey(splittedMessage[1])) {
                    for (Socket eachDstoreConnection : dstoresConnectionsList) {
                        communicator.sendMessage(eachDstoreConnection,Protocol.REMOVE_TOKEN + " " + splittedMessage[1]);
                    }
                    if (removeLatch.await(timeout,TimeUnit.MILLISECONDS)) {
                        communicator.sendMessage(connection,Protocol.REMOVE_COMPLETE_TOKEN);

                    } else {
                        // TODO : check the else branch
                        System.out.println("-> ERROR : timeout");
                    }
                } else {
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }

            case Protocol.LIST_TOKEN -> {
                // TODO : send to the client a message with all the filenames from index
            }

            default -> {
                System.out.println("couldn't handle client message in ClientHandle method");
            }
        }

    }

    // send the message to the client where to store the files "STORE_TO ..."
    public void handleStore(Socket connection) throws IOException {
        // TODO: need to rechange the way Controller picks the Dstores where the file will be stored
        StringBuilder storeToList = new StringBuilder();
        for (int i = 0; i < dstorePortsList.size(); i++) {
            if(i == dstorePortsList.size() - 1) {
                storeToList.append(dstorePortsList.get(i));
            } else {
                storeToList.append(dstorePortsList.get(i)).append(" ");
            }
        }
        communicator.sendMessage(connection, Protocol.STORE_TO_TOKEN + " " + storeToList);
    }


    public void handleDstoreConnection(Socket connection, int port) throws IOException {

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            String line;
            while ((line = in.readLine()) != null) {
                communicator.displayReceivedMessage(connection,line);

                switch (line.split(" ")[0]) {

                    case Protocol.STORE_ACK_TOKEN ->  {

//                        // once all ACK message is received , we know for sure that the stored is completed so we modifie the index
//                        index.put(line.split(" ")[1],"STORE_COMPLETED");
//                        // TODO delete fileSize from index if the ack token wasnt received
//                        // edit the dstores list by : find the value, get the value ,change the value, and put back teh value
//                        // if the arraylist in the map is empty , put one , otherwise complete it
//                        if ( fileDistributionInDstoresMap.get(line.split(" ")[1]) == null) {
//                            ArrayList<Integer> dstoresList = new ArrayList<>();
//                            dstoresList.add(port);
//                            fileDistributionInDstoresMap.put(line.split(" ")[1],dstoresList);
//                        } else {
//                            ArrayList<Integer> dstoresList = fileDistributionInDstoresMap.get(line.split(" ")[1]);
//                            dstoresList.add(port);
//                            fileDistributionInDstoresMap.put(line.split(" ")[1],dstoresList);
//                        }

                        storeLatch.countDown();

                    }

                    // Dstore do not communicate with controller in LOAD operation

                    case Protocol.REMOVE_ACK_TOKEN ->  {
                        // TODO : I received only one dstore remove_ack
                        // TODO : I need to keep count of how many of this I receive
                        // TODO : remove the file from index
                        removeLatch.countDown();
                    }

                    // Dstore do not communicate with controller in LIST operation

                    default -> {
                        // TODO : if you timeout
                        // TODO : remove the file from store in progress / check the specs if you need to
                        System.out.println(line.split(" ")[0]);
                        System.out.println("I didn't received the ACK message");
                    }
                }
            }
        } catch (SocketTimeoutException e) {
            System.out.println("Timeout expired in handleDstoreConnection");
        } catch (IOException e) {
            System.out.println("Error produced in listening if the file has been stored");
            throw new RuntimeException(e);
        }

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
            e.printStackTrace();
            System.out.println("Unable to create Controller.");
        }
    }
}
