import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.rmi.server.ExportException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Controller {

    // to not pass the argument in multiple functions, I just keep R as class variable
    private final int R;

    // timeout will be the same everywhere
    private final int timeout;

    // communicator send messages and display on terminal the received ones
    private final Communicator communicator;


    // keep ports of connected dstores
    private ArrayList<Integer> dstorePortsList;
    // apparently the dstores sockets to the controller are different from server sockets, so I need to keep track of the real sockets and the ones given by me
    private ConcurrentHashMap<Integer, Integer> dstoresPortsEquivalent;

    // keep a list with all the dstore connections
    private ArrayList<Socket> dstoresConnectionsList;

    // keep track of file progress
    private ConcurrentHashMap<String, String> index;
    // private Map<String, String> index;

    // keep track how many reload tries were requested for one file
    private ConcurrentHashMap<String, Integer> reloadTries;
    // private Map<String, String> reloadTries;

    // keep all the filesize
    private ConcurrentHashMap<String, String> fileSizeMap;

    // keep track of which dstore is each file
    private ConcurrentHashMap<String, ArrayList<Integer>> fileDistributionInDstoresMap;



    private ConcurrentHashMap<String,CountDownLatch> storeLatchMap;
    private ConcurrentHashMap<String,CountDownLatch> removeLatchMap;
    // keep the number of running threads
    // private CountDownLatch storeLatch;
    // private CountDownLatch removeLatch;


    // TODO : put a timeout into rebalance

    public Controller(int cport, int R, int timeout, int rebalance_period) {

        this.R = R;
        this.timeout = timeout;

        this.communicator = new Communicator();

        this.index = new ConcurrentHashMap<>();

        this.fileSizeMap = new ConcurrentHashMap<>();
        this.fileDistributionInDstoresMap = new ConcurrentHashMap<>();
        this.dstoresPortsEquivalent = new ConcurrentHashMap<>();
        this.reloadTries = new ConcurrentHashMap<>();

        this.dstorePortsList = new ArrayList<>();
        this.dstoresConnectionsList = new ArrayList<>();

        this.storeLatchMap = new ConcurrentHashMap<>();
        this.removeLatchMap = new ConcurrentHashMap<>();

        openServerSocket(cport);

    }


    ///    NEW CONNECTION  ////

    public void openServerSocket(int cport) {

        System.out.println("*** CONTROLLER STARTED ***");
        // the Controller should always listen for new connections
        System.out.println("-> WAITING " + R + " DSTORES");
        try (ServerSocket serverSocket = new ServerSocket(cport)) {
            for (;;) {
                Socket connection = serverSocket.accept();
                new Thread(() -> {
                    handleNewConnection(connection);
                }).start();
            }
        } catch (Exception e) {
            System.out.println("error in openServerSocket : " + e);
            e.printStackTrace();
        }
    }

    public void handleNewConnection(Socket connection) {
        BufferedReader in;
        try {
            in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
        } catch (IOException e) {
            System.out.println("ERROR : CAN'T CREATE BUFFER READER IN NEWCONNECTION METHOD");
            e.printStackTrace();
            return;
        }
        String line;

        try {
            newConnection: while ((line = in.readLine()) != null) {
                System.out.println("Received request: " + line);

                String[] splittedMessage = line.split(" ");

                // keep in mind the connections should continue listening in each case
                switch (splittedMessage[0]) {

                    case (Protocol.JOIN_TOKEN) -> {
                        communicator.displayReceivedMessageInController(connection, Integer.parseInt(splittedMessage[1]), line);
                        connectDstore(connection, splittedMessage);
                        break newConnection;
                    }

                    case (Protocol.LIST_TOKEN) -> {
                        communicator.displayReceivedMessageInController(connection, line);
                        handleClientConnection(splittedMessage, connection);
                    }

                    case (Protocol.STORE_TOKEN), (Protocol.LOAD_TOKEN), (Protocol.REMOVE_TOKEN), (Protocol.RELOAD_TOKEN) -> {
                        communicator.displayReceivedMessageInController(connection, line);
                        if (dstorePortsList.size() >= R) {
                            handleClientConnection(splittedMessage, connection);
                        } else {
                            System.out.println("-> ERROR : NOT_ENOUGH_DSTORES");
                            communicator.sendMessage(connection, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                            // CONNECTION SHOULD NOT BE CLOSED
                            // connection.close();
                            System.out.println("-> CLIENT [" + connection.getPort() + "] DISCONNECTED");
                        }
                    }

                    default -> {
                        // communicator.displayReceivedMessage(connection,line);
                        System.out.println("-> UNKNOWN MESSAGE RECEIVED FROM [" + connection.getPort() + "] : " + line);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("-> NEW CONNECTION DROPPED UNEXPECTEBLY");
        }
    }


    ///    CLIENT  ////


    public void handleClientConnection(String[] splittedMessage, Socket connection) {
        switch (splittedMessage[0]) {

            case Protocol.STORE_TOKEN -> {
                if (Objects.equals(index.get(splittedMessage[1]), "REMOVE_IN_PROGRESS") || Objects.equals(index.get(splittedMessage[1]), "STORE_COMPLETED") || Objects.equals(index.get(splittedMessage[1]), "STORE_IN_PROGRESS")) {
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    storeLatchMap.put(splittedMessage[1],new CountDownLatch(R));
                    index.put(splittedMessage[1], "STORE_IN_PROGRESS");
                    // handle store in another method
                    // TODO get rid of that try
                    handleStore(connection, splittedMessage);
                    // once all ACK messages are received , we know for sure that the stored is completed ,so we modify the index
                    try {
                        if (storeLatchMap.get(splittedMessage[1]).await(timeout, TimeUnit.MILLISECONDS)) {
                            //&& fileDistributionInDstoresMap.get(splittedMessage[1]).size() == R
                            index.put(splittedMessage[1], "STORE_COMPLETED");
                            fileSizeMap.put(splittedMessage[1], splittedMessage[2]);
                            communicator.sendMessage(connection, Protocol.STORE_COMPLETE_TOKEN);
                        } else {
                            System.out.println("-> ERROR SYSTEM TIMEOUT : WAITING ALL ACK FROM DSTORES");
                            index.remove(splittedMessage[1]);
                            // TODO : what if the file don't exist as a key to be removed
                            try {
                                fileDistributionInDstoresMap.remove(splittedMessage[1]);
                            } catch(Exception e) {
                                System.out.println("ERROR : CAN'T REMOVE THE DSTORES MAP INTO HANDLE STORE REQUEST");
                            }
                        }
                    } catch (InterruptedException e) {
                        System.out.println("ERROR : STORE_LATCH_MAP PRODUCES ERROR ");
                        e.printStackTrace();
                    }
                }
            }

            case Protocol.LOAD_TOKEN -> {
                handleLoad(connection, splittedMessage);
            }

            case Protocol.RELOAD_TOKEN -> {
                handleReload(connection, splittedMessage);
            }

            case Protocol.REMOVE_TOKEN -> {
                if (index.containsKey(splittedMessage[1]) && ( !Objects.equals(index.get(splittedMessage[1]), "REMOVE_IN_PROGRESS") || index.get(splittedMessage[1]) == null || Objects.equals(index.get(splittedMessage[1]), "")) ) {
                    removeLatchMap.put(splittedMessage[1],new CountDownLatch(R));
                    index.put(splittedMessage[1], "REMOVE_IN_PROGRESS");
                    for (Socket eachDstoreConnection : dstoresConnectionsList) {
                        // send only to the connections that contains the files
                        if (fileDistributionInDstoresMap.get(splittedMessage[1]).contains(dstoresPortsEquivalent.get(eachDstoreConnection.getPort()))) {
                            communicator.sendMessage(eachDstoreConnection, Protocol.REMOVE_TOKEN + " " + splittedMessage[1]);
                        }
                    }
                    try {
                        if (removeLatchMap.get(splittedMessage[1]).await(timeout, TimeUnit.MILLISECONDS)) {
                                //&& fileDistributionInDstoresMap.get(splittedMessage[1]).size() == 0)
                            // TODO : should I put "remove_competed" ?
                            index.remove(splittedMessage[1]);
                            fileSizeMap.remove(splittedMessage[1]);
                            fileDistributionInDstoresMap.remove(splittedMessage[1]);
                            communicator.sendMessage(connection, Protocol.REMOVE_COMPLETE_TOKEN);
                            communicator.displaySentMessage(connection, "LIST SENT");
                        } else {
                            // TODO : check the else branch
                            System.out.println("-> ERROR TIMEOUT : THE FILE WAS NOT REMOVED SUCCESFULLY");
                        }
                    } catch (InterruptedException e) {
                        System.out.println("ERROR : ERROR : CAN'T REMOVE THE DSTORES MAP INTO HANDLE REMOVE REQUEST");
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("-> ERROR : FILE DOES NOT EXIST");
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }

            case Protocol.LIST_TOKEN -> {

                String fileList = "";

                for ( String key : fileDistributionInDstoresMap.keySet() ) {
                    if (Objects.equals(fileList, "")) {
                        fileList = key;
                    } else {
                        fileList = key + " " + fileList;
                    }
                }
                communicator.sendMessage(connection,Protocol.LIST_TOKEN + " " + fileList);
                System.out.println("-> LIST SENT TO [" + connection.getPort() + "]" );

            }

            default -> {
                System.out.println("couldn't handle client message in ClientHandle method");
            }
        }

    }

    // send the message to the client where to store the files "STORE_TO ..."
    public void handleStore(Socket connection, String[] splittedMessage) {

        // TODO: need to change the way Controller picks the Dstores where the file will be stored
        StringBuilder storeToList = new StringBuilder();
        for (int i = 0; i < dstorePortsList.size(); i++) {
            if (i == dstorePortsList.size() - 1) {
                storeToList.append(dstorePortsList.get(i));
            } else {
                storeToList.append(dstorePortsList.get(i)).append(" ");
            }
        }
        this.fileDistributionInDstoresMap.putIfAbsent(splittedMessage[1], new ArrayList<>());
        communicator.sendMessage(connection, Protocol.STORE_TO_TOKEN + " " + storeToList);

    }

    public void handleLoad(Socket connection, String[] splittedMessage) {
        String filename = splittedMessage[1];
        // check if the file is in any dstore
        if (fileDistributionInDstoresMap.containsKey(filename)) {
            ArrayList<Integer> dstoresFileList = fileDistributionInDstoresMap.get(filename);
            reloadTries.put(filename, 0);
            int port = dstoresFileList.get(0);
            communicator.sendMessage(connection, Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSizeMap.get(filename));
        } else {
            System.out.println("ERROR : LOAD");
            communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }
    }

    public void handleReload(Socket connection, String[] splittedMessage) {
        String filename = splittedMessage[1];
        // check if the file is in any dstore
        if (fileDistributionInDstoresMap.containsKey(filename)) {
            int counter = reloadTries.get(filename) + 1;
            reloadTries.put(filename, counter);
            ArrayList<Integer> dstoresFileList = fileDistributionInDstoresMap.get(filename);
            if (reloadTries.get(filename) < dstoresFileList.size()) {
                int port = dstoresFileList.get(reloadTries.get(filename));
                communicator.sendMessage(connection, Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSizeMap.get(filename));
            } else {
                System.out.println("ERROR : RELOAD");
                communicator.sendMessage(connection, Protocol.ERROR_LOAD_TOKEN);
            }
        } else {
            // TODO : what if the index is different from where the files are stored
            // communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            System.out.println("-> ERROR : FILE DOES NOT EXIST AFTER RELOAD REQUEST");
            System.out.println("** THIS SHOULDN'T HAPPEN **");
        }
    }


    ///    DSTORES  ////


    private void connectDstore(Socket connection, String[] splittedMessage) {

        // store the port
        Integer port = Integer.parseInt(splittedMessage[1]);
        dstorePortsList.add(port);
        // dstores send messages on a random port, so keep track of the socket port and the random port
        // map the real port with the cw spec PORT
        dstoresPortsEquivalent.put(connection.getPort(), port);
        System.out.println("-> [" + port + "] JOINED");
        if (dstorePortsList.size() >= R) {
            System.out.println("*** CLIENT REQUESTS OPEN ***");
        } else {
            System.out.println("*** CLIENT CONNECTION CLOSED ***");
        }

        dstoresConnectionsList.add(connection);

        // continue listening to the dstore messages
        handleDstoreConnection(connection);

        // handle dstore disconnection
        dstoresPortsEquivalent.remove(connection.getPort());
        dstorePortsList.remove(port);
        System.out.println("-> [" + port + "] DISCONNECTED");
        System.out.println("-> " + dstorePortsList.size() + " DSTORES REMAINED CONNECTED");
        // TODO : handle all the lost files
    }


    public void handleDstoreConnection(Socket connection) {

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            String line;
            // continue listening to new dstore messages
            while ((line = in.readLine()) != null) {

                communicator.displayReceivedMessageInController(connection, dstoresPortsEquivalent.get(connection.getPort()), line);

                switch (line.split(" ")[0]) {
                    case Protocol.STORE_ACK_TOKEN -> {
                        // edit the dstores list by : find the value, get the value ,change the value, and put back teh value
                        // if the arraylist in the map is empty , put one , otherwise complete it
                        String filename = line.split(" ")[1];
                        this.fileDistributionInDstoresMap.computeIfPresent(filename, (key, value) -> {
                            value.add(dstoresPortsEquivalent.get(connection.getPort()));
                            return value;
                        });

                        System.out.println("-> FILE STORED TO : [" + dstoresPortsEquivalent.get(connection.getPort()) + "]");
                        storeLatchMap.get(filename).countDown();
                    }

                    // Dstore do not communicate with controller in LOAD operation

                    case Protocol.REMOVE_ACK_TOKEN -> {
                        removeLatchMap.get(line.split(" ")[1]).countDown();
                    }

                    // Dstore do not communicate with controller in LIST operation

                    default -> {
                        System.out.println("ERROR : UNKOWN MESSAGE RECEIVED FROM DSTORE : " + line);
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("ERROR : CAN'T LISTEN TO DSTORE");
        }

    }


    ///    MAIN  ////


    public static void main(String[] args) {
        try {
            // get parameters
            int cPort = Integer.parseInt(args[0]);
            int r = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalancePeriod = Integer.parseInt(args[3]);

            // creating the controller
            Controller controller = new Controller(cPort, r, timeout, rebalancePeriod);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to create Controller.");
        }
    }
}
