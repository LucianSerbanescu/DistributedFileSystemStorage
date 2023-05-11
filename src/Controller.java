import javax.sound.midi.InvalidMidiDataException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Arrays;
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


    // keep ports of connected dstores
    private ArrayList<Integer> dstorePortsList;

    // keep a list with all the dstore connections
    private ArrayList<Socket> dstoresConnectionsList;

    // keep track of file progress
    private ConcurrentHashMap<String,String> index;

    // keep all the filesizes
    private ConcurrentHashMap<String,String> fileSizeMap;

    // keep track of which dstore is each file
    private ConcurrentHashMap<String,ArrayList<Integer>> fileDistributionInDstoresMap;

    // apparently the dstores sockets to the controller are different from server sockets, so I need to keep track of the real sockets and the ones given by me
    private ConcurrentHashMap<Integer,Integer> dstoresPortsEquivalent;

    // keep the number of running threads
    private CountDownLatch storeLatch;
    private CountDownLatch removeLatch ;

    // keep track of how many times it tried to reaload
    private int reloadCounter;

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
        this.dstoresPortsEquivalent = new ConcurrentHashMap<>();

        openServerSocket(cport);

    }

    public void openServerSocket(int cport) {

        System.out.println("*** CONTROLLER STARTED ***");
        // the Controller should always listen for new connections
        System.out.println("-> WAITING " + R + " DSTORES");
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
                        System.out.println("-> ERROR : IN OPEN SERVERSOCKET METHOD");
                        throw new RuntimeException(e);
                    }
                }).start();
            } catch(Exception e)  {
                System.out.println("error in openServerSocket : " + e);
            }
        }

    }

    public void handleNewConnection(Socket connection,BufferedReader in) throws IOException, InterruptedException {
        String line;
        // listen new connection first message
        while((line = in.readLine()) != null) {
            String[] splittedMessage = line.split(" ");

            switch (splittedMessage[0]) {
                case (Protocol.JOIN_TOKEN) -> {
                    handleDstoreConnection();
                }

                case ( Protocol.LIST_TOKEN) -> {
                    handleClientConnection();
                }

                case (Protocol.STORE_TOKEN) -> {
                    if() {

                    }
                }

                case (Protocol.LOAD_TOKEN) -> {
                    if() {

                    }
                }
                case (Protocol.REMOVE_TOKEN) -> {
                    if() {

                    }
                }

                default -> {

                }
            }

            }
    }

//    public void handleNewConnection(Socket connection,BufferedReader in) throws IOException, InterruptedException {
//        String line;
//        // start listening from one connection
//        while((line = in.readLine()) != null) {
//            String[] splittedMessage = line.split(" ");
//            if (splittedMessage[0].equals(Protocol.JOIN_TOKEN)) {
//                Integer port = Integer.parseInt(splittedMessage[1]);
//                dstorePortsList.add(port);
//                dstoresPortsEquivalent.put(connection.getPort(),port);
//                communicator.displayReceivedMessage(connection,port,line);
//                // communicator.displayConnectionMessage(connection,line);
//                System.out.println("-> [" + port + "] JOINED");
//                if (dstorePortsList.size() >= R ) {
//                    System.out.println("*** CLIENT REQUESTS OPEN ***");
//                }
//                new Thread(() -> {
//                    try {
//                        dstoresConnectionsList.add(connection);
//                        // continue listening to the dstore messages
//                        handleDstoreConnection(connection);
//
//                        // handle dstore disconnection
//                        dstoresPortsEquivalent.remove(connection.getPort());
//                        System.out.println("-> [" + port + "] DISCONNECTED");
//                        dstorePortsList.remove(port);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }).start();
//                // stop listening to other messages from same connection
//                break;
//            } else {
//                communicator.displayReceivedMessage(connection,line);
//                if(dstorePortsList.size() >= R) {
//                    if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")){
//                        storeLatch = new CountDownLatch(R);
//                        removeLatch = new CountDownLatch(R);
//
//                        new Thread(() -> {
//                            try {
//                                handleClientMessage(splittedMessage, connection);
//                            } catch (IOException | InterruptedException e) {
//                                System.out.println("ERRRRRRRRRRRRRRRRRRRRRROOOOOOORRRRRRRRRR");
//                                throw new RuntimeException(e);
//                            }
//                        }).start();
//                        // stop listening to other messages from same connection
//                        // break;
//                    }
//                } else if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")) {
//                    System.out.println("-> ERROR : not enough dstores connected");
//                    communicator.sendMessage(connection, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
//                    connection.close();
//                }
//                System.out.println("-> CLIENT DISCONNECTED");
//            }
//        }
//    }

    public void handleClientMessage(String[] splittedMessage, Socket connection) throws IOException, InterruptedException {

        switch (splittedMessage[0]) {

            case Protocol.STORE_TOKEN -> {
                if (index.containsKey(splittedMessage[1])) {
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    index.put(splittedMessage[1], "STORE_IN_PROGRESS");
                    // handle store in another method
                    handleStore(connection,splittedMessage);
                    System.out.println("-> SYSTEM TIMEOUT WAITING ALL ACK FROM DSTORES");
                    // once all ACK messages are received , we know for sure that the stored is completed ,so we modify the index
                    if (storeLatch.await(timeout, TimeUnit.MILLISECONDS) && fileDistributionInDstoresMap.get(splittedMessage[1]).size() == R) {
                        index.put(splittedMessage[1],"STORE_COMPLETED");
                        fileSizeMap.put(splittedMessage[1],splittedMessage[2]);
                        communicator.sendMessage(connection, Protocol.STORE_COMPLETE_TOKEN);
                    } else {
                        System.out.println("-> ERROR TIMEOUT : THE FILE WAS NOT STORED SUCCESSFULLY ");
                        index.remove(splittedMessage[1]);
                        // TODO : what if the file don't exist as a key to be removed
                        fileDistributionInDstoresMap.remove(splittedMessage[1]);
                    }
                }
            }

            case Protocol.LOAD_TOKEN -> {
                reloadCounter = 0;
                handleLoad(connection,splittedMessage);
            }

            case Protocol.RELOAD_TOKEN -> {
                handleReload(connection,splittedMessage);
            }

            case Protocol.REMOVE_TOKEN -> {
                if (index.containsKey(splittedMessage[1])) {

                    for (Socket eachDstoreConnection : dstoresConnectionsList) {
                        // send only to the connections that contains the files
                        if(fileDistributionInDstoresMap.get(splittedMessage[1]).contains(dstoresPortsEquivalent.get(eachDstoreConnection.getPort()))) {
                            communicator.sendMessage(eachDstoreConnection,Protocol.REMOVE_TOKEN + " " + splittedMessage[1]);
                        }
                    }
                    if (removeLatch.await(timeout,TimeUnit.MILLISECONDS)) {
                        index.remove(splittedMessage[1]);
                        fileDistributionInDstoresMap.remove(splittedMessage[1]);
                        fileSizeMap.remove(splittedMessage[1]);
                        communicator.sendMessage(connection,Protocol.REMOVE_COMPLETE_TOKEN);

                    } else {
                        // TODO : check the else branch
                        System.out.println("-> ERROR TIMEOUT : THE FILE WAS NOT REMOVED SUCCESFULLY");
                    }
                } else {
                    System.out.println("-> ERROR : FILE DOES NOT EXIST");
                    communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }

            case Protocol.LIST_TOKEN -> {

                for(Socket eachDstore : dstoresConnectionsList) {
                    eachDstore.close();
                }

                String fileList = "";
                // System.out.println(fileDistributionInDstoresMap.keySet());

                for ( String key : fileDistributionInDstoresMap.keySet() ) {
                    if (fileList == "") {
                        fileList = key;
                    } else {
                        fileList = key + " " + fileList;
                    }
                }

                communicator.sendMessage(connection,Protocol.LIST_TOKEN + " " + fileList);

//                File f = new File ("/Users/macbookpro/Documents/My Programming Projects/DistributedFileSystemStorage/src");
//
//                communicator.sendMessage(connection, Arrays.toString(f.listFiles()));

            }

            default -> {
                System.out.println("couldn't handle client message in ClientHandle method");
            }
        }

    }

    // send the message to the client where to store the files "STORE_TO ..."
    public void handleStore(Socket connection,String[] splittedMessage) throws IOException {

        // TODO: need to change the way Controller picks the Dstores where the file will be stored
        StringBuilder storeToList = new StringBuilder();
        for (int i = 0 ; i < dstorePortsList.size(); i++) {
            if(i == dstorePortsList.size() - 1) {
                storeToList.append(dstorePortsList.get(i));
            } else {
                storeToList.append(dstorePortsList.get(i)).append(" ");
            }
        }
        communicator.sendMessage(connection, Protocol.STORE_TO_TOKEN + " " + storeToList);


    }

    public void handleLoad (Socket connection , String[] splittedMessage) {
        String filename = splittedMessage[1];
        // check if the file is in any dstore
        //System.out.println(fileDistributionInDstoresMap.containsKey(filename));
        if (fileDistributionInDstoresMap.containsKey(filename)) {
            reloadCounter++;
            ArrayList<Integer> dstoresFileList = fileDistributionInDstoresMap.get(filename);
            int pos = dstoresFileList.get(0) + reloadCounter;
            communicator.sendMessage(connection,Protocol.LOAD_FROM_TOKEN+ " " + pos + " " + fileSizeMap.get(filename));
        } else {
            // TODO : what if the index is different from where the files are stored
            communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }
    }

    public void handleReload (Socket connection, String[] splittedMessage) {
        String filename = splittedMessage[1];
        // check if the file is in any dstore
        // System.out.println(fileDistributionInDstoresMap.containsKey(filename));
        if (fileDistributionInDstoresMap.containsKey(filename)) {
            reloadCounter++;
            ArrayList<Integer> dstoresFileList = fileDistributionInDstoresMap.get(filename);
            int pos = dstoresFileList.get(0) + reloadCounter;
            if ( pos < dstoresFileList.size()) {
                communicator.sendMessage(connection,Protocol.LOAD_FROM_TOKEN+ " " + pos + " " + fileSizeMap.get(filename));

            } else {
                communicator.sendMessage(connection,Protocol.ERROR_LOAD_TOKEN);
            }
        } else {
            // TODO : what if the index is different from where the files are stored
            communicator.sendMessage(connection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }
    }


    public void handleDstoreConnection(Socket connection) throws IOException {

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            String line;
            while ((line = in.readLine()) != null) {
                communicator.displayReceivedMessage(connection,dstoresPortsEquivalent.get(connection.getPort()),line);

                switch (line.split(" ")[0]) {

                    case Protocol.STORE_ACK_TOKEN ->  {
                        // edit the dstores list by : find the value, get the value ,change the value, and put back teh value
                        // if the arraylist in the map is empty , put one , otherwise complete it
                        if ( fileDistributionInDstoresMap.get(line.split(" ")[1]) == null) {
                            ArrayList<Integer> dstoresList = new ArrayList<>();
                            dstoresList.add(dstoresPortsEquivalent.get(connection.getPort()));
                            System.out.println("-> FILE STORED TO : [" + dstoresPortsEquivalent.get(connection.getPort()) + "]");
                            fileDistributionInDstoresMap.put(line.split(" ")[1],dstoresList);
                        } else {
                            ArrayList<Integer> dstoresList = fileDistributionInDstoresMap.get(line.split(" ")[1]);
                            dstoresList.add(dstoresPortsEquivalent.get(connection.getPort()));
                            fileDistributionInDstoresMap.put(line.split(" ")[1],dstoresList);
                        }
                        storeLatch.countDown();
                    }

                    // Dstore do not communicate with controller in LOAD operation

                    case Protocol.REMOVE_ACK_TOKEN ->  {

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
