import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Controller {

    // to not pass the argument in multiple functions, I just keep R as class variable
    private final int R;

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
    private CountDownLatch latch;

    public Controller (int cport, int R, int timeout, int rebalance_period) {

        this.R = R;

        latch = new CountDownLatch(R);

        this.communicator = new Communicator();

        this.index = new ConcurrentHashMap<>();
        this.fileSizeMap = new ConcurrentHashMap<>();
        this.dstorePortsList = new ArrayList<>();
        this.fileDistributionInDstoresMap = new ConcurrentHashMap<>();

        openServerSocket(cport);

    }

    public void openServerSocket(int cport) {

        // the Controller should always listen for new connections
        System.out.println("waiting for R dstores to connect");
        for(;;){
            try (ServerSocket serverSocket = new ServerSocket(cport)) {
                Socket connection = serverSocket.accept();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );
                handleNewConnection(connection,in);

            }catch(Exception e){ System.out.println("error in openServerSocket : " + e); }
        }

    }

    public void handleNewConnection(Socket connection,BufferedReader in) throws IOException, InterruptedException {
        String line;
        // start listening
        while((line = in.readLine()) != null) {
            String[] splittedMessage = line.split(" ");
            if (splittedMessage[0].equals(Protocol.JOIN_TOKEN)) {

                Integer port = Integer.parseInt(splittedMessage[1]);
                dstorePortsList.add(port);
                System.out.println(port + " joined");

                new Thread(() -> {
                    try {
                        // continue listening to the dstore messages
                        handleDstoreConnection(connection,port);

                        // handle dstore disconnection
                        System.out.println("Dstore " + port + " disconnected");
                        dstorePortsList.remove(port);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();

                // stop listening to other connections
                break;
            } else {
                if(dstorePortsList.size() >= R) {
                    if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")){
                        handleClientMessage(splittedMessage, connection);
                        break;
                    }
                } else if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")) {
                    communicator.sendMessage(connection,Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    connection.close();
                }
            }
        }
    }

    public void handleClientMessage(String[] splittedMessage, Socket connection) throws IOException, InterruptedException {

        switch (splittedMessage[0]) {

            case Protocol.STORE_TOKEN -> {
                if (index.containsKey(splittedMessage[1])) {
                    communicator.sendMessage(connection,Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    index.put(splittedMessage[1], "STORE_IN_PROGRESS");
                    handleStore(connection);
                    latch.await();
                    communicator.sendMessage(connection,Protocol.STORE_COMPLETE_TOKEN);
                }
            }

            case Protocol.LOAD_TOKEN -> {
                // TODO : Handle ERROR_LOAD
                String filename = splittedMessage[1];
                // check if the file is in any dstore
                if (fileDistributionInDstoresMap.containsKey(filename)) {
                    ArrayList<Integer> dstoresFileList = fileDistributionInDstoresMap.get(filename);
                    communicator.sendMessage(connection,Protocol.LOAD_FROM_TOKEN+ " " + dstoresFileList.get(0) + " " + fileSizeMap.get(filename));

                    // TODO : listen for RELOAD if loading from previous dstore fails
                    // TODO : send back the new port of another dstore

                } else {
                    // TODO : what if the index is different from where the files are stored
                    communicator.sendMessage(connection,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }

            case Protocol.REMOVE_TOKEN -> {
                if (index.containsKey(splittedMessage[1])) {
                    // TODO : send message to ALL dstores that they need to remove the file
                    // TODO : I might want to have a list with all connections and do a for to access all of them
                    //communicator.sendMessage(dstoreconnection, message);

                    // TODO : after I contacted all the dstores, I want to wait for all of them to response then I send REMOVE_COMPLETED
                    //latch.await();
                    //communicator.sendMessage(connection,Protocol.REMOVE_COMPLETE_TOKEN);
                } else {
                    communicator.sendMessage(connection,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            }

            case Protocol.LIST_TOKEN -> {
                // TODO : send to the client a message with all the filenames from index
            }

            default -> {
                System.out.println("couldn't handle client message");
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
        communicator.sendMessage(connection,Protocol.STORE_TO_TOKEN + " " + storeToList);
    }


    public void handleDstoreConnection(Socket connection, int port) throws IOException {

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            String line;
            while ((line = in.readLine()) != null) {
                switch (line.split(" ")[0]) {

                    case Protocol.STORE_ACK_TOKEN ->  {

                        index.put(line.split(" ")[1],"STORE_COMPLETED");
                        fileSizeMap.put(line.split(" ")[1],line.split(" ")[2]);
                        // edit the dstores list by : find the value, get the value ,change the value, and put back teh value
                        ArrayList<Integer> dstoresList = fileDistributionInDstoresMap.get(line.split(" ")[1]);
                        dstoresList.add(port);
                        fileDistributionInDstoresMap.put(line.split(" ")[1],dstoresList);

                        latch.countDown();
                    }

                    // Dstore do not communicate with controller in LOAD operation

                    case Protocol.REMOVE_ACK_TOKEN ->  {
                        // TODO : I received only one dstore remove_ack
                        // TODO : I need to keep count of how many of this I receive
                    }

                    // Dstore do not communicate with controller in LIST operation

                    default -> {
                        // TODO : if you timeout
                        // TODO : remove the file from store in progress / check the specs if you need to
                        System.out.println(line.split(" ")[0]);
                        System.out.println("I didn't received the STORE_ACK message");
                    }
                    }
                }
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
