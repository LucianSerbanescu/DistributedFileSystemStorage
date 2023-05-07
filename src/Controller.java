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

        openServerSocket(cport);

    }

    public void openServerSocket(int cport) {

        System.out.println("waiting for R dstores to connect");
        for(;;){
            try (ServerSocket serverSocket = new ServerSocket(cport)) {
                Socket connection = serverSocket.accept();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );
                String line;
                while((line = in.readLine()) != null) {
                    String[] splittedMessage = line.split(" ");
                    if (splittedMessage[0].equals("JOIN")) {
                        int port = Integer.parseInt(splittedMessage[1]);
                        dstorePortsList.add(port);
                        System.out.println(port + " joined");
                        new Thread(() -> {
                            try {
                                // just listen and keep the connection open
                                handleDstoreConnection(connection);
                                System.out.println("Dstore " + port + " disconnected");
                                dstorePortsList.remove(port);
                            } catch (IOException e) {
                             throw new RuntimeException(e);
                            }
                        }).start();
                        // don't listen because we know it is a dstore
                        break;
                    } else {
                        if(dstorePortsList.size() >= R) {
                            if (communicator.receiveAnyMessage(splittedMessage).equals("CLIENT")){
                                handleClientMessage(splittedMessage, connection);
                                break;
                            }
                        } else {
                            System.out.println("NOT ENOUGHT DSTORES");
                            communicator.sendMessage(connection,"NOT ENOUGHT DSTORES");
                            connection.close();
                        }
                    }
                }

            }catch(Exception e){ System.out.println("error " + e); }
        }

    }

    public void handleClientMessage(String[] splittedMessage, Socket connection) throws IOException, InterruptedException {

        switch (splittedMessage[0]) {

            case "STORE" -> {
                index.storeInProgress(splittedMessage[1]);
                handleStore(connection);
                Thread.sleep(1000);
                checkStoredFinished();
                // TODO need to check if they are actually completed
                communicator.sendMessage(connection,Protocol.STORE_COMPLETE_TOKEN);
            }

            case "LOAD" -> {

            }

            case "LIST" -> {

            }

            case "REMOVE"-> {

            }

            default -> {

            }
        }

    }

    private void checkStoredFinished() {



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
    }


    public void handleDstoreConnection(Socket connection) throws IOException {

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            String line;
            while ((line = in.readLine()) != null) {
                switch (line.split(" ")[0]) {
                    case Protocol.STORE_ACK_TOKEN ->  {
                        index.updateIndexStatus(line.split(" ")[1]);

                    }
                    case Protocol.LOAD_TOKEN ->  {

                    }
                    default -> {
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
