import javax.naming.ldap.SortKey;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.FileSystemNotFoundException;
import java.util.HashMap;
import java.util.Objects;

public class DStore {

    private int cport;
    private int R;
    private int timeout;
    private String file_folder;
    private ServerSocket dstoreSocket;
    private Socket connection;
    private InetAddress localAddress ;
    private Communicator communicator;
    protected HashMap<String, Integer> fileSizeMap;

    private File fileStore;

    // TODO : need to add the file path in parameter
    public DStore(int port, int cport, int timeout) throws IOException {

        communicator = new Communicator();

        try {
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        //TODO try to close all the Dstores when the controller is closed
//        try (Socket connection = new Socket(localAddress,cport) ) {
//
//            communicator.sendMessage(connection, "JOIN " + port);
//            communicator.listenMessagesThread(connection);
//            System.out.println("JOIN " + port + " send");
//
//            openServerSocket(port);
//
//        } catch (Exception e) {
//            System.out.println("error in constructor controller" + e);
//        }

        connectController(localAddress, cport, port);
        openServerSocket(port);


        fileSizeMap = new HashMap<>();
        var file_folder = "/storagePlace";
        this.file_folder = file_folder;
        //setupFileStore(file_folder);

    }

    public void openServerSocket (int port) {

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // the for keeps the connection open
            for(;;){
                try{
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

    public void connectController (InetAddress localAddress, int cport, int port) {

        Socket connection = null;
        try {
            connection = new Socket(localAddress,cport);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        communicator.sendMessage(connection, "JOIN " + port);
        communicator.listenMessagesThread(connection);
        System.out.println("JOIN " + port + " send");

    }

    public synchronized void handleNewConnection (Socket connection) throws IOException {

        storeFile(connection);

        if (!connection.isClosed()) {
            System.out.println("connected");
        } else {
            System.out.println("not connected");
        }

        //listens to other messages
//        BufferedReader in = new BufferedReader(
//                new InputStreamReader(connection.getInputStream())
//        );
//        String line;
//
//        while((line = in.readLine()) != null) {
//            String[] splittedMessage = line.split(" ");
//            if(Objects.equals(receivedMessage(splittedMessage), "CLIENT")){
//                handleClientMessage(connection);
//                break;
//            }
//        }
    }

    private void handleClientMessage(Socket connection) {
        //System.out.println("file name received");
        communicator.sendMessage(connection,"ACK");
    }

    public String receivedMessage (String[] splittedMessage) throws IOException {

        return switch (splittedMessage[0]) {
            case "STORE", "REMOVE", "LOAD", "LIST" -> ("CLIENT");
            default -> ("NOT CLIENT");
        };

    }


    public void setupFileStore(String file_folder){
        // creating file object
        this.fileStore = new File(file_folder);

        // creating the file if it doesn't exist
        if(!this.fileStore.exists()){
            this.fileStore.mkdir();
        }
    }

    public void storeFile(Socket connection) throws IOException {

        InputStream in = connection.getInputStream();
        byte[] buf = new byte[1000]; int buflen;
        buflen = in.read(buf);
        String firstBuffer = new String(buf,0,buflen);
        int firstSpace = firstBuffer.indexOf(" ");
        String command = firstBuffer.substring(0,firstSpace);
        System.out.println("command "+command);
        if(command.equals("STORE")){
            int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
            String fileName=
                    firstBuffer.substring(firstSpace+1,secondSpace);
            System.out.println("fileName "+fileName);
            File outputFile = new File(fileName);
            FileOutputStream out = new FileOutputStream(outputFile);
            out.write(buf,secondSpace+1,buflen-secondSpace-1);
            while ((buflen=in.read(buf)) != -1){
                System.out.print("*");
                out.write(buf,0,buflen);
            }
            System.out.println(" hahaha /n");
            // close all the connections
            in.close(); connection.close(); out.close();
        }
    }


    public static void main (String args[]) {

        try{
            // get parameters
            int port = Integer.parseInt(args[0]);
            int cport = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            // int file_folder = String.parseString(args[3]);s

            // creating the dstore
            DStore dstore = new DStore(port, cport, timeout);
        }
        catch(Exception e){
            System.out.println("Unable to create DStore.");
        }
    }

}
