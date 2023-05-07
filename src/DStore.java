import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

public class DStore {

    private int cport;
    private int R;
    private int timeout;
    private String getFile_folder;
    private String file_folder;
    private ServerSocket dstoreSocket;
    private Socket connection;
    private InetAddress localAddress ;
    private Communicator communicator;
    private HashMap<String, Integer> fileSizeMap;

    private File fileStore;

    // TODO : need to add the file path in parameter
    public DStore(int port, int cport, int timeout, String file_folder) throws IOException {

        communicator = new Communicator();

        try {
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        //TODO try to close all the Dstores when the controller is closed

        connectController(localAddress, cport, port);
        openServerSocket(port);

        fileSizeMap = new HashMap<>();
        this.file_folder = file_folder;
        setupFileStore(file_folder);

    }

    public void connectController (InetAddress localAddress, int cport, int port) {
        try {
            this.connection = new Socket(localAddress,cport);
            communicator.sendMessage(connection, "JOIN " + port);
            communicator.listenAndDisplayToTerminal(connection);
            System.out.println("JOIN " + port + " send");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    // TODO rewrite this class
    public synchronized void handleNewConnection (Socket connection) throws IOException {

        //communicator.listenAndDisplayToTerminal(connection);
        // TODO check what kind of message is received and from who and treat it appropriately



        // TODO receive the STORE filename filesize command
        // then send this   communicator.sendMessage(connection,Protocol.ACK_TOKEN); back to the client
        // then actually store the file
             // then send to Controller a message back that the file is stored

        // NOTE : after all this the controller send a message to the client as well



        String receivedMessage = storeFile(connection);

        if(receivedMessage.equals(Protocol.STORE_TOKEN)) {
            System.out.println("STORED COMPLETED");
        } else {
            switch (receivedMessage) {
                case Protocol.LOAD_TOKEN :

                default :
                    System.out.println("Unknown message");
            }
        }

    }



    // TODO rewrite this class
    public String storeFile(Socket connection) throws IOException {

        InputStream in = connection.getInputStream();
        byte[] buf = new byte[1000]; int buflen;
        buflen = in.read(buf);
        String firstBuffer = new String(buf,0,buflen);
        int firstSpace = firstBuffer.indexOf(" ");
        String command = firstBuffer.substring(0,firstSpace);
        System.out.println("message starts with " + command);
        if(command.equals(Protocol.STORE_TOKEN)){
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
            // close all the connections
            in.close(); connection.close(); out.close();
            return Protocol.STORE_TOKEN;
        } else {
            // TODO delete this
            return command;
        }
    }


    public void setupFileStore(String file_folder){
        // creating file object
        this.fileStore = new File(file_folder);

        // creating the file if it doesn't exist
        if(!this.fileStore.exists()){
            this.fileStore.mkdir();
        }
    }


    public static void main (String args[]) {

        try{
            // get parameters
            int port = Integer.parseInt(args[0]);
            int cport = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            String file_folder = args[3];

            // creating the dstore
            DStore dstore = new DStore(port, cport, timeout, file_folder);
        }
        catch(Exception e){
            System.out.println("Unable to create DStore.");
        }
    }

}
