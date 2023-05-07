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
    private Socket controllerConnection;
    private Socket clientConnection;
    private InetAddress localAddress ;
    private Communicator communicator;
    private HashMap<String, Integer> fileSizeMap;

    private File fileFolder;

    // TODO : need to add the file path in parameter
    public DStore(int port, int cport, int timeout, String file_folder) throws IOException {

        fileSizeMap = new HashMap<>();
        this.file_folder = file_folder;
        setupFileStore(file_folder);

        communicator = new Communicator();

        try {
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        //TODO try to close all the Dstores when the controller is closed

        connectController(localAddress, cport, port);
        openServerSocketForClient(port);



    }

    public void connectController (InetAddress localAddress, int cport, int port) {
        try {
            this.controllerConnection = new Socket(localAddress,cport);
            communicator.sendMessage(controllerConnection, "JOIN " + port);
            communicator.listenAndDisplayToTerminal(controllerConnection);
            System.out.println("JOIN " + port + " send");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void openServerSocketForClient(int port) {

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // the for keeps the connection open
            for(;;){
                try{
                    Socket clientConnection = serverSocket.accept();
                    this.clientConnection = clientConnection;
                    new Thread(() -> {
                        try {
                            this.handleClientConnection(clientConnection);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                    //clientConnection.close();
                }catch(Exception e){System.out.println("error "+e);}
            }

        }catch(Exception e){System.out.println("error "+e);}


    }

    // TODO rewrite this class
    public synchronized void handleClientConnection(Socket clientConnection) throws IOException {

        BufferedReader in = new BufferedReader(
                new InputStreamReader(clientConnection.getInputStream())
        );

        // check if the message is from a client
        String line;
        while((line = in.readLine()) != null) {
            System.out.println("message received : " + line);
            String[] splittedMessage = line.split(" ");
            switch (splittedMessage[0]) {
                case Protocol.STORE_TOKEN :
                    fileSizeMap.put(splittedMessage[1], Integer.valueOf(splittedMessage[2]));
                    communicator.sendMessage(clientConnection,Protocol.ACK_TOKEN);
                    storeFile(clientConnection,fileFolder + "/" + splittedMessage[1]);
                    // notice that the connection to the controller is done in way before and stored in a class variable
                    communicator.sendMessage(controllerConnection,Protocol.STORE_ACK_TOKEN + " " + splittedMessage[1]);
                    break;
                case Protocol.LOAD_TOKEN :

                default :
                    System.out.println("no switch case covered in handled new clientConnection");
            }
        }





        //communicator.listenAndDisplayToTerminal(clientConnection);
        // TODO check what kind of message is received and from who and treat it appropriately



        // TODO receive the STORE filename filesize command
        // then send this   communicator.sendMessage(clientConnection,Protocol.ACK_TOKEN); back to the client
        // then actually store the file
             // then send to Controller a message back that the file is stored

        // NOTE : after all this the controller send a message to the client as well


    }



    // TODO rewrite this class
    public void storeFile(Socket connection,String fileName) throws IOException {

        InputStream in = connection.getInputStream();
        byte[] buf = new byte[1000]; int buflen;
        buflen = in.read(buf);
        //String firstBuffer = new String(buf,0,buflen);
        //int firstSpace = firstBuffer.indexOf(" ");
        //String command = firstBuffer.substring(0,firstSpace);
        //System.out.println("message starts with " + command);
        //if(command.equals(Protocol.STORE_TOKEN)) {
            //int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
        //String fileName =
                //firstBuffer.substring(firstSpace + 1, secondSpace);
        System.out.println("fileName with path is : " + fileName);
        File outputFile = new File(fileName);
        FileOutputStream out = new FileOutputStream(outputFile);
        //out.write(buf, secondSpace + 1, buflen - secondSpace - 1);
        while ((buflen = in.read(buf)) != -1) {
            System.out.print("*");
            out.write(buf, 0, buflen);
        }
        // close all the connections
        in.close();
        connection.close();
        out.close();
       // }
    }


    public void setupFileStore(String file_folder){
        // creating file object
        this.fileFolder = new File(file_folder);

        // creating the file if it doesn't exist
        if(!this.fileFolder.exists()){
            this.fileFolder.mkdir();
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
