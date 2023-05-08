import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class DStore {

    private int cport;
    private int R;
    private int timeout;
    private String getFile_folder;
    private String file_folder;
    private ServerSocket dstoreSocket;
    // it is always only one controller connection ,so we don't mind having it as a class variable
    private Socket controllerConnection;
    private Socket clientConnection;
    private InetAddress localAddress ;
    private Communicator communicator;

    private File fileFolder;

    public DStore(int port, int cport, int timeout, String file_folder) throws IOException {

        this.file_folder = file_folder;
        setupFileStore(file_folder);

        communicator = new Communicator();

        try {
            localAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        // TODO : connect controller in a new thread because the controller will send messages to the Dstore
        connectController(localAddress, cport, port);
        openServerSocketForClient(port);

    }

    public void connectController (InetAddress localAddress, int cport, int port) {
        try {
            this.controllerConnection = new Socket(localAddress,cport);
            communicator.sendMessage(controllerConnection, Protocol.JOIN_TOKEN + " " + port);
            communicator.listenAndDisplayToTerminal(controllerConnection);
            System.out.println(Protocol.JOIN_TOKEN + " " + port + " send");
            // TODO : listen to controller messages
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void openServerSocketForClient(int port) {

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // permanently listen for new clients
            for(;;){
                try{
                    Socket clientConnection = serverSocket.accept();
                    // every client is in a new thread
                    new Thread(() -> {
                        try {
                            this.handleClientConnection(clientConnection);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                }catch(Exception e){System.out.println("error "+e);}
            }

        }catch(Exception e){System.out.println("error "+e);}
        // keep in mind that it opened a client connection in a new thread and that's it.
        // the main tread is now finished
    }

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

                case Protocol.STORE_TOKEN -> {
                    communicator.sendMessage(clientConnection,Protocol.ACK_TOKEN);
                    storeFile(clientConnection,fileFolder + "/" + splittedMessage[1]);
                    // notice that the connection to the controller is done in way before and stored in a class variable
                    communicator.sendMessage(controllerConnection,Protocol.STORE_ACK_TOKEN + " " + splittedMessage[1]);
                }

                case Protocol.LOAD_DATA_TOKEN -> {
                    loadFile(clientConnection,fileFolder + "/" + splittedMessage[1]);
                    // TODO : listen for RELOAD if loading from previous dstore fails
                }

                case Protocol.REMOVE_TOKEN -> {

                }

                case Protocol.LIST_TOKEN -> {

                }

                default -> {
                    System.out.println("no switch case covered in handled new clientConnection");
                }
            }
            // TODO : is this necessary ?
            break;
        }

    }


    public void storeFile(Socket connection,String filename) throws IOException {

        InputStream in = connection.getInputStream();
        byte[] buf = new byte[1000]; int buflen;
        buflen = in.read(buf);
        //String firstBuffer = new String(buf,0,buflen);
        //int firstSpace = firstBuffer.indexOf(" ");
        //String command = firstBuffer.substring(0,firstSpace);
        //System.out.println("message starts with " + command);
        //if(command.equals(Protocol.STORE_TOKEN)) {
            //int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
        //String filename =
                //firstBuffer.substring(firstSpace + 1, secondSpace);
        System.out.println("filename with path is : " + filename);
        File outputFile = new File(filename);
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

    private void loadFile(Socket connection,String filename) throws IOException {

        InputStream in = connection.getInputStream();
        byte[] buf = new byte[1000]; int buflen;
        buflen = in.read(buf);

        System.out.println("filename with path is : " + filename);
        File inputFile = new File(filename);
        FileInputStream inf = new FileInputStream(inputFile);
        OutputStream out = connection.getOutputStream();
        while ((buflen = inf.read(buf)) != -1){
            System.out.print("*");
            out.write(buf,0,buflen);
        }
        in.close(); inf.close(); connection.close(); out.close();
    }


    public void setupFileStore(String file_folder){
        // creating folder
        this.fileFolder = new File(file_folder);

        // creating the folder if it doesn't exist
        if(!this.fileFolder.exists()){
            this.fileFolder.mkdir();
        } else {
            File[] files = fileFolder.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
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
