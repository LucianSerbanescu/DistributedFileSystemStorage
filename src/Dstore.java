import java.io.*;
import java.net.*;

public class Dstore {

    private int cport;
    private int port;
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

    public Dstore(int port, int cport, int timeout, String file_folder) throws IOException {

        this.cport = cport;
        this.port = port;
        this.timeout = timeout;

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

            System.out.println("-> CONNECTION ESTABLISHED TO CONTROLLER [" + controllerConnection.getPort() + "]");
            System.out.println("-> ACTUAL LOCAL PORT DSTORE " + controllerConnection.getLocalPort());
            communicator.sendMessage(controllerConnection, Protocol.JOIN_TOKEN + " " + port);
            // System.out.println(Protocol.JOIN_TOKEN + " " + port + " SENT");
            System.out.println("[" + port + "]" + " -> " + "[" + cport + "] " + Protocol.JOIN_TOKEN);

            new Thread(() -> {
                try {
                    listenToController(controllerConnection);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void listenToController(Socket controllerConnection) throws IOException {

        BufferedReader in = new BufferedReader(
                new InputStreamReader(controllerConnection.getInputStream())
        );
        String line;
        while((line = in.readLine()) != null) {
            // System.out.println("-> RECEIVE : " + line);
            communicator.displayReceivedMessageInController(controllerConnection,line);
            String[] splittedMessage = line.split(" ");

            switch (splittedMessage[0]) {

                case Protocol.REMOVE_TOKEN -> {
                    // delete file from dstore folder
                    File file = new File((fileFolder + "/" + splittedMessage[1]));
                    if (file.delete()) {
                        System.out.println("-> FILE " + splittedMessage[1] + " DELETED");

                    } else {
                        System.out.println("-> NO FILE TO DELETE");
                        communicator.sendMessage(controllerConnection,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + splittedMessage[1]);
                    }
                    communicator.sendMessage(controllerConnection, Protocol.REMOVE_ACK_TOKEN + " " + splittedMessage[1]);
                }

            }
        }
    }


    public void openServerSocketForClient(int port) {

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // permanently listen for new clients
            for(;;){
                try{
                    Socket clientConnection = serverSocket.accept();
                    System.out.println("CLIENT [" + clientConnection.getPort() + "] ACCEPTED");
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
        // TODO : which while is more appropriate ?
        // while((line = in.readLine()) != null) {
        while (!clientConnection.isClosed()) {
            line = in.readLine();
            communicator.displayReceivedMessageInDstore(clientConnection,port,line);
            String[] splittedMessage = line.split(" ");

            switch (splittedMessage[0]) {

                case Protocol.STORE_TOKEN -> {
                    clientConnection.setSoTimeout(timeout);
                    communicator.sendMessage(clientConnection, Protocol.ACK_TOKEN);
                    storeFile(clientConnection,fileFolder + "/" + splittedMessage[1]);
                    // notice that the connection to the controller is done in way before and stored in a class variable
                    communicator.sendMessage(controllerConnection, Protocol.STORE_ACK_TOKEN + " " + splittedMessage[1]);
                    System.out.println("-> STORE FINISHED");
                }

                case Protocol.LOAD_DATA_TOKEN -> {
                    clientConnection.setSoTimeout(timeout);
                    loadFile(clientConnection,fileFolder + "/" + splittedMessage[1]);
                }

                // In REMOVE operation the dstore receives no messages form the client

                // In LIST operation the dstore receives no messages form the client

                default -> {
                    System.out.println("no switch case covered in handled new clientConnection");
                }
            }
            // TODO : is this necessary ?
            // break;
        }
        System.out.println("CLIENT [" + clientConnection.getPort() + "] DISCONNECTED");
    }


    public void storeFile(Socket clientConnection,String filename) throws IOException {

        InputStream in = clientConnection.getInputStream();
        byte[] buf = new byte[1000]; int buflen;
        //String firstBuffer = new String(buf,0,buflen);
        //int firstSpace = firstBuffer.indexOf(" ");
        //String command = firstBuffer.substring(0,firstSpace);
        //System.out.println("message starts with " + command);
        //if(command.equals(Protocol.STORE_TOKEN)) {
            //int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
        //String filename =
                //firstBuffer.substring(firstSpace + 1, secondSpace);
        System.out.println("-> PATH : " + filename);
        File outputFile = new File(filename);
        FileOutputStream out = new FileOutputStream(outputFile);
        //out.write(buf, secondSpace + 1, buflen - secondSpace - 1);
        System.out.println("STORING FILE");
        while ((buflen = in.read(buf)) != -1) {
            System.out.println("*");
            out.write(buf, 0, buflen);
        }
        // close all the connections
        in.close();
        // clientConnection.close();
        out.close();
       // }
    }

    // TODO : load is not working
    private void loadFile(Socket clientConnection,String filename) throws IOException {

        byte[] buf = new byte[1000]; int buflen;

        System.out.println("-> FILE PATH : " + filename);
        File inputFile = new File(filename);
        FileInputStream inf = new FileInputStream(inputFile);
        OutputStream out = clientConnection.getOutputStream();
        System.out.println("LOADING FILE");
        while ((buflen = inf.read(buf)) != -1){
            System.out.println("*");
            out.write(buf,0,buflen);
        }
        inf.close();
        // clientConnection.close();
        out.close();
        System.out.println("-> LOAD FINISHED");
    }


    public void setupFileStore(String file_folder){
        // creating folder
        this.fileFolder = new File(file_folder);

        // creating the folder if it doesn't exist
        if(!this.fileFolder.exists()){
            this.fileFolder.mkdir();
        } else {
            File[] files = fileFolder.listFiles();
            //assert files != null;
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
            Dstore dstore = new Dstore(port, cport, timeout, file_folder);
        }
        catch(Exception e){
            System.out.println("Unable to create DStore.");
        }
    }

}
