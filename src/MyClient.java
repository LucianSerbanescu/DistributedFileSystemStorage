import java.io.*;
import java.net.*;
import java.util.Objects;

public class MyClient {

    //private Communicator communicator;
    private Socket connection;
    //protected Client client;
    //protected Logger.LoggingType logger;

    //add a buffer receiver to the client
    public MyClient(int cport, int timeout) throws SocketException {

        //communicator = new Communicator();

        //callController(cport);
        //callController(connection);

        //callController(cport);
        callDstore(2333);
    }

    public void callController(int cport) {

        try{
            // create connection
            InetAddress localAddress = InetAddress.getLocalHost();
            Socket connection = new Socket(localAddress,cport);

            sendMessage(connection, Protocol.STORE_TOKEN + " file1 100");
            listenMessagesFromController(connection);
            // TODO after receiving the message form controller pass the ports to make the connection
        }catch(Exception e){System.out.println("error"+e);}

            System.out.println("MESSAGES " + " sent");
            //Thread.sleep(1000);
    }

    public void callDstore(int port) {
        try{

            InetAddress localAddress = InetAddress.getLocalHost();
            Socket dstoreConnection = new Socket(localAddress,port);

            String messageString = (Protocol.STORE_TOKEN + " file1.txt 101");
            String[] messageSplitted = messageString.split(" ");

            sendMessage(dstoreConnection,messageString);

            new Thread(() -> {
                try {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(dstoreConnection.getInputStream())
                    );
                    while(true) {
                        String line;
                        if (Objects.equals(line = in.readLine(), Protocol.ACK_TOKEN)){
                            File inputFile = new File(messageSplitted[1]);
                            sendFile(dstoreConnection,inputFile);
                            break;
                            // TODO : Wait for the STORE COMPLETE message form controller after all R dstores have saved the file
                        }
                        //System.out.println(line + " received");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();

        }catch(Exception e){System.out.println("Error in callDstore");}
    }

    public void sendFile(Socket dstoreConnection, File inputFile) {
        try {
            FileInputStream in = new FileInputStream(inputFile);
            try{
                OutputStream out = dstoreConnection.getOutputStream();
                byte[] buf = new byte[1000]; int buflen;
                while ((buflen=in.read(buf)) != -1){
                    System.out.print("*");
                    out.write(buf,0,buflen);
                }
                //close connection
                out.close();
            }catch(Exception e){System.out.println("error"+e);}
            //System.out.println();
            //close connection
            in.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public synchronized void sendMessage(Socket connection , String message) {

        try {
            PrintWriter out = new PrintWriter(connection.getOutputStream());
            out.println(message); out.flush();
        } catch (IOException e) {
            System.out.println("error in sendMessage");
        }
    }

    public void listenMessagesFromController(Socket connection) {
        // new thread for listening to the controllers messages
        new Thread(() -> {

            BufferedReader in = null;
            try {
                in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream())
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String line;

            // listens until decide who made the connection
            while(true) {
                try {
                    if ((line = in.readLine()) == null){
                        if(line.split(" ")[0] == Protocol.STORE_TO_TOKEN){
                            callDstore(Integer.parseInt(line.split(" ")[1]));
                            break;
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(line + " received");
                //String[] splittedMessage = line.split(" ");
            }

        }).start();
    }



    public static void main(String[] args) {

        try {
            // get parameters
            int cport = Integer.parseInt(args[0]);
            int timeout = Integer.parseInt(args[1]);
            MyClient myClient = new MyClient(cport,timeout);
        } catch (Exception e) {
            System.out.println("unable to create a Client");
        }
    }
}

