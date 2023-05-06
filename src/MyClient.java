import javax.naming.ldap.SortKey;
import java.io.*;
import java.net.*;
import java.sql.Connection;

public class MyClient {

    private Communicator communicator;
    private Socket connection;
    //protected Client client;
    //protected Logger.LoggingType logger;

    //add a buffer receiver to the client
    public MyClient(int cport, int timeout) throws SocketException {

        communicator = new Communicator();

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

            communicator.sendMessage(connection,Protocol.STORE_TOKEN + " file1 100");
            listenMessagesThread(connection);
            // TODO after receiving the message form controller pass the ports to make the connection
        }catch(Exception e){System.out.println("error"+e);}

            System.out.println("MESSAGES " + " sent");
            //Thread.sleep(1000);
    }


    public void listenMessagesThread (Socket connection) {
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

    public void callDstore(int port) {
        try{
            // get the local host
            InetAddress localAddress = InetAddress.getLocalHost();
            Socket dstoreConnection = new Socket(localAddress,port);

            //communicator.sendMessage(dstoreConnection,Protocol.STORE_TOKEN + " file1 100");
            String[] message = (Protocol.STORE_TOKEN + " file2.png 100").split(" ");

            sendFile(dstoreConnection, communicator,message);

            System.out.println("back in callDstore method");
            //System.out.println(Protocol.STORE_TOKEN + " file1 100" + " sent");

            // for this to work you need an open connection
            // listenMessagesThread(dstoreConnection);

        }catch(Exception e){System.out.println("Error in callDstore");}
    }

    public void sendFile(Socket dstoreConnection, Communicator communicator, String[] message) {
        System.out.println("send message : " + message[0]+" "+message[1]+" "+message[2]);
        if(message[0].equals(Protocol.STORE_TOKEN)){
            File inputFile = new File(message[1]);
            try {
                FileInputStream in = new FileInputStream(inputFile);
                //System.out.println("gets here");
                try{
                    OutputStream out = dstoreConnection.getOutputStream();
                    //out.write(("STORE"+" "+message[1]+" ").getBytes());
                    out.write(("STORE"+" "+"newimage" + " ").getBytes());
                    byte[] buf = new byte[1000]; int buflen;
                    while ((buflen=in.read(buf)) != -1){
                        System.out.print("*");
                        out.write(buf,0,buflen);
                    }
                    //close connection
                    out.close();
                }catch(Exception e){System.out.println("error"+e);}
                System.out.println();
                //close connection
                in.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
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

