import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Objects;

public class MyClient {

    //private Communicator communicator;
    private Socket connection;
    //protected Client client;
    //private Logger.LoggingType logger;

    //add a buffer receiver to the client
    public MyClient(int cport, int timeout) throws IOException {

        String filename = "file1.txt";

        int fileSize = 100;

        //Client client = new Client(cport,timeout,logger);

        //client.store(new File(filename));

        callControllerTest1(cport, filename, fileSize);



    }

    public void callControllerTest1(int cport, String filename, int fileSize) {

        try{
            // create connection
            InetAddress localAddress = InetAddress.getLocalHost();
            Socket connection = new Socket(localAddress,cport);

            sendMessage(connection, Protocol.STORE_TOKEN + " " + filename + " " + fileSize);

            ArrayList<Integer> allDstoresPorts = listenForAllDstoresPorts(connection);

            for (Integer allDstoresPort : allDstoresPorts) {
                callDstore(allDstoresPort);
            }

            listenStoreComplete(connection);


        }catch(Exception e){
            System.out.println("error"+e);
        }

            System.out.println("MESSAGES " + " sent");
    }

    public void callDstore(int port) {
        try{

            InetAddress localAddress = InetAddress.getLocalHost();
            Socket dstoreConnection = new Socket(localAddress,port);

            String messageString = (Protocol.STORE_TOKEN + " to_store.txt 101");
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

    public ArrayList<Integer> listenForAllDstoresPorts(Socket connection) {

        ArrayList<Integer> allPorts = new ArrayList<>();

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            while(true) {
                String line;
                if (Objects.equals(((line = in.readLine())).split(" ")[0], Protocol.STORE_TO_TOKEN)) {
                    System.out.println(line);
                    for (int i = 1; i < line.split(" ").length; i++) {
                        //System.out.println(line.split(" ")[i]);
                        allPorts.add(Integer.parseInt(line.split(" ")[i]));
                    }
                    break;
                } else {
                    System.out.println(line);
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return allPorts;
    }

    public void listenStoreComplete(Socket connection) {

        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream())
            );
            while(true) {
                String line;
                if (Objects.equals(((line = in.readLine())).split(" ")[0], Protocol.STORE_COMPLETE_TOKEN)) {
                    System.out.println(line);
                    break;
                } else if (Objects.equals(((line = in.readLine())).split(" ")[0], Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN)) {
                    System.out.println(line);
                    break;
                } else if (Objects.equals(((line = in.readLine())).split(" ")[0], Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) {
                    System.out.println(line);
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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

