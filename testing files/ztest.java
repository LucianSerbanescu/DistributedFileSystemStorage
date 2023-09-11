import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ztest {
    public static void main(String[] args) throws IOException, InterruptedException {
        new Thread(() -> {
            new Controller(12345, 3, 100, 1000);
        }).start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 3; i++) {
            int finalI = i;
//            Dstore dstore;
            new Thread(() -> {
                Dstore dstore = null;
                try {
                    dstore = new Dstore(2333 + finalI, 12345, 100, String.format("folder%d", finalI));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }

        Thread.sleep(2000);
        Client client = new Client(12345, 100, Logger.LoggingType.ON_TERMINAL_ONLY);
        client.connect();
        client.store(new File("small_file.jpg"));
        Thread.sleep(5000);
        client.store(new File("file1.txt"));
        client.load("small_file.jpg");
    }
}
