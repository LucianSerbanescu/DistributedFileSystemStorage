import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.*;

public class zztest1 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Random random = new Random();
        new Thread(() -> new Controller(12345, 10, 100, 1000)).start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 101; i++) {
            int finalI = i;
            new Thread(() -> {
                try {
                    new Dstore(2333 + finalI, 12345, 100, String.format("folder%d", finalI));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }

        Thread.sleep(2000);

        ConcurrentLinkedDeque<Client> clients = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < 30; i++) {
            createNewClient(clients);
        }
        Thread.sleep(500);

        var files = new File("Client/folderToUpload").listFiles();
        System.out.println(files.length);

        while (true) {
            clients.forEach(client -> {
                try {
                    System.out.println();
                    int d = random.nextInt(6);
                    var file = files[random.nextInt(files.length)];
                    var fileName = files[random.nextInt(files.length)].getName();
                    switch (d) {
                        case 0, 4, 5 -> {
                            client.store(file);

                        }
                        case 1 -> {
                            client.load(fileName);

                        }
                        case 2 -> {
                            client.remove(fileName);

                        }
                        case 3 -> {
                            client.list();

                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Thread.sleep(2000);
        }
    }

    private static void createNewClient(ConcurrentLinkedDeque<Client> clients) {
        new Thread(() -> {
            try {
                Client client = new Client(12345, 1000, Logger.LoggingType.ON_TERMINAL_ONLY);
                clients.add(client);
                client.connect();
                client.list();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }
}
