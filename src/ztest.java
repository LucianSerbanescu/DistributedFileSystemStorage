import java.io.IOException;

public class ztest {
    public static void main(String[] args) {
        new Thread(() -> {
            new Controller(12345, 3, 100, 1_000_000_000);
        }).start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            new Thread(() -> {
                try {
                    new Dstore(2333 + finalI, 12345, 100, String.format("folder%d", finalI));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }
    }
}
