import java.io.File;
import java.io.IOException;

public class ztest2 {
    public static void main(String[] args) throws IOException, InterruptedException {
        for (int i = 2; i < 15; i++) {
            int finalI = i;
//            Dstore dstore;
            var thread = new Thread(() -> {
                Dstore dstore = null;
                try {
                    dstore = new Dstore(2333 + finalI, 12345, 100, String.format("folder%d", finalI));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
        }
    }
}
