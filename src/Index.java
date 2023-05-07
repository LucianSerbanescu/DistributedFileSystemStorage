import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

    protected ConcurrentHashMap<String,String> index;

    public Index() {
        index = new ConcurrentHashMap<>();
    }

//    public void storeInProgress(String filename) {
//        index.put(filename,"store in progress");
//    }

    public String getFileProgress(String filename) {
        return index.get(filename);
    }

    public boolean storeInProgress(String fileName) {
        if (index.putIfAbsent(fileName, "STORE_IN_PROGRESS") == null) {
            return true;
        }
        return false;
    }

    public void updateIndexStatus(String filename) {
        index.put(filename,"STORE_COMPLETED");
    }

}
