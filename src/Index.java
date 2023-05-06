import java.util.HashMap;

public class Index {

    protected HashMap<String,String> index;

    public Index() {
        index = new HashMap<>();
    }

    public void store(String filename, String message) {
        index.put(filename,message);
        System.out.println(index.get("file1"));
    }

    public void updateIndexStatus(String filename, String status) {

    }

}
