import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Rebalance {
    private List<String> filename=new ArrayList<>();
    private int portNumber;

    private List<HashMap<String,Integer>> fileToSend=new ArrayList<>();


    public Rebalance( List<String> filename, int portNumber) {
        this.filename = filename;
        this.portNumber = portNumber;
      //  this.dstores = dstores;
    }

    public List<String> getFilename(){
        return filename;
    }

//    public List<Integer> getDstores(){
//        return dstores;
//    }

    public void removeFile(String fileName){
        filename.remove(fileName);
    }

    public void addFileToSend(String file,int portToSend){
        HashMap<String,Integer> toSend=new HashMap<>();
        toSend.put(file,portToSend);
        fileToSend.add(toSend);
    }




}
