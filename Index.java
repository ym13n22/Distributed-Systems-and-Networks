import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Index {
    private String fileName;

    private int fileSize;

    private int port;

    private String state;

    private Socket socket;

    private BufferedReader bufferedReader;

    private PrintWriter printWriter;

    private int storeReplication;

    public Index(String fileName,int port){
        this.fileName=fileName;
        this.fileSize=0;
        this.port=port;
        this.state=null;
        this.socket=null;
        this.bufferedReader=null;
        this.printWriter=null;
        this.storeReplication=0;
    }

    public void setFileSize(int fileSize){
        this.fileSize=fileSize;
    }


    public int getFileSize(){
        return fileSize;
    }

    public String getFileName(){
        return fileName;
    }

    public void setPort(int port){
        this.port=port;
    }
    public int getPort(){
        return port;
    }

    public  void setState(String state){
        if (state.equals("storing")){
            this.state=state;
        }
        if(state.equals("stored")){
            this.state=state;
        }
        if(state.equals("removing")){
            this.state=state;
        }
        if(state.equals("removed")){
            this.state=state;
        }
        if(state.equals("loading")){
            this.state=state;
        }
        if(state.equals("loaded")){
            this.state=state;
        }
    }

    public String getState(){
        return state;
    }

    public void setSocket(Socket socket){
        this.socket=socket;
    }

    public Socket getSocket(){
        return this.socket;
    }

    public void setBufferedReader(BufferedReader bufferedReader){
        this.bufferedReader=bufferedReader;
    }

    public BufferedReader getBufferedReader(){
        return bufferedReader;
    }

    public void setPrintWriter(PrintWriter printWriter){
        this.printWriter=printWriter;
    }

    public PrintWriter getPrintWriter(){
        return printWriter;
    }

    public void addStoreReplication(){
        this.storeReplication=storeReplication+1;
    }

    public void deleteStoreReplication(){
        this.storeReplication=storeReplication-1;
    }

    public int getStoreReplication(){
        return  this.storeReplication;
    }


}
