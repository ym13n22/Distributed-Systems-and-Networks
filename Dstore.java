import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ClientInfoStatus;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Dstore {
    private int port;//destore's port
    private int cport;//controller's port
    private int timeout;//milliseconds
    private String file_folder;

    Set<String> files=new HashSet<>();

    List<HashMap<String,Integer>> filePair=new ArrayList<>();


    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;//listen
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];

        Dstore dstore1 = new Dstore(port, cport, timeout, file_folder);
        ExecutorService threadPool= Executors.newFixedThreadPool(50);


        try {
            ServerSocket serverSocketListen=new ServerSocket(port);
            Socket connectController=new Socket(InetAddress.getLoopbackAddress(),cport);
            PrintWriter controllerOut=new PrintWriter(connectController.getOutputStream(),true);
            controllerOut.println(Protocol.JOIN_TOKEN+" "+port);
            System.out.println("Send "+Protocol.JOIN_TOKEN+" "+port);
            for(;;){
                Socket listenSocket=serverSocketListen.accept();
                threadPool.submit(()->{dstore1.handleClient(listenSocket,connectController);});
                //handleClient.start();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private void handleClient(Socket listenSocket,Socket controllerSocket){
        try {
            BufferedReader listenReader=new BufferedReader(new InputStreamReader(listenSocket.getInputStream()));
            PrintWriter listenOut=new PrintWriter(listenSocket.getOutputStream(),true);
            PrintWriter controllerOut=new PrintWriter(controllerSocket.getOutputStream(),true);
            String input;
            while((input=listenReader.readLine())!=null){
                System.out.println("receive Message "+ input);
                String[] stringSplit=input.split(" ");
                String message=stringSplit[0];
                if(message.equals(Protocol.STORE_TOKEN)){handleSTORE(listenOut,input,listenSocket,controllerOut);}
                if(message.equals(Protocol.LOAD_DATA_TOKEN)){handleLoadData(input,listenSocket);}
                if(message.equals(Protocol.REMOVE_TOKEN)){handleRemove(input,controllerOut,listenSocket);}
                if(message.equals(Protocol.LIST_TOKEN)){handleList(listenOut);}
                if(message.equals(Protocol.REBALANCE_TOKEN)){handleRebalance(input,listenOut);}
              //  if(message.equals(Protocol.ACK_TOKEN)){}
                if(message.equals(Protocol.REBALANCE_STORE_TOKEN)){handleBalanceStore(input,listenOut,listenSocket,controllerOut);}
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    private void handleSTORE(PrintWriter listenOut,String input,Socket clientSocket,PrintWriter controllerOut){

        try {
            listenOut.println(Protocol.ACK_TOKEN);
            String[] stringSplit=input.split(" ");
            String fileName=stringSplit[1];
            String fileSize=stringSplit[2];
//            Index index=new Index(fileName,0);
//            index.setFileSize(Integer.parseInt(fileSize));
            Boolean isSaved=fileReceiving(fileName,Integer.parseInt(fileSize),clientSocket.getInputStream());
            if(isSaved){
                controllerOut.println(Protocol.STORE_ACK_TOKEN+ " "+fileName);
                files.add(fileName);
                System.out.println("Send controller "+ Protocol.STORE_ACK_TOKEN+ " "+fileName);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private synchronized Boolean  fileReceiving(String fileName, int filesize, InputStream inputStream){
        Boolean isSaved;
        try {
            byte[] data = new byte[filesize];
            inputStream.readNBytes(data, 0, filesize);
            Path folderPath = Paths.get(file_folder);
            if (!Files.exists(folderPath)) {
                Files.createDirectories(folderPath); // 创建文件夹及其父文件夹（如果不存在）
            }
            Path filePath = folderPath.resolve(fileName);
            Files.write(filePath, data); // 将 data 写入文件 fileName 中，存入 file_folder
            isSaved=true;
            HashMap<String,Integer> fileMap=new HashMap<>();
            fileMap.put(fileName,filesize);
            filePair.add(fileMap);
        } catch (IOException e) {
            isSaved=false;
            throw new RuntimeException(e);
        }
        return isSaved;
    }

    public synchronized void handleLoadData(String input,Socket listenSock){
        String[] strSplit=input.split(" ");
        String fileName=strSplit[1];
        Path path=Paths.get(file_folder,fileName);
        try {
            byte[] fileData= new byte[0];
            fileData = Files.readAllBytes(path);
            System.out.println("LoadDATA "+fileName);
            listenSock.getOutputStream().write(fileData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void handleRemove(String input,PrintWriter controllerOut,Socket listenSocker){
        String[] strSplit=input.split(" ");
        String fileName=strSplit[1];
        Path path=Paths.get(file_folder,fileName);
        if(Files.exists(path)){
            try {
                Files.delete(path);
                System.out.println("successfully delete "+fileName);
                files.remove(fileName);
                controllerOut.println(Protocol.REMOVE_ACK_TOKEN +" "+fileName);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
//            try {
//                listenSocker.close();
//                System.out.println("Socket closed");
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
        }

    }

    private void handleList(PrintWriter out){
        String outString=Protocol.LIST_TOKEN;
        for(String file:files){
            outString=outString+" "+file;
        }
        out.println(outString);
        System.out.println("List "+ outString);
    }

    private synchronized void handleRebalance(String input,PrintWriter out) {
        String[] strSplit = input.split(" ");
        ArrayList<String> strings = new ArrayList<>(Arrays.asList(strSplit));
        int numFileSend = Integer.parseInt(strings.get(1));
        System.out.println("numFileSend " + numFileSend);
        int currentIndex = 2; // 当前索引，从文件名开始
        ArrayList<String> additionalFiles = new ArrayList<>(); // 存储额外文件名的列表
        for (int fileIndex = 0; fileIndex < numFileSend; fileIndex++) {
            String fileName = strings.get(currentIndex);
            System.out.println("gIVE fileName " + fileName);
            int numPorts = Integer.parseInt(strings.get(currentIndex + 1));
            System.out.println("Number of ports for " + fileName + ": " + numPorts);
            for (int portIndex = 0; portIndex < numPorts; portIndex++) {
                int port = Integer.parseInt(strings.get(currentIndex + 2 + portIndex));
                System.out.println("Test port " + port);
                connectSend(port, fileName);
            }
            currentIndex += numPorts + 2; // 移动到下一个文件名
        }
        int removeNum=Integer.parseInt(strings.get(currentIndex));
      //  System.out.println("remove number "+removeNum);
        currentIndex++;
        // 处理额外文件
        for (int i = 0; i < removeNum; i++) {
            additionalFiles.add(strings.get(currentIndex + i));
            String fileName=strings.get(currentIndex+i);
            Path path=Paths.get(file_folder,fileName);
            if(Files.exists(path)){
                try {
                    Files.delete(path);
                    files.remove(fileName);
                    System.out.println("remove file "+fileName);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }else {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            }

        }
     //   System.out.println("Additional files: " + additionalFiles);
    }
    private void connectSend(int port,String fileName){
        try {
            Socket socket=new Socket(InetAddress.getLoopbackAddress(),port);
            PrintWriter out=new PrintWriter(socket.getOutputStream(),true);
            BufferedReader in=new BufferedReader(new InputStreamReader(socket.getInputStream()));
            int fileSize = 0;
            for(HashMap<String,Integer> pair:filePair){
                for (Map.Entry<String,Integer> e:pair.entrySet()){
                    if(e.getKey().equals(fileName)){
                        fileSize=e.getValue();
                    }
                }
            }
            out.println(Protocol.REBALANCE_STORE_TOKEN+" "+fileName+" "+fileSize);
            System.out.println("Send Port "+port+" "+Protocol.REBALANCE_STORE_TOKEN+" "+fileName+" "+fileSize);
            String input=in.readLine();
            String[] stSplit=input.split(" ");
            String message=stSplit[0];
            System.out.println("receive the message from new destore "+input);
            Path path=Paths.get(file_folder,fileName);
            if(message.equals(Protocol.ACK_TOKEN)){handleACK(path,socket);}
            if(Files.exists(path)){
                Files.delete(path);
                files.remove(fileName);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    private synchronized void handleBalanceStore(String input,PrintWriter out,Socket socket,PrintWriter controllerOut){
        try {
            String[] split=input.split(" ");
            String fileName=split[1];
            int fiSize=Integer.parseInt(split[2]);

            Path path=Paths.get(file_folder,fileName);
            out.println(Protocol.ACK_TOKEN);
            int port= socket.getPort();
            System.out.println("send ACK to "+port);
            if(!Files.exists(path)){
                Boolean isSaved=fileReceiving(fileName,fiSize,socket.getInputStream());
                if(isSaved){
                    files.add(fileName);
                }
            }
            controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
            System.out.println("send rebalance complete");

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }

    }

    private void handleACK(Path path,Socket listenSock){
        try {
            byte[] fileData= new byte[0];
            fileData = Files.readAllBytes(path);
            listenSock.getOutputStream().write(fileData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}