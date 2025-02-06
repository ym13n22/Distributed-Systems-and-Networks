import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Controller {

    private int cport;//controller port
    private int r;//replication factor
    private int timeout;//in milliseconds
    private int rebalance_period;//next balance operation

    private Set<Integer> storePorts= new HashSet<>();

    private Set<String> setToStore=new HashSet<>();

    private Set<String> setStored=new HashSet<>();

    private Set<String> setToRemove=new HashSet<>();

    private Set<String> setRemoved=new HashSet<>();

    Socket clientSocket;
    Boolean allComplete=false;

    Boolean allDelete=false;

    Timer timer=new Timer();

    private List<HashMap> files=Collections.synchronizedList(new ArrayList<>());

    private List<HashMap> fileLoad=Collections.synchronizedList(new ArrayList<>());

    private List<HashMap> IO=Collections.synchronizedList(new ArrayList<>());

    Set<Rebalance> toRebalanceList=new HashSet<>();

    List<HashMap<Rebalance,Integer>> reBPair=new ArrayList<>();//reblance,port

    List<HashMap<Integer,Integer>> portNeedFile=new ArrayList<>();//port,fileLess

    List<HashMap<Socket,Integer>> socketPort=new ArrayList<>();

  //  List<HashMap<Index,String>> indexFileName=new ArrayList<>();

    Set <Index> indexList=new HashSet<>();

    public 	Controller(int cport,int R,int timeout,int rebalance_period){
        this.cport=cport;
        this.r=R;
        this.timeout=timeout;
        this.rebalance_period=rebalance_period;
    }
    public static void main(String[] args){
        int cPort=Integer.parseInt(args[0]);
        int replicationFactor=Integer.parseInt(args[1]);
        int timeout=Integer.parseInt(args[2]);
        int rebalance_Period=Integer.parseInt(args[3]);

        Controller controller=new Controller(cPort,replicationFactor,timeout,rebalance_Period);
        //here to call the method of the class Controller
        ExecutorService threadPool= Executors.newFixedThreadPool(50);

        try {
            ServerSocket serverSocket=new ServerSocket(cPort);
            for(;;){
                Socket clientSocket=serverSocket.accept();
                threadPool.submit(()->{controller.handleReceive(clientSocket,threadPool);});
                for(int port: controller.storePorts) {
//                    System.out.println("Port new "+port);
//                    Socket sSocket = new Socket(InetAddress.getLoopbackAddress(), port);
//                    threadPool.submit(()->{
//                        controller.createNewdsOI(sSocket);});
                }
//                threadPool.submit(()->{
//                    if(controller.clientPort!=0) {
//                        Socket cSocket = null;
//                        try {
//                            cSocket = new Socket(InetAddress.getLoopbackAddress(), controller.clientPort);
//                            PrintWriter clOut =new PrintWriter(cSocket.getOutputStream(),true);
//                            System.out.println("new connected");
//                            if(controller.allDelete==true){
//                                clOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
//                                System.out.println("Send Remove Completeeeeee");
//                            }
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//
//                    }
//
//                });

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleReceive(Socket clientSocket,ExecutorService pool){
        try {
            PrintWriter out=new PrintWriter(clientSocket.getOutputStream(),true);
            BufferedReader in=new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            String input;
            while((input=in.readLine())!=null){
                System.out.println("Receive Message "+input);
                String[] messageSplit=input.split(" ");
                String message=messageSplit[0];
                if(message.equals(Protocol.LIST_TOKEN)){handleList(out,clientSocket,input);}
                if(message.equals(Protocol.JOIN_TOKEN)){receiveJoin(input,pool);}
                if(message.equals(Protocol.STORE_TOKEN)){
                    receiveStore(out,input);
                   this.clientSocket =clientSocket;
                        Thread.sleep(0);

//                    if(allComplete){
//                        out.println(Protocol.STORE_COMPLETE_TOKEN);
//                        System.out.println("send complete");
//                    }
                }
                if(message.equals(Protocol.STORE_ACK_TOKEN)){
                    handleStoreACK(input,clientSocket);
                }
                if(message.equals(Protocol.LOAD_TOKEN)){handleLoad(input,out);}
                if(message.equals(Protocol.RELOAD_TOKEN)){handleReload(input,out);}
                if(message.equals(Protocol.REMOVE_TOKEN)){
                    Thread.sleep(100);
                    handleRemove(input,out);
                //    System.out.println("allDELETE "+allDelete);
                    Thread.sleep(0);
                //    int it=0;
//                    while (allDelete==true&&it<100){
//                        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
//                        System.out.println("Send REMOVE");
//                        it++;
//                    }
                }
                if(message.equals(Protocol.REMOVE_ACK_TOKEN)){handleRemoveACK(input);}
                if(message.equals(Protocol.REBALANCE_COMPLETE_TOKEN)){}
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void receiveJoin(String input,ExecutorService threadPool){
        String[] messageSplit=input.split(" ");
        String port=messageSplit[1];
        int portInt=Integer.parseInt(port);
        storePorts.add(portInt);
    //    System.out.println("add " +portInt);

        try {
            Socket sSocket = new Socket(InetAddress.getLoopbackAddress(), portInt);
            threadPool.submit(()->{
       //         System.out.println("Port new "+port);
                HashMap<Socket,Integer> hashMap=new HashMap();
                hashMap.put(sSocket,portInt);
                socketPort.add(hashMap);
                createNewdsOI(sSocket)
                ;});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private synchronized int receiveStore(PrintWriter out,String input){
        String symbol=Protocol.STORE_TO_TOKEN;
        String[] messageSplit=input.split(" ");
        String fileName=messageSplit[1];


        if(storePorts.size()==0){
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
//            System.out.println("Send "+Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return 0;
        }
//        if(setToStore.contains(fileName)){
//            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
//            System.out.println("Send "+Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
//            return;
//        }
        for(Index ind :indexList){
            if(ind.getFileName().equals(fileName)){
                if(ind.getState().equals("storing")||ind.getState().equals("stored")||ind.getState().equals("removing")){
                    out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
       //             System.out.println("Send "+Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return 0;
                }
            }

        }
        int fileSize=Integer.parseInt(messageSplit[2]);

        for(int p:storePorts){
            Index index=new Index(fileName,p);
           // System.out.println("new Index");
            indexList.add(index);
            index.setFileSize(fileSize);
            index.setState("storing");//设置state storing
            // allComplete=false;
        }

        int storeSize= storePorts.size();
        for(int i=0;i<storePorts.size();i++){
            List<Integer> portList = new ArrayList<>(storePorts);
            symbol=symbol+" "+portList.get(i);
        }
        out.println(symbol);

  //      System.out.println("Send "+symbol);
        setToStore.add(fileName);
        HashMap<String,Integer> fileMap=new HashMap<>();
        fileMap.put(fileName,fileSize);
        files.add(fileMap);
 //       System.out.println("hashmap "+ fileMap);
  //      System.out.println("add " +fileName);
        return storeSize;
    }

    private synchronized void handleStoreACK(String input,Socket storeSocket){
        String[] messageSplit=input.split(" ");
        String fileName=messageSplit[1];
        setStored.add(fileName);
        for(Index i:indexList){
            if(i.getFileName().equals(fileName)){
                i.addStoreReplication();
            }
        }
        int dsNumber=setToStore.size();

        Boolean allStored=true;
        for(Index i:indexList){
            if(i.getState()!=null) {
                if(i.getFileName().equals(fileName)) {
                   if((i.getStoreReplication())!=dsNumber){
                       allStored=false;
                   }

                }
            }
        }
        if(allStored){
            try {
                PrintWriter out=new PrintWriter(clientSocket.getOutputStream(),true);
                out.println(Protocol.STORE_COMPLETE_TOKEN);
         //       System.out.println("Store Complete Token Send");
                for(Index i:indexList){//把状态换成stored
                    if(i.getFileName().equals(fileName)){
                        i.setState("stored");
                        i.setSocket(storeSocket);
                        try {
                            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(storeSocket.getInputStream()));
                            i.setBufferedReader(bufferedReader);
                            PrintWriter printWriter=new PrintWriter(storeSocket.getOutputStream(),true);
                            i.setPrintWriter(printWriter);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


//        allComplete=false;
//            int storedNumber=0;
//            if(setToStore.size()==setStored.size()) {
//                for (String file : setToStore) {
//                    if (setStored.contains(file)) ;
//                    storedNumber = storedNumber + 1;
//                }
//                if (storedNumber == setToStore.size()) {
//                    System.out.println("Size " + storedNumber);
//                    allComplete=true;
//                }
//            }
    }

    private void handleList(PrintWriter out,Socket listenSocket,String input){
//        if(!storePorts.contains(listenSocket.getPort())){
            String output=Protocol.LIST_TOKEN;
//            if(setStored.size()==0){
//                output=Protocol.LIST_TOKEN;
//            }else {
//                for(String fileName:setStored) {
//                    output = output + " " + fileName;
//                }
//            }
        for(Index i:indexList){
            if(i.getState().equals("stored")){
                output=output+" "+i.getFileName();
            }
        }
            out.println(output);
            if(storePorts.size()==0){
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            }

//        else {
//            String[] split=input.split(" ");
//            int f=split.length;
//            int n=storePorts.size();
//            int bNm=r*f/n;
//
//            int num=3*10/7;
//            System.out.println("test int "+num+" ==4");
//
//        }

    }

    private synchronized void handleLoad(String input,PrintWriter out){
        String[] strSplit=input.split(" ");
        String fileName=strSplit[1];
        if(storePorts.size()==0){
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
     //       System.out.println("Send error "+Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        for(Index i:indexList){
            if(i.getFileName().equals(fileName)&&!i.getState().equals("stored")){
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
       //         System.out.println("Send error" +Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN +" " +fileName);
                return;
            }
        }
        int fileSize = 0;
        for(HashMap<String,Integer> hashMap:files){
                for(Map.Entry<String,Integer> entry: hashMap.entrySet()){
                    if(entry.getKey().equals(fileName)) {
                        fileSize = entry.getValue();
                    }
        //            System.out.println("Print key " + entry.getKey());
            }
        }
        List<Integer> portList = new ArrayList<>(storePorts);
        String port= String.valueOf(portList.get(0));
        out.println(Protocol.LOAD_FROM_TOKEN+" "+port+" "+fileSize);
        HashMap<String,Integer> fileload=new HashMap<>();
        fileload.put(fileName,Integer.parseInt(port));
        fileLoad.add(fileload);
    }

    private synchronized void handleReload(String input,PrintWriter out) {
        String[] strSplit = input.split(" ");
        String fileName = strSplit[1];
        int fileSize = 0;
        for (HashMap<String, Integer> hashMap : files) {
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                if (entry.getKey().equals(fileName)) {
                    fileSize = entry.getValue();
                }
            }
        }
        List<Integer> oldPort = new ArrayList<>();
        int newport=0;
        int oPort = 0;
        for (HashMap<String, Integer> hashMap : fileLoad) {
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                if (entry.getKey().equals(fileName)) {
                    oPort= entry.getValue();
                    oldPort.add(entry.getValue());
                }
            }
        }
        for (int port:storePorts){
           if(!oldPort.contains(port)){
               newport=port;
           }else {
               newport=oPort;
           }
        }

        out.println(Protocol.LOAD_FROM_TOKEN+" "+newport+" "+fileSize);
        HashMap<String,Integer> fileload=new HashMap<>();
        fileload.put(fileName,newport);
        fileLoad.add(fileload);
    }

    private synchronized void handleRemove(String input,PrintWriter outClient){
        String[] strSplit = input.split(" ");
        String fileName = strSplit[1];
        if(storePorts.size()==0){
            outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            System.out.println("Send error "+Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        for(Index i:indexList){
            if(i.getState()!=null){
                if(i.getFileName().equals(fileName)&&i.getState().equals("removed")){
                    outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    System.out.println("file error "+i.getState()+" "+fileName);
                    System.out.println("Send error" +Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN +" " +fileName);
                    return;
                }
            }

        }
        for(HashMap<BufferedReader,PrintWriter> hashMap:IO){
            for(Map.Entry<BufferedReader,PrintWriter> entry:hashMap.entrySet()){
                BufferedReader in=entry.getKey();
                PrintWriter out=entry.getValue();
                out.println(Protocol.REMOVE_TOKEN +" "+ fileName);
                //System.out.println("out ");
            }
        }
        setToRemove.add(fileName);
        allDelete=false;
        for(Index i:indexList){
            if(i.getFileName().equals(fileName)){
                i.setState("removing");
                indexList.remove(i);
            }
        }
    }

    private void createNewdsOI(Socket socket){
        try {
            BufferedReader in=new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out=new PrintWriter(socket.getOutputStream(),true);
           HashMap<BufferedReader,PrintWriter> portIO=new HashMap<>();
           portIO.put(in,out);
           IO.add(portIO);
            //schedulePeriodicTask(out);
            System.out.println("begin to test out");
            String inp;
            while ((inp=in.readLine())!=null){
                System.out.println("input from ds " +inp);
                String[] split=inp.split(" ");
                String message=split[0];
                if(message.equals(Protocol.LIST_TOKEN)){handleListDs(inp,socket,out);}
                if(message.equals(Protocol.REBALANCE_COMPLETE_TOKEN)){}
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void handleListDs(String input,Socket socket,PrintWriter out){
        List<HashMap<String,Integer>> sendList=new ArrayList<>();//fileName,portToSend
        List<String> removeList=new ArrayList<>();
        int port=socket.getPort();
        String[] split=input.split(" ");
        if(split.length==1){
      //      System.out.println("RETURN");
            return;
        }
        List<String> fileName = new ArrayList<>();
        for (int i = 1; i < split.length; i++) {
            fileName.add(split[i]);
        }
      //  System.out.println("fileNumber List ready to rebalance "+fileName);
       // Rebalance rebalance1=new Rebalance(fileName,port);
       // toRebalanceList.add(rebalance1);
        List<List<String>> rebalanceList=giveBalance();

        List<String> redundant=new ArrayList<>();

        for(List<String> list:rebalanceList){
            if(Integer.parseInt(list.get(0))==port){
                for(int i=0;i<fileName.size();i++){
                    if(!list.contains(fileName.get(i))){
                        redundant.add(fileName.get(i));
                    }
                }
            }
        }
        System.out.println("re List"+redundant);
        List<String> send=new ArrayList<>();
        Set<String> remove=new HashSet<>();
        for(List<String> list:rebalanceList){
            if(Integer.parseInt(list.get(0))!=port){
                for(String redu:redundant){
                    if(list.contains(redu)){
                        HashMap<String,Integer> sendPair=new HashMap<>();
                        sendPair.put(redu,Integer.parseInt(list.get(0)));
                        sendList.add(sendPair);
                        send.add(redu);
                       // System.out.println("Send "+sendPair);

                    }
                }
            }
        }

        for(String re:redundant){
            if(!send.contains(re)){
                remove.add(re);
            }
        }
        System.out.println("Test remove and send");
        System.out.println("sendList "+sendList);
        System.out.println("remove List"+remove);


        String strOut=giveSendString(sendList,removeList);
        out.println(Protocol.REBALANCE_TOKEN +" "+strOut);
        System.out.println("rEBAANCE "+Protocol.REBALANCE_TOKEN +" "+strOut );
//        HashMap<Rebalance,Integer> rePair=new HashMap<>();
//        rePair.put(rebalance1,port);
//        reBPair.add(rePair);
//            int f=setStored.size();
//            int n=storePorts.size();
//            int lNum=r*f/n;
//            System.out.println("lower "+lNum);
//            int upNum=lNum+1;
//            System.out.println("upper "+upNum);
//            int fileSize= rebalance1.getFilename().size();
//            if(fileSize<lNum){
//                HashMap less=new HashMap();
//                less.put(port,lNum-fileSize);
//             portNeedFile.add(less);
//            }
//            if(fileSize>lNum){
//               sendList=handleGreaterFile(rebalance1);
//                removeList=reblanceRemove(sendList,fileName,upNum,rebalance1);
//                String strOut=giveSendString(sendList,removeList);
//                out.println(Protocol.REBALANCE_TOKEN +" "+strOut);
//                System.out.println("REBALANCE sEND " +Protocol.REBALANCE_TOKEN +" "+strOut);
//            }
    }



    private synchronized List<List<String>> giveBalance() {
        int storeNum=storePorts.size();
        List<List<String>> storeList = new ArrayList<>();
        for (int i = 0; i < storeNum; i++) {
            storeList.add(new ArrayList<>());
        }

        int index=0;
        List<Integer> storePortsList = new ArrayList<>(storePorts);
        for(int i=0;i<storePortsList.size();i++){
           storeList.get(i).add(String.valueOf(storePortsList.get(i)));
        }


        int currentStoreIndex = 0;
        for (String file : setStored) {
            for (int i = 0; i < r; i++) {
                if (currentStoreIndex >= storeNum) {
                    currentStoreIndex = 0; // 如果超出存储空间的数量，则从第一个存储空间开始放置
                }
                storeList.get(currentStoreIndex).add(file);
                currentStoreIndex++;
            }
        }

        // 输出每个存储空间的文件列表
        for (int i = 0; i < storeList.size(); i++) {
            System.out.println("Store " + i + " files: " + storeList.get(i));
        }
        return storeList;
    }


    private String giveSendString( List<HashMap<String,Integer>> sendList,List<String> removeList){
        String str = null;
        Set<String> fileToSend=new HashSet<>();
        for(HashMap<String,Integer> send:sendList){
            for (Map.Entry<String,Integer> e:send.entrySet()){
                fileToSend.add(e.getKey());
            }
        }
        System.out.println("fileToSend "+fileToSend);

        List<List> fileToSendStore=new ArrayList<>();
        for(String file:fileToSend){
            List<Object> fileList=new ArrayList<>();
            fileList.add(file);
            for(HashMap<String,Integer> send:sendList){
                for (Map.Entry<String,Integer> e:send.entrySet()){
                   if(file.equals(e.getKey())){
                       fileList.add(e.getValue());
                   }
               }
            }fileToSendStore.add(fileList);
        }
        System.out.println("FILE TO SEND STORE "+fileToSendStore);
        String sendAll= String.valueOf(fileToSend.size());
        System.out.println("SnedAll String "+sendAll);
        for(List fileStore :fileToSendStore){
            String fileNumPort=String.valueOf(fileStore.get(0))+" "+(fileStore.size()-1);
            for(int i=1;i<fileStore.size();i++){
                fileNumPort=fileNumPort+" "+fileStore.get(i);
            }
            sendAll=sendAll+" "+fileNumPort;
        }
    //    System.out.println("send all "+sendAll);

        String remove= String.valueOf(removeList.size());
        for(String re:removeList){
            remove=remove+" "+re;
        }
        str=sendAll+" "+remove;
       // System.out.println("send String "+str);
        return str;

    }
    private void schedulePeriodicTask(final PrintWriter out) {
        toRebalanceList.clear();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                out.println(Protocol.LIST_TOKEN);
                System.out.println("send List " +Protocol.LIST_TOKEN);
            }
        };
        timer.schedule(task, 0, rebalance_period);
    }

    private synchronized void handleRemoveACK(String input) {
        allComplete=false;
        Boolean allRemoved=true;
        String[] strSplit = input.split(" ");
        String fileName = strSplit[1];
        setStored.remove(fileName);
        setRemoved.add(fileName);
        for(Index i:indexList){
            if(i.getFileName().equals(fileName)&&i.getState().equals("removing")){
                i.deleteStoreReplication();
            }
        }
        for(Index i:indexList){
            if(i.getFileName().equals(fileName)&&i.getStoreReplication()>0){
                System.out.println("i.getStoreReplication "+fileName +" "+i.getStoreReplication());
                allRemoved=false;
            }
        }
        if(allRemoved){
            try {
                PrintWriter out=new PrintWriter(clientSocket.getOutputStream(),true);
                out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                System.out.println("Send Remove complete " +fileName);
                for(Index i:indexList){
                    if(i.getFileName().equals(fileName)){
                        i.setState("removed");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

      //  allDelete=false;
        int fileNumber = 0;
        if (setToRemove.size() == setRemoved.size()) {
            for (String fTMove : setToRemove) {
                System.out.println(fTMove);
                if (setRemoved.contains(fTMove)) {
                    fileNumber = fileNumber + 1;
                }
            }
            if (fileNumber == setToRemove.size()) {
                allDelete=true;
            }
         }
        System.out.println("allremove "+allDelete);
    }


    private List<HashMap<String,Integer>> handleGreaterFile(Rebalance rebalance1){
        List<HashMap<String,Integer>> fileToRep=new ArrayList<>();
        List<HashMap<String,Integer>> sendList=new ArrayList<>();//fileName,portToSend

        List<HashMap<String,Integer>> fileRPairList=new ArrayList<>();
        for(String fileName:rebalance1.getFilename()){
            int replicationNow=1;
            for(Rebalance rebalance:toRebalanceList){
                for(String fileCheck:rebalance.getFilename()){
                    if(fileName.equals(fileCheck)){
                        replicationNow=replicationNow+1;
                    }
                }
            }HashMap<String,Integer> fileRpair=new HashMap<>();
            fileRpair.put(fileName,replicationNow);
            fileRPairList.add(fileRpair);
        }

        for(HashMap<String,Integer> pair:fileRPairList){
            for(Map.Entry<String,Integer> en:pair.entrySet()){
                if(en.getValue()<r){
                    String fileName=en.getKey();
                    HashMap<String,Integer> fTRep=new HashMap<>();
                    fTRep.put(fileName,r-en.getValue());
                    fileToRep.add(fTRep);
                }
            }
        }


        for(HashMap<String,Integer> file:fileToRep) {//fileName,repeatNumberLeft
            for(Map.Entry<String,Integer> fileEn:file.entrySet()) {
                int repeatNumberLeft = fileEn.getValue();
                if (repeatNumberLeft != 0) {
                    for (HashMap<Integer, Integer> pair : portNeedFile) {//port,fileLess
                        if (repeatNumberLeft != 0) {
                            for (Map.Entry<Integer, Integer> en : pair.entrySet()) {
                                int fileNumberNeed = en.getValue();
                                if (fileNumberNeed != 0 && repeatNumberLeft != 0) {
                                    for (HashMap<Rebalance, Integer> reP : reBPair) {//reblance,port
                                        if (fileNumberNeed != 0 && repeatNumberLeft != 0) {
                                            for (Map.Entry<Rebalance, Integer> rpen : reP.entrySet()) {
                                                if (en.getKey() == rpen.getValue()) {
                                                    if (!rpen.getKey().getFilename().contains(fileEn.getKey()) && repeatNumberLeft != 0 && fileNumberNeed != 0) {
                                                        HashMap<String, Integer> fileSend = new HashMap<>();
                                                        fileSend.put(fileEn.getKey(), en.getKey());//fileName,portToSend
                                                        sendList.add(fileSend);
                                                        repeatNumberLeft = repeatNumberLeft - 1;
                                                        fileEn.setValue(repeatNumberLeft);
                                                        fileNumberNeed = fileNumberNeed - 1;
                                                        en.setValue(fileNumberNeed);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return sendList;

    }

    private List<String> reblanceRemove(List<HashMap<String,Integer>> sendList,List<String> fileName,int up,Rebalance rebalance1){
//        int fileNumberNow=fileName.size();
        List<String> remove=new ArrayList<>();
        List<String> fileToRemove=new ArrayList<>();
        List<String> filePreferToLeave=new ArrayList<>();
        for(HashMap<String,Integer> send:sendList){
            for(Map.Entry<String,Integer> e:send.entrySet()){
                if(fileName.contains(e.getKey())){
                    fileName.remove(e.getKey());
                    rebalance1.removeFile(e.getKey());
                }
            }
        }

        List<HashMap<String,Integer>> fileRPairList=new ArrayList<>();
        for(String fileName1:rebalance1.getFilename()){
            int replicationNow=1;
            for(Rebalance rebalance:toRebalanceList){
                for(String fileCheck:rebalance.getFilename()){
                    if(fileName1.equals(fileCheck)){
                        replicationNow=replicationNow+1;
                    }
                }
            }HashMap<String,Integer> fileRpair=new HashMap<>();
            fileRpair.put(fileName1,replicationNow);
            fileRPairList.add(fileRpair);
        }

        for(HashMap<String,Integer> pair:fileRPairList){
            for(Map.Entry<String,Integer> en:pair.entrySet()){
                if(en.getValue()>r){
                    String fileName2=en.getKey();
                    fileToRemove.add(fileName2);
                }
            }
        }

        for(HashMap<String,Integer> pair:fileRPairList){
            for(Map.Entry<String,Integer> en:pair.entrySet()){
                if(en.getValue()<=r){
                    String fileName4=en.getKey();
                    filePreferToLeave.add(fileName4);
                }
            }
        }



        System.out.println("fileNumberNow "+fileName.size());
        for(String fiTR:fileToRemove){
            if(fileName.size()>up){
                remove.add(fiTR);
                fileName.remove(fiTR);
                rebalance1.removeFile(fiTR);
            }
        }
        System.out.println("fileNumberNow "+fileName.size());
        for(String file:fileName){
            if(fileName.size()>up){
                if(!filePreferToLeave.contains(file)){
                    remove.add(file);
                    fileName.remove(file);
                    rebalance1.removeFile(file);
                }
            }
        }
        System.out.println("fileNumberNow "+fileName.size());

        while (fileName.size()>up){
            remove.add(fileName.get(0));
            fileName.remove(0);
            rebalance1.removeFile(fileName.get(0));
        }
        return remove;
    }



}