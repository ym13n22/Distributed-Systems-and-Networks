import java.util.*;

public class Test {
    private Set<String> setStored = new HashSet<>();

    public static void main(String[] args) {
        Test main = new Test();
        main.setStored.add("file1");
        main.setStored.add("file2");
        main.setStored.add("file3");
        main.setStored.add("file4");
        int port=1025;
        List<String> redundant=new ArrayList<>();
        redundant.add("File1.txt");
        redundant.add("File4.txt");
        redundant.add("File5.txt");
        redundant.add("File6.txt");
        redundant.add("small_file.jpg");
        redundant.add(" File2.txt");
        List<List<String>> rebalanceList=new ArrayList<>();
        List<String> port1=new ArrayList<>();
        port1.add("1026");
        port1.add("File4.txt");
        port1.add("File5.txt");
        port1.add("File6.txt");
        port1.add("small_file.jpg");
        port1.add(" File2.txt");
        List<String> port2=new ArrayList<>();
        port2.add("1027");
        port2.add("File4.txt");
        port2.add("File5.txt");
        port2.add("File6.txt");
        List<String> port3=new ArrayList<>();
        port3.add("1028");
        port3.add("File4.txt");
        port3.add("File5.txt");
        port3.add("File6.txt");
        port3.add("small_file.jpg");

        rebalanceList.add(port1);
        rebalanceList.add(port2);
        rebalanceList.add(port3);
       main. gieSend(rebalanceList,port,redundant);
        //main.giveBalance(5, 3); // 示例参数：up = 3, low = 2, storeNum = 2, r = 2
    }

    private void giveBalance(int storeNum, int r) {
        List<List<String>> storeList = new ArrayList<>();
        for (int i = 0; i < storeNum; i++) {
            storeList.add(new ArrayList<>());
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
    }

    private void gieSend(List<List<String>> rebalanceList,int port,List<String> redundant){
        System.out.println("rebalanceList "+rebalanceList);
        System.out.println("port "+port);
        System.out.println("redundant "+redundant);
        List<HashMap<String,Integer>> sendList=new ArrayList<>();
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
                        System.out.println("Send "+sendPair);

                    }
                }
            }
        }

   for(String re:redundant){
       if(!send.contains(re)){
          remove.add(re);
       }
   }

        System.out.println("Send List "+sendList);
        System.out.println("Remove "+remove);
    }
}
