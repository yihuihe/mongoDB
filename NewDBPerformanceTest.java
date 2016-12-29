import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by MCYarn on 2016/12/28.
 */
public class NewDBPerformanceTest {
    public static void main (String[] args) throws IOException {
        try {
            //连接数据库
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            //MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress("192.168.0.23:40000")),
             //       Arrays.asList(MongoCredential.createCredential("asideal", "admin", "1q2w3e4r".toCharArray())));
            //使用数据库hyh3,并操作其中的集合
            MongoCollection<Document> property = mongoClient.getDatabase("hyh").getCollection("property");
            MongoCollection<Document> relation = mongoClient.getDatabase("hyh").getCollection("relation");
            MongoCollection<Document> side = mongoClient.getDatabase("hyh").getCollection("side");
            //collection.createIndex(new Document("src_id",1).append("unique",true));


            List<Document> Result = new ArrayList<Document>();
            List<Document> oneItem = new ArrayList<Document>();
            List<Document> nodes = new ArrayList<Document>();
            List<Document> edges = new ArrayList<Document>();


            ArrayList<String> two_level_result = new ArrayList<String>();

            List<String> Path = new ArrayList<String>();
            List<String> Side = new ArrayList<String>();
            String startPoint = "1336235";
            Path.add(startPoint);

            File fileResult = new File("TwoLevel.txt");
            FileWriter fw = new FileWriter(fileResult, true);
            BufferedWriter bw = new BufferedWriter(fw);
            //查询操作。
            // Date date = new Date();
            //DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

            bw.write(startPoint);
            bw.newLine();
            long s = System.currentTimeMillis();
            bw.write("开始查询的时间：" + s);
            bw.newLine();

            FindIterable<Document> findIterable1 = relation.find(new Document("src_id", startPoint));
            //FindIterable<Document> findIterable1 = collection.find(new Document("dst_type","1020008").append("src_type","1030001"));
            // String timeEnd1 = format.format(date);

            //显示查询结果
            HashSet destination1 = new HashSet();
            Map<String,String> side1 = new HashMap<String, String>();
            //System.out.println("这是第一次查询的结果");
            findIterable1.forEach(new Block<Document>() {
                @Override
                public void apply(Document document) {
                   // System.out.println(document.toJson());
                    //destination1.add(document.get("friends"));
                    Set<Document> findResult1 = new HashSet<Document>();
                    document.get("friends");
                   // System.out.println(document.get("friends"));
                    findResult1.addAll((Collection<? extends Document>) document.get("friends"));
                    //System.out.println(findResult1);
                    Iterator<Document> its = findResult1.iterator();
                    while (its.hasNext()){
                         Document s = its.next();
                      // System.out.println(s);
                        destination1.add(s.get("dst_id"));
                        side1.put(String.valueOf(s.get("dst_id")),String.valueOf(s.get("key")));
                    }
                }
            });


            int count = 1;
            HashSet destination2 = new HashSet();
            //二次查询
            for (Iterator it = destination1.iterator(); it.hasNext(); ) {
                String searchId = it.next().toString();
                if (Path.size() > 1) {
                    Side.remove(0);
                    Path.remove(1);
                    Path.add(searchId);
                    Side.add(side1.get(searchId));
                } else {
                    Path.add(searchId);
                    Side.add(side1.get(searchId));
                }
                //记录用于写入文件保存结果路径
                // String relationshipDathSecondTemp = relationshipDathSecond + count + searchId;
                // String timeStart2 = format.format(date);
                FindIterable<Document> findIterable2 = relation.find(new Document("src_id", searchId));

                //System.out.println("这是第二次查询的第" + count + "组数据的结果");
                Map<String,String> side2 = new HashMap<String, String>();
                findIterable2.forEach(new Block<Document>() {
                    @Override
                    public void apply(Document document) {

                       // System.out.println(document.toJson());
                        Set<Document> findResult2 = new HashSet<Document>();
                        document.get("friends");
                       // System.out.println(document.get("friends"));
                        findResult2.addAll((Collection<? extends Document>) document.get("friends"));
                      //  System.out.println(findResult2);
                        Iterator<Document> its = findResult2.iterator();
                        while (its.hasNext()){
                            Document s = its.next();
                           // System.out.println(s);
                            destination2.add(s.get("dst_id"));
                            side2.put(String.valueOf(s.get("dst_id")),String.valueOf(s.get("key")));
                            // System.out.println(s.get("dst_id"));
                            if (Path.size() > 2) {
                                Side.remove(1);
                                Path.remove(2);
                                Path.add(s.get("dst_id").toString());
                                Side.add(side2.get(s.get("dst_id").toString()));
                            } else{
                                Path.add(s.get("dst_id").toString());
                                Side.add(side2.get(s.get("dst_id").toString()));
                            }

                        }
                        //将两层关系写入文件
                        String context = new String();
                        context = "";
                        long searchPointStart = System.currentTimeMillis();
                        for (int i = 0; i < Path.size(); i++) {
                            context += Path.get(i) + " ";
                            FindIterable<Document> pointTemp = property.find(new Document("id",Path.get(i))).limit(1);
                            Iterator<Document> ii = pointTemp.iterator();
                            while (ii.hasNext()){
                                Document point = ii.next();
                                nodes.add(point);
                            }
                        }
                        long searchPointEnd = System.currentTimeMillis();
                        long searchPointTime = (searchPointEnd - searchPointStart);
                        System.out.println("查询节点属性所花的时间：" + searchPointTime);

                        long searchEdgeStart = System.currentTimeMillis();
                        for (int j = 0;j<Side.size();j++){
                            FindIterable<Document> sideTemp = side.find(new Document("key",Side.get(j))).limit(1);
                            Iterator<Document> jj = sideTemp.iterator();
                            while(jj.hasNext()){
                                Document side = jj.next();
                                edges.add(side);
                            }
                        }
                        long searchEdgeEnd = System.currentTimeMillis();
                        long searchEdgeTime = (searchEdgeEnd - searchEdgeStart);
                        System.out.println("查询边属性所花的时间：" + searchEdgeTime);

                        oneItem.add(new Document("start_point",startPoint).append("nodes",nodes).append("edges",edges));
                        Result.add(new Document("Datas",oneItem));

                        System.out.println(Result.toString());
                        try {

                            bw.write(context);
                            bw.newLine();

                        } catch (IOException ewrite) {
                            ewrite.printStackTrace();
                        }
                       // System.out.println(context);
                        context = "";
                       // System.out.println(document.toJson());
                    }
                });
                count++;
                if(Path.size()==3){
                    Path.remove(2);
                }
                if(Side.size()==2){
                    Side.remove(1);
                }
                /*
                for (int j = 1; j < Path.size(); j++) {
                    Path.remove(j);
                }
                for(int k = 0;k<Side.size();k++){
                    Side.remove(k);
                }
                */
            }
            long e = System.currentTimeMillis();
            bw.write("结束查询的时间：" + e);
            bw.newLine();
            long d = (e-s);
            bw.write("消耗时间：" + d);
            System.out.println("二层关系总查询时间：" + d);
            bw.newLine();
            bw.newLine();
            bw.newLine();
            count = 1;
            Path.remove(0);
                        /*
                        for(Iterator its = destination2.iterator(); its.hasNext();){
                            System.out.println(its.next().toString() + "Note Note Note Note Note Note Note Note");

                        }
                        */
            int countResult = 0;
            if (destination2.size() > 0) {

                for (Iterator its = destination2.iterator(); its.hasNext(); ) {
                    countResult++;
                    two_level_result.add(its.next().toString());
                }
            }
            //  bw.write("二次查询有下一层关系的节点有几个：" + countResult);
            //  bw.newLine();
            //  System.out.println("二次查询有下一层关系的节点有几个：" + countResult);
            //     if(countResult !=0) {
            //          for (int k = 0; k < two_level_result.size(); k++) {
            //             System.out.print(two_level_result.get(k));
            //         }
            //     }
            countResult = 0;
            bw.flush();
            bw.close();
            mongoClient.close();
        }catch(Exception e){
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }
}
