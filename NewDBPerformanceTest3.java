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
public class NewDBPerformanceTest3 {
    public static void main (String[] args) throws IOException {
        try {
            //连接数据库
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
           /// MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress("192.168.0.23:40000")),
              //      Arrays.asList(MongoCredential.createCredential("asideal", "admin", "1q2w3e4r".toCharArray())));
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

            File fileResult = new File("ThreeLevel.txt");
            FileWriter fw = new FileWriter(fileResult, true);
            BufferedWriter bw = new BufferedWriter(fw);

            bw.write(startPoint);
            bw.newLine();
            long s = System.currentTimeMillis();
            bw.write("开始查询的时间：" + s);
            bw.newLine();

            FindIterable<Document> findIterable1 = relation.find(new Document("src_id", startPoint));

            //显示查询结果
            HashSet destination1 = new HashSet();
            Map<String,String> side1 = new HashMap<String, String>();
           // System.out.println("这是第一次查询的结果");
            findIterable1.forEach(new Block<Document>() {
                @Override
                public void apply(Document document) {
                    Set<Document> findResult1 = new HashSet<Document>();
                    findResult1.addAll((Collection<? extends Document>) document.get("friends"));
                    Iterator<Document> its = findResult1.iterator();
                    while (its.hasNext()){
                        Document s = its.next();
                        destination1.add(s.get("dst_id"));
                        side1.put(String.valueOf(s.get("dst_id")),String.valueOf(s.get("key")));
                    }
                }
            });


            HashSet destination2 = new HashSet();
            //二次查询
            for (Iterator it = destination1.iterator(); it.hasNext(); ) {
                String searchId = it.next().toString();
                if (Path.size() > 1) {
                    Side.remove(0);
                    Path.remove(1);
                    Path.add(searchId);
                    Side.add(side1.get(searchId));
                } else{
                    Path.add(searchId);
                    Side.add(side1.get(searchId));
                }

                FindIterable<Document> findIterable2 = relation.find(new Document("src_id", searchId));

                Map<String,String> side2 = new HashMap<String, String>();
                findIterable2.forEach(new Block<Document>() {
                    @Override
                    public void apply(Document document) {
                        Set<Document> findResult2 = new HashSet<Document>();
                        findResult2.addAll((Collection<? extends Document>) document.get("friends"));
                        //  System.out.println(findResult2);
                        Iterator<Document> its = findResult2.iterator();
                        while (its.hasNext()){
                            Document s = its.next();
                            destination2.add(s.get("dst_id"));
                            side2.put(String.valueOf(s.get("dst_id")),String.valueOf(s.get("key")));

                            if (Path.size() > 2) {
                                Side.remove(1);
                                Path.remove(2);
                                Path.add(s.get("dst_id").toString());
                                Side.add(side2.get(s.get("dst_id").toString()));
                            } else{
                                Path.add(s.get("dst_id").toString());
                                Side.add(side2.get(s.get("dst_id").toString()));
                            }

                            String searchId3 = s.get("dst_id").toString();
                            HashSet destination3 = new HashSet();
                            Map<String,String> side3 = new HashMap<String, String>();
                            FindIterable<Document> findIterable3 = relation.find(new Document("src_id", searchId3));
                            findIterable3.forEach(new Block<Document>() {
                                @Override
                                public void apply(Document document) {
                                    Set<Document> findResult3 = new HashSet<Document>();
                                    findResult3.addAll((Collection<? extends Document>) document.get("friends"));
                                    Iterator<Document> its3 = findResult3.iterator();
                                    while (its3.hasNext()){
                                        Document s3 = its3.next();
                                        destination3.add(s3.get("dst_id"));
                                        side3.put(String.valueOf(s3.get("dst_id")),String.valueOf(s3.get("key")));


                                        if (Path.size() > 3) {
                                            Side.remove(2);
                                            Path.remove(3);
                                            Path.add(s3.get("dst_id").toString());
                                            Side.add(side3.get(s3.get("dst_id").toString()));
                                        } else{
                                            Path.add(s3.get("dst_id").toString());
                                            Side.add(side3.get(s3.get("dst_id").toString()));
                                        }
                                        //将三层关系写入文件
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

                                    }
                                }
                            });
                            if(Path.size()==4){
                                Path.remove(3);
                            }
                            if(Side.size()==3){
                                Side.remove(2);
                            }
                            /*
                            while(Path.size()>2){
                                Path.remove(Path.indexOf(Path.size()-1));
                            }
                            while(Side.size()>1){
                                Side.remove(Side.indexOf(Side.size()-1));
                            }
                            */
                        }
                    }
                });
                if(Path.size()==3){
                    Path.remove(2);
                }
                if(Side.size()==2){
                    Side.remove(1);
                }

                /*
                while(Path.size()>1){
                    Path.remove(Path.indexOf(Path.size()-1));
                }
                while(Side.size()>0){
                    Side.remove(Side.indexOf(Side.size()-1));
                }
                */

            }

            long e = System.currentTimeMillis();

            bw.write("结束查询的时间：" + e);
            bw.newLine();
            long d = (e-s);
            bw.write("消耗时间：" + d);
            System.out.println("三层关系总查询时间：" + d);
            bw.newLine();
            bw.newLine();
            bw.newLine();
            Path.remove(0);


            bw.flush();
            bw.close();
            mongoClient.close();
        }catch(Exception e){
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }
}
