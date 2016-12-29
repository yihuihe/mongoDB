import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by MCYarn on 2016/12/28.
 */
public class ConnectLocalhost {
    public static void addDataToDB (String filename) {
        try {

            long s = System.currentTimeMillis();
            //bw.write("开始查询的时间：" + s);
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            //使用数据库hyh2,集合property、relation和side
            MongoCollection<Document> property = mongoClient.getDatabase("hyh").getCollection("property");
            MongoCollection<Document> relation = mongoClient.getDatabase("hyh").getCollection("relation");
            MongoCollection<Document> side = mongoClient.getDatabase("hyh").getCollection("side");



            List<Document> property_list = new ArrayList<Document>();
            List<Document> relation_list = new ArrayList<Document>();
            List<Document> side_list = new ArrayList<Document>();

            Map<String,Set<Document>> relation_map = new HashMap<String, Set<Document>>();



            File file = new File(filename);
            BufferedReader reader = null;

            try {
                String tempString = null;
                reader = new BufferedReader(new FileReader(file));
                int line = 1;

                while ((tempString = reader.readLine()) != null) {
                    // System.out.println(line);
                    line ++;

                    String[] splitData = new String[]{};
                    splitData = tempString.split("\\s+");
                    //将数据进行一一对应
                    String srcType = splitData[1];
                    String srcValue = splitData[2];
                    String dstType = splitData[3];
                    String dstValue = splitData[4];
                    String srcId = splitData[6];
                    String dstId = splitData[7];
                    String relationType = splitData[8];
                    String relationValue = splitData[9];
                    String relationStartTime = splitData[10];
                    String relationEndTime = splitData[11];

                    if (srcType.equals("null")) {
                        System.out.println("srcType is null");
                        continue;
                    }

                    //添加relation关系表
                    Set<Document> friends  = new HashSet<Document>();
                    String key = new ObjectId(new Date()).toString();
                    Document friendship = new Document("dst_id",dstId).append("key",key);
                    boolean addOperation = friends.add(friendship);

                    if(!(relation_map.containsKey(srcId))){
                        relation_map.put(srcId,friends);
                    }
                    else{
                        Set<Document> set = relation_map.get(srcId);
                        set.add(new Document("dst_id",dstId).append("key",key));
                    }
                    // System.out.println("step ++++++++++++");
                    //添加key关系表
                    Set<Document> keyItem = new HashSet<Document>();
                    Document keyValue = new Document("relationType",relationType).append("relationValue",relationValue)
                            .append("relationStartTime",relationStartTime).append("relationEndTime",relationEndTime);
                    keyItem.add(keyValue);


                    property_list.add(new Document("id", srcId).append("val", srcValue).append("ft", srcType));
                    //relation.add(new Document("src_id", srcId).append("src_type", srcType).append("dst_id", dstId).append("dst_type", dstType));
                    side_list.add(new Document("key",key).append("keyItem",keyItem));

                    if(line %1000 ==0){
                        /*
                        for (Map.Entry<String,Set<Document>> entry : relation_map.entrySet()) {
                            //entry.getKey() ;entry.getValue(); entry.setValue();
                            //map.entrySet()  返回此映射中包含的映射关系的 Set视图。
                            relation_list.add(new Document("src_id",entry.getKey()).append("friends",entry.getValue()));
                            // System.out.println("key= " + entry.getKey() + " and value= "
                            //              + entry.getValue());
                        }
                        */
                        property.insertMany(property_list);
                        //relation.insertMany(relation_list);
                        side.insertMany(side_list);
                        property_list.clear();
                        //relation_list.clear();
                        side_list.clear();
                    }


                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
            /*
                //entry.getKey() ;entry.getValue(); entry.setValue();
                //map.entrySet()  返回此映射中包含的映射关系的 Set视图。
             */
            int count=0;
            for (Map.Entry<String,Set<Document>> entry : relation_map.entrySet()) {
                FindIterable<Document> Item = relation.find(new Document("src_id",entry.getKey()));
                if(relation.count(new Document("src_id",entry.getKey()))!=0) {
                    Item.forEach(new Block<Document>() {
                        @Override
                        public void apply(Document document) {
                            Set<Document> temp = new HashSet<Document>();
                            temp.addAll((Collection<? extends Document>) document.get("friends"));
                            Iterator<Document> its = entry.getValue().iterator();
                            while (its.hasNext()) {
                                temp.add(its.next());
                            }
                            relation.findOneAndReplace(new Document("src_id", entry.getKey()), new Document("src_id", entry.getKey()).append("friends", temp));
                        }
                    });
                }
                else {
                   relation_list.add(new Document("src_id", entry.getKey()).append("friends", entry.getValue()));
                    // System.out.println("key= " + entry.getKey() + " and value= "
                    //              + entry.getValue());
                }
            }

            property.insertMany(property_list);
            side.insertMany(side_list);
            if(relation_list.size()>0)
            {
                relation.insertMany(relation_list);
            }
/*
            //创建索引
            property.createIndex(new Document("id",1));
            relation.createIndex(new Document("src_id",1).append("unique",true));
            side.createIndex(new Document("key",1));
*/
            mongoClient.close();
            long e = System.currentTimeMillis();
           // bw.write("结束查询的时间：" + e);
            //bw.newLine();
            long d = (e-s);
           // bw.write("消耗时间：" + d);
            System.out.println("插入一个文件的入库时间：" + d);
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ":" + e.getMessage());
        }
    }

    public static void main (String[] args){

        //addDataToDB("testFile1.nb");
        //addDataToDB("testFile2.nb");
        addDataToDB("testFile3.nb");

        //addDataToDB("add.txt");
        //addDataToDB("test.txt");
        //addDataToDB("teat2.txt");

        //collection2.createIndex(new Document("src_id",1).append("src_type",1));


    }

}
