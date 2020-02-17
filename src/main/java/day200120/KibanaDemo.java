package day200120;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * Kibana测试
 *
 */
public class KibanaDemo {
    //获取es的client
    private static TransportClient client = null;
    static{
        //1.设置连接集群的名称
        Settings settings = Settings.builder().put("cluster.name", "tosin-application").build();
        //2.连接集群
        client = new PreBuiltTransportClient(settings);
        //9300是API连接ES的端口
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bd-01-01"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    //读取数据
    public static ArrayList<String> readJson(String file) throws IOException {
        ArrayList<String> list = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
        String temp;
        while((temp = reader.readLine()) != null){
            list.add(temp);
        }
        return list;
    }

    /**
     * 创建Index
     */
    public static void createIndex(){
        client.admin().indices().prepareCreate("school").get();
    }
    /**
     * 创建mapping，对新建的Index创建，如果新的Index中有数据，是不创建
     * */
    public static void createMappingIK() throws IOException, ExecutionException, InterruptedException {
        //设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("s_level")
                .field("type", "text")
                .field("store", "true")
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("s_city")
                .field("type", "text")
                .field("store", "true")
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("s_no")
                .field("type", "text")
                .field("store", "true")
                .endObject()
                .startObject("s_province").field("type", "text").field("store", "true").field("analyzer", "ik_smart").endObject()
                .startObject("s_name").field("type", "text").field("store", "true").field("analyzer", "ik_smart").endObject()

                .endObject()
                .endObject()
                .endObject();
        //添加mapping到Index
        PutMappingRequest mappingRequest = Requests.putMappingRequest("school").type("article").source(builder);
        client.admin().indices().putMapping(mappingRequest).get();
    }

    /**
     * XContent方式创建Document
     * */
    public static void createXContentDocument() throws IOException {
        ArrayList<String> jsonList = readJson("E:\\tosin\\DevelopmentEnvironment\\workspaces\\IdeaProjects\\elasticsearch-test\\data\\全国高校信息.json");

        for(String json: jsonList){
            JSONObject jsonObject = JSON.parseObject(json);
            String s_level = jsonObject.getString("s_level");
            String s_city = jsonObject.getString("s_city");
            String s_no = jsonObject.getString("s_no");
            String s_province = jsonObject.getString("s_province");
            String s_name = jsonObject.getString("s_name");

            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .field("s_level", s_level)
                    .field("s_city", s_city)
                    .field("s_no", s_no)
                    .field("s_province", s_province)
                    .field("s_name", s_name)
                    .endObject();
            //创建文档
            IndexResponse indexResponse = client.prepareIndex("school", "article")
                    .setSource(builder).execute().actionGet();
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        createIndex();
        createMappingIK();
        createXContentDocument();
        client.close();
    }
}
