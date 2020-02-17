package day200120;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 四 IK分词器
 * 4.2.2 JavaAPI操作
 */
public class AnalysisIk {
    //es的client
    private TransportClient client;

    /**
     * 3.1.2 获取Transport Client
     * （1）ElasticSearch服务默认端口9300。
     * （2）Web管理平台端口9200。
     * （3）显示log4j2报错，在resource目录下创建一个文件命名为log4j2.xml并添加如下内容
     */
    @Before
    public void getClient() throws UnknownHostException {
        //1.设置连接集群的名称
        Settings settings = Settings.builder().put("cluster.name", "tosin-application").build();

        //2.连接集群
        client = new PreBuiltTransportClient(settings);

        //9300是API连接ES的端口
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bd-01-01"), 9300));
        //3 打印集群名称
        System.out.println(client.toString());
    }


    /**
     * 1）创建索引
     */
    @Test
    public void createIndex(){
        //创建索引(数据库)
        client.admin().indices().prepareCreate("blog4").get();
        //关闭资源
        client.close();
    }

    /**
     * 2）创建mapping
     * 创建使用ik分词器的mapping
     *
     * {
     *     "mappings": {
     *         "article": {
     *             "properties": {
     *                 "id1": {
     *                     "analyzer": "ik_smart",
     *                     "store": true,
     *                     "type": "text"
     *                 },
     *                 "title2": {
     *                     "analyzer": "ik_smart",
     *                     "type": "text"
     *                 },
     *                 "content": {
     *                     "analyzer": "ik_smart",
     *                     "type": "text"
     *                 }
     *             }
     *         }
     *     }
     * }
     */
    @Test
    public void createMapping() throws ExecutionException, InterruptedException, IOException {
        //1设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                    .startObject("properties")
                    .startObject("id1")
                //6版本 Caused by: RemoteTransportException[[node-1][192.168.0.241:9300][indices:admin/mapping/put]]; nested: MapperParsingException[No handler for type [string] declared on field [id1]];
//                        .field("type", "string")
                        .field("type", "text")
                //6版本 Caused by: RemoteTransportException[[node-1][192.168.0.241:9300][indices:admin/mapping/put]]; nested: IllegalArgumentException[Could not convert [id1.store] to boolean]; nested: IllegalArgumentException[Failed to parse value [yes] as only [true] or [false] are allowed.];
//                        .field("store", "yes")
                        .field("store", "true")
                        .field("analyzer", "ik_smart")
                    .endObject()
                    .startObject("title2")
                        .field("type", "text")
                        .field("store", "false")
                        .field("analyzer", "ik_smart")
                    .endObject()
                    .startObject("content")
                        .field("type", "text")
                        .field("store", "false")
                        .field("analyzer", "ik_smart")
                    .endObject()
                    .endObject()
                    .endObject()
                .endObject();
        // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog4").type("article").source(builder);
        client.admin().indices().putMapping(mapping).get();

        // 3 关闭资源
        client.close();
    }

    /**
     * 3）插入数据
     */
    @Test
    public void createDocumentByMap(){
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id1", "2");
        json.put("title2", "Lucene");
        json.put("content", "它提供了一个分布式的web接口");

        //2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog4", "article", "3")
                .setSource(json).execute().actionGet();

        //3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:"+indexResponse.getType());
        System.out.println("id:"+indexResponse.getId());
        System.out.println("version:"+indexResponse.getVersion());
        System.out.println("result:"+indexResponse.getResult());
        //4 关闭连接
        client.close();
    }

    /**
     * 4 词条查询
     */
    @Test
    public void termQuery(){
        //1 第一field查询
        SearchResponse searchResponse = client.prepareSearch("blog4").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "提供")).get();

        //2 打印查询结果 获取命中次数，查询结果有多少对
        SearchHits hits = searchResponse.getHits();
        // 获取命中次数，查询结果有多少对
        System.out.println("查询结果有："+hits.getTotalHits()+"条");

        for(SearchHit hit: hits){
            System.out.println(hit.getSourceAsString());
        }

        //3 关闭连接
        client.close();
    }
}
