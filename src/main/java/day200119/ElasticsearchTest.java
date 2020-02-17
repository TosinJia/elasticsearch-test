package day200119;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 3.1 API基本操作
 *
 */
public class ElasticsearchTest {
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
     * 3.1.3 创建索引
     * 查看索引
     * 方式1 http://bd-01-03:9200/blog2
     * 方式2
     *   信息
     *    索引狀態
     *    索引信息
     */
    @Test
    public void createIndex_blog(){
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();
        // 2 关闭连接
        client.close();
    }

    /**
     * 3.1.4 删除索引
     *
     * 查看索引
     * 方式1 http://bd-01-03:9200/blog2
     */
    @Test
    public void deleteIndex(){
        // 1 删除索引
        client.admin().indices().prepareDelete("blog2").get();
        // 2 关闭连接
        client.close();
    }

    /**
     * 3.1.5 新建文档 方式一（源数据json串）
     * 可以先創建索引blog
     * 当直接在ElasticSearch建立文档对象时，如果索引不存在的，默认会自动创建，映射采用默认方式
     * http://bd-01-03:9200/blog/article/1
     * 插件
     *   - 數據瀏覽
     *     - 点击对应的数据
     */
    @Test
    public void createDocumentByJson(){
        //1 文档数据准备
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器 \"," + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";
        //2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1")
                .setSource(json).execute().actionGet();
        //3 打印返回的结果
        System.out.println("index:"+indexResponse.getIndex());
        System.out.println("type:"+indexResponse.getType());
        System.out.println("id:"+indexResponse.getId());
        System.out.println("version:"+indexResponse.getVersion());
        System.out.println("result:"+indexResponse.getResult());
        //4 关闭连接
        client.close();
    }

    /**
     * 3.1.6 新建文档 方式二（源数据map方式添加json）
     * 插件
     *   - 數據瀏覽
     *     - 刷新
     *       - 点击对应的数据
     */
    @Test
    public void createDocumentByMap(){
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web 接口");

        //2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2")
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
     * 3.1.7 新建文档 方式三（源数据es构建器添加json）
     */
    @Test
    public void createDocumentByXContent() throws IOException {
        //1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("id", 4)
                .field("title", "基于 Lucene的搜索服务器")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于 RESTful web接口。")
                .endObject();

        //2 创建文档
//        IndexResponse indexResponse = client.prepareIndex("blog", "article", "3")
//                .setSource(builder).get();
        //没有设置ID，会自动生成 "_id": "x521TXABuuObJXY2qnMI"
        IndexResponse indexResponse = client.prepareIndex("blog", "article")
                .setSource(builder).get();

        //3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        //4 关闭连接
        client.close();
    }

    /**
     * 3.1.8 搜索文档数据（单个索引）
     */
    @Test
    public void getData(){
        //1 查询文档
        GetResponse response = client.prepareGet("blog", "article", "5").get();

        //2 打印搜索的结果
        System.out.println(response.getSourceAsString());

        //3 关闭连接
        client.close();
    }

    /**
     * 3.1.9 搜索文档数据（多个索引）
     */
    @Test
    public void getMultiData(){
        //1 查询多个文档
        MultiGetResponse response = client.prepareMultiGet()
                .add("blog", "article", "1")
                .add("blog", "article", "2", "3")
                .add("blog", "article", "x521TXABuuObJXY2qnMI").get();

        //2 遍历返回的结果
        for(MultiGetItemResponse itemResponse: response){
            GetResponse getResponse = itemResponse.getResponse();
            // 如果获取到查询结果
            if(getResponse.isExists()){
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
        //3 关闭资源
        client.close();
    }

    /**
     * 3.1.10 更新文档数据（update）
     * 插件
     *   - 數據瀏覽
     *     - 刷新
     *       - 点击对应的数据
     */
    @Test
    public void updateData() throws IOException, ExecutionException, InterruptedException {
        //1 创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("3");

        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器update")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于 RESTful web接口。大数据前景无限update")
                .field("createDate", "2020-01-19").endObject()
            );

        //2 获取更新后的值
        UpdateResponse updateResponse = client.update(updateRequest).get();

        //3 打印返回的结果
        System.out.println("index:" + updateResponse.getIndex());
        System.out.println("type:" + updateResponse.getType());
        System.out.println("id:" + updateResponse.getId());
        System.out.println("version:" + updateResponse.getVersion());
        System.out.println("result:" + updateResponse.getResult());

        //4 关闭连接
        client.close();
    }

    /**
     * 3.1.11 更新文档数据（upsert）
     * 设置查询条件, 查找不到则添加IndexRequest内容，查找到则按照UpdateRequest更新。
     * 第一次执行
     *   http://bd-01-03:9200/blog/article/5
     *   getData {"title":"搜索服务器","content":"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。 Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引 擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。"}
     * 第二次执行
     *   getData {"title":"搜索服务器","content":"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。 Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引 擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。","user":"tosin"}
     */
    @Test
    public void testUpsert() throws IOException, ExecutionException, InterruptedException {
        //设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest("blog", "article", "5")
                .source(XContentFactory.jsonBuilder().startObject()
                .field("title", "搜索服务器")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。 Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引 擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject());

        //设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("blog", "article", "5")
                .doc(XContentFactory.jsonBuilder().startObject()
                .field("user", "tosin").endObject()).upsert(indexRequest);

        client.update(upsert).get();
        client.close();
    }

    /**
     * 3.1.12 删除文档数据（prepareDelete）
     */
    @Test
    public void deleteData(){
        //1 删除文档数据
        DeleteResponse deleteResponse = client.prepareDelete("blog", "article", "5").get();

        //2 打印返回的结果
        System.out.println("index:" + deleteResponse.getIndex());
        System.out.println("type:" + deleteResponse.getType());
        System.out.println("id:" + deleteResponse.getId());
        System.out.println("version:" + deleteResponse.getVersion());
        System.out.println("result:" + deleteResponse.getResult());

        //3 关闭连接
        client.close();
    }

    /**
     * 3.2.1 查询所有（matchAllQuery）
     */
    @Test
    public void matchAllQuery(){
        //1 执行查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery()).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits();
        // 获取命中次数，查询结果有多少对
        System.out.println("查询结果有："+hits.getTotalHits()+"条");

        //打印出每条结果
        for(SearchHit hit :hits){
            System.out.println(hit.getSourceAsString());
        }

        // 3 关闭连接
        client.close();
    }

    /**
     * 3.2.2 对所有字段分词查询（queryStringQuery）
     * 类似like
     */
    @Test
    public void query(){
        //1 条件查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("全文")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits();
        // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有："+hits.getTotalHits()+"条");

        for(SearchHit hit: hits){
            System.out.println(hit.getSourceAsString());
        }
        // 3 关闭连接
        client.close();
    }

    /**
     * 3.2.3 通配符查询（wildcardQuery） 需要分词
     * * ：表示多个字符（0个或多个字符）
     * ？：表示单个字符
     * 需要分词器
     *   *全* 结果为4条
     *   *全文* 结果为0条
     */
    @Test
    public void wildcardQuery(){
        //1 通配符查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*全*")).get();

        //2 打印查询结果
        SearchHits hits = searchResponse.getHits();
        // 获取命中次数，查询结果有多少对
        System.out.println("查询结果有："+hits.getTotalHits()+"条");

        for(SearchHit hit: hits){
            System.out.println(hit.getSourceAsString());
        }

        //3 关闭连接
        client.close();
    }

    /**
     * 3.2.4 词条查询（TermQuery） 类似=
     * 需要分词器
     *   全文 4条
     *   全文 0条
     */
    @Test
    public void termQuery(){
        //1 第一field查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "全")).get();

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

    /**
     * 3.2.5 模糊查询（fuzzy）
     * 需要分词器
     *   lucene 4条
     *   基 4条
     *   基于 0条
     */
    @Test
    public void fuzzy(){
        //1 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "基")).get();

        //2 打印查询结果 获取命中次数，查询结果有多少对
        SearchHits hits = searchResponse.getHits();
        // 获取命中次数，查询结果有多少对
        System.out.println("查询结果有："+hits.getTotalHits()+"条");
//        for(SearchHit hit: hits){
//            System.out.println(hit.getSourceAsString());
//        }
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            // 获取字符串格式打 印
            System.out.println(searchHit.getSourceAsString());
        }

        //3 关闭连接
        client.close();
    }

}
