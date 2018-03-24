package com.basic.elasticSearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

/**
 * locate com.basic.elasticSearch
 * Created by mastertj on 2018/3/22.
 */
public class TestSearch {

    private static String host="ubuntu2"; // 服务器地址
    private static int port=9300; // 端口
    private TransportClient client=null;

    public static final String CLUSTER_NAME="bigdata";

    //增加elascticsearch X-pack安全认证
    public static Settings.Builder settings=Settings.builder()
            .put("cluster.name",CLUSTER_NAME)
            .put("client.transport.sniff", true)
            .put("xpack.security.transport.ssl.enabled", false)
            .put("xpack.security.user", "elastic:changeme");

    /**
     * 获取客户端
     * @throws Exception
     */
    @Before
    public void getClinet()throws Exception{
        //获取 elascticsearch X-pack安全认证客户端
        client = new PreBuiltXPackTransportClient(settings.build())
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port));
        System.out.println(client);
    }

    /**
     * 关闭连接
     */
    @After
    public void close(){
        if(client!=null)
            client.close();
    }

    /**
     * 查询所有电影
     * @throws Exception
     */
    @Test
    public void searchAll()throws Exception{
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
            Map<String, Object> source = hit.getSource();
            for (Map.Entry<String, Object> stringObjectEntry : source.entrySet()) {
                System.out.println(stringObjectEntry.getKey()+" : "+stringObjectEntry.getValue());
            }
            System.out.println();
        }
    }

    /**
     * 分页查询所有电影
     * @throws Exception
     */
    @Test
    public void searchpaging()throws Exception{
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0)
                .setSize(3)
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
            Map<String, Object> source = hit.getSource();
            for (Map.Entry<String, Object> stringObjectEntry : source.entrySet()) {
                System.out.println(stringObjectEntry.getKey()+" : "+stringObjectEntry.getValue());
            }
            System.out.println();
        }
    }

    /**
     * 排序查询
     */
    @Test
    public void serachSortPaging(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0)
                .setSize(3)
                .addSort("publishDate", SortOrder.ASC)
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 数据列过滤
     */
    @Test
    public void serachInclude(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
                .setFetchSource(new String[]{"title","price"},null)
                .addSort("publishDate",SortOrder.ASC)
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }


    /**
     * 条件查询
     */
    @Test
    public void serachByCondition(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("title","战"))
                .addSort("publishDate",SortOrder.ASC)
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 条件查询(高亮显示)
     */
    @Test
    public void serachHeightlightCondition(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.preTags("<strong><font>");
        highlightBuilder.postTags("</font></strong>");
        highlightBuilder.field("title");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("title","战"))
                .highlighter(highlightBuilder)
                .addSort("publishDate",SortOrder.ASC)
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
            System.out.println(hit.getHighlightFields());
        }
    }
}
