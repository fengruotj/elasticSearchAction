package com.basic.elasticSearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * locate com.basic.elasticSearch
 * Created by mastertj on 2018/3/22.
 */
public class TestMultiSearch {

    private static String host="ubuntu2"; // 服务器地址
    private static int port=9300; // 端口
    private TransportClient client=null;

    public static final String CLUSTER_NAME="bigdata";

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
     * 多个条件查询
     */
    @Test
    public void serachMulti(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("title", "战"))
                .must(QueryBuilders.matchPhraseQuery("content", "星球")))
                .execute()
                .actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 多个条件查询
     */
    @Test
    public void serachMulti2(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("title", "战"))
                .mustNot(QueryBuilders.matchPhraseQuery("content", "武士")))
                .execute()
                .actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 多个条件查询
     * should根据scoure权重得出结果
     */
    @Test
    public void serachMulti3(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("title", "战"))
                .should(QueryBuilders.matchQuery("content","星球"))
                .should(QueryBuilders.rangeQuery("publishDate").gte("2018-01-01")))
                .execute()
                .actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getScore());
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * filter过滤
     * price必须小于40
     */
    @Test
    public void serachFilter(){
        SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("title", "战"))
                .filter(QueryBuilders.rangeQuery("price").lte(40)))
                .execute()
                .actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getScore());
            System.out.println(hit.getSourceAsString());
        }
    }
}
