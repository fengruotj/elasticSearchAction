package com.basic.elasticSearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * locate com.basic.elasticSearch
 * Created by mastertj on 2018/3/22.
 * 基于Smartcn分词器的部分查询
 */
public class TestPartSearch {
    private static String host="ubuntu2"; // 服务器地址
    private static int port=9300; // 端口
    private TransportClient client=null;

    public static final String CLUSTER_NAME="bigdata";
    public static final String ANALYZER="smartcn";

    public static Settings.Builder settings=Settings.builder().put("cluster.name",CLUSTER_NAME);
    /**
     * 获取客户端
     * @throws Exception
     */
    @Before
    public void getClinet()throws Exception{
        client = new PreBuiltTransportClient(settings.build())
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
     * 分词查询
     */
    @Test
    public void serachByCondition(){
        //SearchRequestBuilder srb=client.prepareSearch("film_smartcn").setTypes("dongzuo");
        SearchRequestBuilder srb=client.prepareSearch("film_smartcn").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("title","最后狼").analyzer(ANALYZER))
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 多字段查询
     */
    @Test
    public void serachMultipe(){
        SearchRequestBuilder srb=client.prepareSearch("film_smartcn").setTypes("dongzuo");
        SearchResponse sr=srb.setQuery(QueryBuilders.multiMatchQuery("非洲星球铁拳","title","content").analyzer(ANALYZER))
                .execute().actionGet(); // 查询所有
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
    }
}
