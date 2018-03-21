package com.basic.elasticSearch;

import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * locate com.basic.elasticSearch
 * Created by mastertj on 2018/3/21.
 * java连接elasticSearch集群
 */
public class TestConn {
    private static String host="ubuntu2"; // 服务器地址
    private static int port=9300; // 端口
    private TransportClient client=null;

    public static final String CLUSTER_NAME="bigdata";

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
     * 创建索引 添加文档
     */
    @Test
    public void testIndex(){
        JsonObject jsonObject=new JsonObject();
        jsonObject.addProperty("name","Java 编程思想");
        jsonObject.addProperty("publishDate","2012-11-11");
        jsonObject.addProperty("price","100");

        IndexResponse indexResponse = client.prepareIndex("book", "java", "1")
                .setSource(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引名称:"+indexResponse.getIndex());
        System.out.println("索引类型:"+indexResponse.getType());
        System.out.println("索引Id:"+indexResponse.getId());
        System.out.println("当前实例状态："+indexResponse.status());
    }

    /**
     * 根据id获取文档
     */
    @Test
    public void testGet(){
        GetResponse getFields = client.prepareGet("book", "java", "1").get();
        System.out.println(getFields.getSourceAsString());
    }

    /**
     * 根据id修改文档
     * @throws Exception
     */
    @Test
    public void testUpdate()throws Exception{
        JsonObject jsonObject=new JsonObject();
        jsonObject.addProperty("name", "java编程思想2");
        jsonObject.addProperty("publishDate", "2012-11-12");
        jsonObject.addProperty("price", 102);

        UpdateResponse response=client.prepareUpdate("book", "java", "1").setDoc(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("索引名称："+response.getIndex());
        System.out.println("类型："+response.getType());
        System.out.println("文档ID："+response.getId());
        System.out.println("当前实例状态："+response.status());
    }

    /**
     * 根据ID进行删除文档
     */
    @Test
    public void testDelete(){
        DeleteResponse deleteResponse = client.prepareDelete("book", "java", "1").get();
        System.out.println("索引名称:"+deleteResponse.getIndex());
        System.out.println("索引类型:"+deleteResponse.getType());
        System.out.println("索引Id:"+deleteResponse.getId());
        System.out.println("当前实例状态："+deleteResponse.status());
    }
}
