package com.dk.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * 批量新增文档
 */
public class ESTest_Doc_Create_Batch {
    public static void main(String[] args) throws Exception {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 批量插入数据
        BulkRequest request = new BulkRequest();

        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON, "name", "zhangsan","age",30,"sex","男"));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON, "name", "lisi","age",30,"sex","女"));
        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON, "name", "wangwu","age",40,"sex","女"));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON, "name", "wangwu01","age",40,"sex","男"));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON, "name", "wangwu02","age",50,"sex","女"));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON, "name", "wangwu03","age",50,"sex","男"));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);

        System.out.println(response.getTook());
        System.out.println(response.getItems());

        esClient.close();
    }
}
