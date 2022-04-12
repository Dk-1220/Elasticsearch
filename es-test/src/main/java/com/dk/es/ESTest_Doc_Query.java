package com.dk.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.List;
import java.util.Map;

/**
 * 查询文档（全查询、条件查询、分页查询、查询排序、字段查询（过滤））
 */
public class ESTest_Doc_Query {
    public static void main(String[] args) throws Exception {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 1. 查询索引中全部数据 : matchAllQuery
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 2. 条件查询 : termQuery
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age",40)));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 3. 分页查询 : from，size
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).from(0).size(2));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 4. 查询排序 : sort
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort("age",SortOrder.DESC));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 5. 字段查询（过滤） : fetchSource
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        String[] excludes = {"age"};
//        String[] includes = {};
//
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).fetchSource(includes, excludes));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 6. 组合查询 : BoolQueryBuilder
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
////        boolQueryBuilder.must(QueryBuilders.matchQuery("age",30));
////        boolQueryBuilder.must(QueryBuilders.matchQuery("sex","男"));
////        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex","男"));
//        boolQueryBuilder.should(QueryBuilders.matchQuery("age",30));
//        boolQueryBuilder.should(QueryBuilders.matchQuery("age",40));
//
//        request.source(new SearchSourceBuilder().query(boolQueryBuilder));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

//        // 7. 范围查询 : RangeQueryBuilder（gt：大于，gte：大于等于，lt：小于，lte：小于等于）
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
//        rangeQueryBuilder.gte(30);  // 大于等于30
//        rangeQueryBuilder.lte(40);  // 小于等于40
//
//        request.source(new SearchSourceBuilder().query(rangeQueryBuilder));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 8. 模糊查询 : fuzzyQueryBuilder（fuzziness(Fuzziness.TWO)指定字符个数偏差）
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("name", "wangwu").fuzziness(Fuzziness.TWO);
//
//        request.source(new SearchSourceBuilder().query(fuzzyQueryBuilder));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        // 9. 高亮查询 : HighlightBuilder
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name", "zhangsan");
//
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.preTags("<font color='red'>");  // 指定高亮前缀
//        highlightBuilder.postTags("</font>");  // 指定高亮后缀
//        highlightBuilder.field("name");  // 指定高亮字段
//
//        request.source(new SearchSourceBuilder().query(termsQueryBuilder).highlighter(highlightBuilder));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//            System.out.println(hit.getHighlightFields());
//        }

        // 10. 聚合查询 : AggregationBuilder
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//
//        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
//
//        request.source(new SearchSourceBuilder().aggregation(aggregationBuilder));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        System.out.println(response.getTook());
//
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit : hits) {
//            System.out.println(hit.getSourceAsString());
//        }
//
//        String maxAge = new ObjectMapper().writeValueAsString(response.getAggregations());
//        System.out.println(maxAge);

        // 11. 分组查询 : AggregationBuilder
        SearchRequest request = new SearchRequest();
        request.indices("user");

        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("ageGroup").field("age");

        request.source(new SearchSourceBuilder().aggregation(aggregationBuilder));

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        System.out.println(response.getTook());

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        String ageGroup = new ObjectMapper().writeValueAsString(response.getAggregations());
        System.out.println(ageGroup);

        esClient.close();
    }
}
