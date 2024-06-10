package io.github.tonnycao.esrest.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AggregationOps {


    @Autowired
    private RestHighLevelClient client;

    /***
     *
     * @param index
     * @param field
     * @param name
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public ParsedStats stats(String index, String field, String name, SearchSourceBuilder searchSourceBuilder) throws IOException {
        AggregationBuilder aggr = AggregationBuilders.stats(name).field(field);
        searchSourceBuilder.aggregation(aggr);
        searchSourceBuilder.size(0);
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        return  aggregations.get(name);
    }

    /***
     *
     * @param index
     * @param field
     * @param name
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public ParsedPercentiles percentiles(String index, String field, String name, SearchSourceBuilder searchSourceBuilder) throws IOException {
        AggregationBuilder aggr = AggregationBuilders.percentiles(name).field(field);
        searchSourceBuilder.aggregation(aggr);
        searchSourceBuilder.size(0);
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        return aggregations.get(name);
    }

    /***
     *
     * @param index
     * @param field
     * @param name
     * @param size
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public List<? extends Terms.Bucket> bucketTerms(String index, String field, String name, Integer size, SearchSourceBuilder searchSourceBuilder) throws IOException {
        AggregationBuilder aggr = AggregationBuilders.terms(name).field(field);
        searchSourceBuilder.size(size);
        searchSourceBuilder.aggregation(aggr);
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();

        Terms byCompanyAggregation = aggregations.get(name);
        List<? extends Terms.Bucket> buckets = byCompanyAggregation.getBuckets();
        return buckets;
    }

    public List<? extends Histogram.Bucket> bucketHistogram(String index, String field, Integer min, Integer max, Integer interval, String name,  SearchSourceBuilder searchSourceBuilder) throws IOException {
        AggregationBuilder aggr = AggregationBuilders.histogram(name)
                .field(field)
                .extendedBounds(min, max)
                .interval(interval);

        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(aggr);
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Histogram byCompanyAggregation = aggregations.get(name);
        List<? extends Histogram.Bucket> buckets = byCompanyAggregation.getBuckets();
        return buckets;
    }
    /***
     *
     * @param index
     * @param field
     * @param name
     * @param format
     * @param interval
     * @param size
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public List<? extends Histogram.Bucket> bucketDateHistogram(String index, String field, String name, String format, DateHistogramInterval interval, Integer size, SearchSourceBuilder searchSourceBuilder) throws IOException {
        AggregationBuilder aggr = AggregationBuilders.dateHistogram(name)
                .field(field)
                .interval(1)
                .dateHistogramInterval(interval)
                .format(format);
        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(aggr);
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Histogram byCompanyAggregation = aggregations.get(name);
        List<? extends Histogram.Bucket> buckets = byCompanyAggregation.getBuckets();
        return buckets;
    }

    /***
     *
     * @param index
     * @param bucketField
     * @param bucketName
     * @param bucketSize
     * @param metricField
     * @param metricName
     * @param metricSize
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public List<ParsedTopHits> bucketTop(String index, String bucketField, String bucketName, Integer bucketSize,
                                                   String metricField, String metricName, Integer metricSize,
                                                   SearchSourceBuilder searchSourceBuilder) throws IOException {
        AggregationBuilder metricTop = AggregationBuilders.topHits(metricName)
                .size(metricSize)
                .sort(metricField, SortOrder.DESC);

        AggregationBuilder salaryBucket = AggregationBuilders.terms(bucketName)
                .field(bucketField)
                .size(bucketSize);

        salaryBucket.subAggregation(metricTop);

        // 查询源构建器
        searchSourceBuilder.size(0);
        searchSourceBuilder.aggregation(salaryBucket);
        // 创建查询请求对象，将查询条件配置到其中
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        // 执行请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取响应中的聚合信息
        Aggregations aggregations = response.getAggregations();
        // 输出内容
        List<ParsedTopHits> items  = new ArrayList<>();
        // 分桶
            Terms byCompanyAggregation = aggregations.get(bucketName);
            List<? extends Terms.Bucket> buckets = byCompanyAggregation.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                ParsedTopHits topHits = bucket.getAggregations().get(metricName);
                items.add(topHits);
            }
            return items;
    }
}
