package io.github.tonnycao.esrest.search;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class SearchOps {

    @Autowired
    private RestHighLevelClient client;

    /***
     * fuzzy  search query builder
     * @param field
     * @param value
     * @param fuzziness
     * @param prefixLength
     * @param maxExpansions
     * @return
     */
    public QueryBuilder fuzzy(String field, String value, Integer fuzziness, Integer prefixLength, Integer maxExpansions) {
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, value)
                .fuzziness(fuzziness)
                .prefixLength(prefixLength)
                .maxExpansions(maxExpansions);

        return matchQueryBuilder;
    }


    /***
     * match search query builder
     * @param map
     * @return
     */
    public BoolQueryBuilder match(Map<String, Object> map) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        if (CollectionUtil.isNotEmpty(map)) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                // if value is collection, then use should
                if (entry.getValue() instanceof Collection) {
                    Iterator iter = ((Collection<?>) entry.getValue()).iterator();
                    while (iter.hasNext()) {
                        Object theme = (Object) iter.next();
                        TermQueryBuilder vehTypeQuery = QueryBuilders.termQuery(entry.getKey(), theme);
                        boolQueryBuilder.should(vehTypeQuery);
                    }
                    boolQueryBuilder.minimumShouldMatch(1);
                } else {
                    // if value is not collection, then use must
                    boolQueryBuilder.must(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
                }
            }
        }
        return boolQueryBuilder;
    }

    /***
     * prefix search query builder
     * @param field
     * @param value
     * @return
     */
    public BoolQueryBuilder prefix(String field, String value) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        PrefixQueryBuilder vinQuery = QueryBuilders.prefixQuery(field, value);
        boolQueryBuilder.must(vinQuery);

        return boolQueryBuilder;
    }

    /***
     * range search query builder
     * @param field
     * @param start
     * @param end
     * @param format
     * @return
     */
    public QueryBuilder range(String field, Object start, Object end, String format) {
        QueryBuilder qb = null;
        if (null != start) {
            qb = QueryBuilders.rangeQuery(field).format(format).gte(start);
        } else if (null != end) {
            qb = QueryBuilders.rangeQuery(field).format(format).lte(end);
        } else if (null != start && null != end) {
            qb = QueryBuilders.rangeQuery(field).format(format).gte(start).lte(end);
        }
        return qb;
    }

    /***
     * wildcard search
     * @param field
     * @param value
     * @return
     */
    public QueryBuilder wildcard(String field, String value) {
        QueryBuilder qb = QueryBuilders.wildcardQuery(field, "*" + value.trim());
        return qb;
    }


    /***
     * completion suggest builder
     * @param field
     * @param keyword
     * @param size
     * @return
     */
    public SuggestBuilder completionSuggest(String field, String keyword, Integer size) {
        CompletionSuggestionBuilder completionSuggestionBuilder = SuggestBuilders.completionSuggestion(field)
                .prefix(keyword).skipDuplicates(true)
                .size(size);

        String suggestionName = field;
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(suggestionName, completionSuggestionBuilder);
        return suggestBuilder;
    }

    /**
     * term search query builder
     * @param field
     * @param value
     * @return
     */
    public QueryBuilder term(Object field, String value){
        QueryBuilder matchQueryBuilder = null;
        if (field instanceof String[]) {
            matchQueryBuilder = QueryBuilders.multiMatchQuery(value, (String[]) field);
        } else if (field instanceof String) {
            matchQueryBuilder = new MatchQueryBuilder((String) field, value);
        }
        return matchQueryBuilder;
    }

    /***
     * compound search query builder
     */
    public QueryBuilder buildMultiQuery(QueryBuilder ...queryBuilders){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (QueryBuilder queryBuilder : queryBuilders) {
            boolQueryBuilder.must(queryBuilder);
        }
        return boolQueryBuilder;
    }

    /***
     * term search
     * @param queryBuilder
     * @return
     */
    public SearchSourceBuilder doSearchSourceBuilder(QueryBuilder queryBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        return searchSourceBuilder;
    }


    /***
     * do search request
     * @param index
     * @param searchSourceBuilder
     * @param size
     * @param from
     * @param orderBy
     * @param order
     * @param unmappedType
     * @return
     * @throws IOException
     */
    public Map<String, Object> fetch(String index, SearchSourceBuilder searchSourceBuilder, Integer size, Integer from,
                                     String orderBy, String order, String unmappedType) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types("_doc");
        searchRequest.indices(index);

        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        if (null == orderBy) {
            orderBy = "id";
        }

        searchSourceBuilder.fetchSource(true);
        if (null != order && null != orderBy) {
            searchSourceBuilder.sort(new FieldSortBuilder(orderBy).order(SortOrder.fromString(order)).unmappedType(unmappedType));
        }
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices(index);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        Map<String, Object> searchMap = new HashMap<>();
        Long total = response.getHits().getTotalHits().value;
        List<Map<String, Object>> items = new ArrayList<>(response.getHits().getHits().length);
        Arrays.asList(response.getHits().getHits()).forEach(item -> {
            items.add(item.getSourceAsMap());
        });
        searchMap.put("total", total);
        searchMap.put("totalPage", Math.ceil(total / size));
        searchMap.put("from", from);
        searchMap.put("items", items);

        return searchMap;
    }

    /***
     * multi search
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public Map<String, Object> multiSearch(SearchSourceBuilder searchSourceBuilder) throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        SearchRequest firstSearchRequest = new SearchRequest();
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);
        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
        Map<String, Object> searchMap = new HashMap<>();
        return searchMap;
    }

    /***
     * count search document
     * @param indexName
     * @param searchSourceBuilder
     * @return
     * @throws IOException
     */
    public Long count(String indexName, SearchSourceBuilder searchSourceBuilder) throws IOException {
        CountRequest countRequest = new CountRequest(indexName);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client
                .count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }


    /***
     * search by After
     * @param indexName
     * @param sourceBuilder
     * @param searchAfterList
     * @return
     */
    public Map<String, Object> searchAfter(String indexName, SearchSourceBuilder sourceBuilder,  List<Object> searchAfterList){

        Map<String, Object> result = new HashMap<>();
        if(null !=searchAfterList && searchAfterList.size()>0){
            Object[] arr = searchAfterList.stream().toArray(String[] ::new);
            sourceBuilder.searchAfter(arr);
        }
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indices(indexName);
        searchRequest.types("_doc");
        searchRequest.source(sourceBuilder);

        log.info("index {}: {}",indexName,sourceBuilder.toString());

        List<Map<String, Object>> items = new ArrayList<>();
        Object[] nextSearchAfter =  new Object[searchAfterList.size()];

        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            if(searchResponseIsNotNull(response)){
                Long total = response.getHits().getTotalHits().value;
                result.put("total", total);
                result.put("totalPage",  (int)Math.ceil(total/sourceBuilder.size()));
                SearchHit[] hits = response.getHits().getHits();
                List<SearchHit> hitList = Arrays.asList(hits);
                for(SearchHit item: hitList){
                    nextSearchAfter =  item.getSortValues();
                    items.add(item.getSourceAsMap());
                }
            }else{
                result.put("total", 0);
                result.put("totalPage",  0);
            }
        } catch (IOException e) {
            log.error("ES searchAfter error:{}", e.getMessage());
        }
        result.put("searchAfter", nextSearchAfter);
        result.put("items", items);
        return result;
    }

    /***
     * search by Scroll
     * @param indexName
     * @param sourceBuilder
     * @param minutes
     * @param scrollId
     * @return
     */
    public Map<String, Object> scrollSearch(String indexName, SearchSourceBuilder sourceBuilder, Integer minutes, String scrollId){
        Map<String, Object> result = new HashMap<>();

        SearchResponse searchResponse = null;
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);

        // set scroll expired time 5 minutes
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(minutes));
        searchRequest.scroll(scroll);
        // query data by scrollId
        if(StrUtil.isNotBlank(scrollId)){
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            try {
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("es scrollSearch exception: {}", e.getMessage());
            }

        }else{
            try {
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("es scrollSearch exception: {}", e.getMessage());
            }
        }

        List<Map<String, Object>> items = this.buildResponse(searchResponse);

        Long total = searchResponse.getHits().getTotalHits().value;
        result.put("total", total);

        if(total>0){
            result.put("totalPage",   (int)Math.ceil(total/sourceBuilder.size()));
        }else{
            result.put("totalPage",  0);
        }
        result.put("items", items);
        result.put("scrollId", searchResponse.getScrollId());
        result.put("timestamp", System.currentTimeMillis());

        return result;
    }


    /***
     * clear Scroll
     * @param scrollId
     */
    public void clearScroll(String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        try {
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("ES clear Scroll exception: {}", e.getMessage());
        }
    }

    /***
     * check searchResponse is not null
     * @param searchResponse
     * @return
     */
    private boolean searchResponseIsNotNull(SearchResponse searchResponse) {
        return !Objects.isNull(searchResponse)
                && !Objects.isNull(searchResponse.getHits())
                && !Objects.isNull(searchResponse.getHits().getHits())
                && searchResponse.getHits().getHits().length > 0
                && searchResponse.getHits().getTotalHits().value > 0;
    }

    /***
     * build response data
     * @param searchResponse
     * @return
     */
    private List<Map<String, Object>> buildResponse(SearchResponse searchResponse) {
        List<Map<String, Object>> items = new ArrayList<>();

        if (searchResponseIsNotNull(searchResponse)) {
            SearchHit[] hits = searchResponse.getHits().getHits();
            Arrays.asList(hits).forEach(item -> {
                items.add(item.getSourceAsMap());
            });
        }

        return items;
    }
}