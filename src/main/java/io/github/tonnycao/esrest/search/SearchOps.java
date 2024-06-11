package io.github.tonnycao.esrest.search;

import cn.hutool.core.collection.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
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
     * term search
     * @param searchSourceBuilder
     * @param field
     * @param value
     * @return
     */
    public SearchSourceBuilder term(SearchSourceBuilder searchSourceBuilder, Object field, String value){
        if(null != field && null != value){
            if(field instanceof String[]){
                QueryBuilders.multiMatchQuery(value, (String[])field);
            }else if(field instanceof String){
                MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder((String) field, value);
                searchSourceBuilder.query(matchQueryBuilder);
            }
        }
        return searchSourceBuilder;
    }

    /***
     * fuzzy  search
     * @param field
     * @param value
     * @param fuzziness
     * @param prefixLength
     * @param maxExpansions
     * @return
     */
    public QueryBuilder fuzzy(String field, String value, Integer fuzziness, Integer prefixLength, Integer maxExpansions){
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, value)
                .fuzziness(fuzziness)
                .prefixLength(prefixLength)
                .maxExpansions(maxExpansions);

        return matchQueryBuilder;
    }


    /***
     * match search
     * @param map
     * @return
     */
    public BoolQueryBuilder match(Map<String, Object> map){
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        if(CollectionUtil.isNotEmpty(map)){
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if(entry.getValue() instanceof Collection){
                    Iterator iter = ((Collection<?>) entry.getValue()).iterator();
                    while(iter.hasNext()){
                        Object theme = (Object)iter.next();
                        TermQueryBuilder vehTypeQuery = QueryBuilders.termQuery(entry.getKey(),  theme);
                        boolQueryBuilder.should(vehTypeQuery);
                    }
                    boolQueryBuilder.minimumShouldMatch(1);
                }else{
                    boolQueryBuilder.must(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
                }
            }
        }
        return boolQueryBuilder;
    }

    /***
     * prefix search
     * @param field
     * @param value
     * @return
     */
    public BoolQueryBuilder prefix(String field, String value){
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        PrefixQueryBuilder vinQuery = QueryBuilders.prefixQuery(field, value);
        boolQueryBuilder.must(vinQuery);

        return boolQueryBuilder;
    }

    /***
     * range search
     * @param field
     * @param start
     * @param end
     * @param format
     * @return
     */
    public QueryBuilder range(String field, Object start, Object end, String format){
        QueryBuilder  qb = null;
        if(null != start){
            qb =   QueryBuilders.rangeQuery(field).format(format).gte(start);
        }else if(null != end){
            qb =   QueryBuilders.rangeQuery(field).format(format).lte(end);
        } else  if(null != start && null != end){
            qb =   QueryBuilders.rangeQuery(field).format(format).gte(start).lte(end);
        }
        return qb;
    }

    /***
     * wildcard search
     * @param field
     * @param value
     * @return
     */
    public QueryBuilder wildcard(String field, String value){
        QueryBuilder qb  =  QueryBuilders.wildcardQuery(field, "*"+ value.trim());
        return qb;
    }


    /***
     * completion suggest
     * @param field
     * @param keyword
     * @param size
     * @return
     */
    public SuggestBuilder completionSuggest(String field, String keyword, Integer size){
        CompletionSuggestionBuilder completionSuggestionBuilder = SuggestBuilders.completionSuggestion(field)
                .prefix(keyword).skipDuplicates(true)
                .size(size);

        String suggestionName = field;
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(suggestionName, completionSuggestionBuilder);
        return suggestBuilder;
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
        if(null == orderBy){
            orderBy = "id";
        }

        searchSourceBuilder.fetchSource(true);
        if(null != order && null != orderBy){
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
        searchMap.put("totalPage", Math.ceil(total/size));
        searchMap.put("from", from);
        searchMap.put("items", items);

        return searchMap;
    }


}
