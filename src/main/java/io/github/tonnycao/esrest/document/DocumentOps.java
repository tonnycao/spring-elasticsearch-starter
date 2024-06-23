package io.github.tonnycao.esrest.document;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Slf4j
@Service
public class DocumentOps {


    @Autowired
    private RestHighLevelClient client;

    /***
     * add one doc
     * @param name
     * @param jsonMap
     * @return
     * @throws IOException
     */
    public Boolean addDoc(String name, Map<String, Object> jsonMap) throws IOException {
        IndexRequest request = new IndexRequest(name);
        request.type("_doc");
        if(null != jsonMap.get("id")){
            request.id(jsonMap.get("id").toString());
        }
        String json = JSONObject.toJSONString(jsonMap, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        request.source(json, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        log.info("{}", JSONObject.toJSONString(response));
        return response.getResult() == DocWriteResponse.Result.CREATED;
    }

    /***
     * query one doc
     * @param indexName
     * @param id
     * @return
     * @throws IOException
     */
    public Map<String, Object> getDoc(String indexName, String id) throws IOException {
        GetRequest request = new GetRequest (
                indexName,
                "_doc",
                id);
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
        return getResponse.getSource();
    }

    /***
     * batch add docs
     * @param index
     * @param items
     * @return
     * @throws IOException
     */
    public Integer bulkAddDoc(String index, List<Map<String, Object>> items) throws IOException {
        BulkRequest bulk = new BulkRequest();

        for (Map<String, Object> item: items) {
            IndexRequest request = new IndexRequest(index);
            String json = JSONObject.toJSONString(item, SerializerFeature.WriteMapNullValue,
                    SerializerFeature.DisableCircularReferenceDetect,
                    SerializerFeature.WriteDateUseDateFormat);
            request.type("_doc");
            if(null != item.get("id")){
                request.id(item.get("id").toString());
            }
            request.source(json, XContentType.JSON);
            bulk.add(request);
        }

        BulkResponse responses =  client.bulk(bulk,RequestOptions.DEFAULT);
        return responses.getItems().length;
    }

    /***
     * check doc by id
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    public Boolean exits(String index,  String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    /***
     * delete by id
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    public Boolean deleteById(String index,  String id) throws IOException {
        DeleteRequest request = new DeleteRequest(
                index, id);
        DeleteResponse deleteResponse = client.delete(
                request, RequestOptions.DEFAULT);
        return deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED) ||  deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND);
    }

    /***
     * update one doc by id
     * @param indexName
     * @param id
     * @param data
     * @return
     * @throws IOException
     */
    public Boolean updateById(String indexName, String id, Map<String, Object> data) throws IOException {

        UpdateRequest request = new UpdateRequest(indexName, "_doc", id);
        String json = JSONObject.toJSONString(data, SerializerFeature.WriteMapNullValue,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteDateUseDateFormat);
        request.doc(json, XContentType.JSON);

        UpdateResponse updateResponse = client.update(
                request, RequestOptions.DEFAULT);

        return updateResponse.getResult() == DocWriteResponse.Result.UPDATED;
    }
}
