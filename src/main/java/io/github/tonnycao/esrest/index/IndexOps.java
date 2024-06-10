package io.github.tonnycao.esrest.index;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


@Slf4j
@Service
public class IndexOps {

    @Autowired
    private RestHighLevelClient client;

    /***
     * create by name
     * @param name
     * @return
     * @throws IOException
     */
    public Boolean create(String name) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(name);
        CreateIndexResponse response =  client.indices().create(request,RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /***
     * delete one by name
     * @param name
     * @return
     * @throws IOException
     */
    public Boolean delete(String name) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(name);
        AcknowledgedResponse response = client.indices().delete(request,RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /***
     * 根据名称确定索引是否存在
     * @param name
     * @return
     * @throws IOException
     */
    public Boolean checkExist(String name) throws IOException {
        org.elasticsearch.action.admin.indices.get.GetIndexRequest request = new org.elasticsearch.action.admin.indices.get.GetIndexRequest();
        request.indices(name);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /***
     * query  one index by name
     * @param name
     * @return
     * @throws IOException
     */
    public String queryOne(String name) throws IOException {
        org.elasticsearch.action.admin.indices.get.GetIndexRequest request = new GetIndexRequest();
        request.indices(name);
        GetIndexResponse getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
        return JSONObject.toJSONString(getIndexResponse.getMappings());
    }

    /***
     * set index Mapping
     * @param name
     * @param properties
     * @return
     * @throws IOException
     */
    public Boolean create(String name, Map<String, Map<String, Object>> properties, Map<String, Object> setting) throws IOException {
        CreateIndexRequest request =new CreateIndexRequest(name);
        Map<String, Object> jsonMap = new HashMap<>();
        {
            jsonMap.put("properties", properties);
        }
        Map<String, Object> source = new HashMap<>();
        source.put("enabled", false);
        jsonMap.put("_source", source);
        request.mapping("_doc", jsonMap);
        request.settings(setting);
        AcknowledgedResponse response =  client.indices().create(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

}
