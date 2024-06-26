package io.github.tonnycao.esrest.index;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class IndexTemplateOps {

    @Autowired
    private RestHighLevelClient client;

    /***
     * standard create
     * @param name
     * @param patterns
     * @param properties
     * @param setting
     * @throws IOException
     */
    public Boolean create(String name, List<String> patterns,
                                    Map<String, Map<String, Object>> properties, Map<String, Object> setting, String aliasName) throws IOException {

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(name);
        request.patterns(patterns);

        Map<String, Object> jsonMap = new HashMap<>();
        {
            jsonMap.put("properties", properties);
        }

        Map<String, Object> source = new HashMap<>();
        source.put("enabled", false);
        jsonMap.put("_source", source);

        request.mapping("_doc", jsonMap);
        request.settings(setting);
        if(StrUtil.isNotBlank(aliasName)){
            request.alias(new Alias(aliasName));
        }
        AcknowledgedResponse response = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /***
     * create by json
     * @param name
     * @param jsonStr
     * @return
     * @throws IOException
     */
    public Boolean createByJson(String name, String jsonStr) throws IOException {

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(name);
        request.source(jsonStr, XContentType.JSON);
        request.create(true);
        request.order(20);
        AcknowledgedResponse response = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * existing check
     * @param name
     * @return
     * @throws IOException
     */
    public Boolean checkExist(String name) throws IOException {
        IndexTemplatesExistRequest request;
        request = new IndexTemplatesExistRequest(name);
        return client.indices().existsTemplate(request, RequestOptions.DEFAULT);
    }

    /***
     * query one
     * @return
     * @throws IOException
     */
    public Map<String, String> queryOne(String name) throws IOException {
        GetIndexTemplatesRequest request = new GetIndexTemplatesRequest(name);

        GetIndexTemplatesResponse response = client.indices().getTemplate(request, RequestOptions.DEFAULT);
        Map<String, String> map = new HashMap<>();
        map.put("setting", response.getIndexTemplates().get(0).getMappings().toString());
        map.put("mapping", response.getIndexTemplates().get(0).getMappings().toString());
        return map;
    }



    /***
     * delete one by name
     * @param name
     * @throws IOException
     */
    public Boolean deleteOne(String name) throws IOException {
        DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest();
        request.name(name);
        AcknowledgedResponse deleteTemplateAcknowledge = client.indices().deleteTemplate(request, RequestOptions.DEFAULT);
        return deleteTemplateAcknowledge.isAcknowledged();
    }


}
