package com.yao.search.EsDemo;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by wengqiankun on 2017/4/17.
 */
public class RestClientTest {
    RestClient restClient;
    ObjectMapper mapper = new ObjectMapper();


    @Test
    public void testSearch() {
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        restClient = RestClient.builder(new HttpHost("10.6.80.104", 9220)).build();
        String query = "{\n" +
                "    \"size\": 2," +
                "    \"query\": {\n" +
                "        \"userId\": #userId#\n" +
                "    }\n" +
                "}";
        HttpEntity entity = new StringEntity(query, ContentType.APPLICATION_JSON);
        try {
            Response response = restClient.performRequest("POST", "/product/_search", Collections.<String, String>emptyMap(), entity);
            JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());
            for (JsonNode node : jsonNode.get("hits").get("hits")) {
              
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
