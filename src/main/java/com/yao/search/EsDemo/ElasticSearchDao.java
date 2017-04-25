package com.yao.search.EsDemo;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.elasticsearch.common.settings.Settings.builder;

/**
 * @author 三劫散仙 搜索技术交流群：324714439 一个关于elasticsearch批量提交 和search query的的例子
 **/
public class ElasticSearchDao {

	ObjectMapper mapper = new ObjectMapper();

	// es的客户端实例
	TransportClient client = null;
	private void init(){
		client = new PreBuiltTransportClient(settings());
		client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.6.80.104", 9310)));

	}

	private Settings settings() {
        return builder()
        		.put("cluster.name", "es5-test")
        		.put("client.transport.sniff", true)
//        		.put("client.transport.ignore_cluster_name", false)
        		.put("client.transport.ping_timeout", "5s")
        		.put("client.transport.nodes_sampler_interval", "5s")
        		.build();
    }

    protected TransportClient client() {
		if(client == null) {
			init();
		}
    	return client;
	}

	@Test
	public void testCreate() {
		CreateIndexRequest request = new CreateIndexRequest();
		request.index("test111");
		client().admin().indices().create(request).actionGet();
	}

	@Test
	public void testIndex() throws IOException {
		IndexRequestBuilder indexRequestBuilder =
				client().prepareIndex().setIndex("test111").setType("test").setId("1");

		TestIndex testIndex = new TestIndex("1", "test1");
		String json = mapper.writeValueAsString(testIndex);
		indexRequestBuilder.setSource(json);

		indexRequestBuilder.execute().actionGet();
	}

	@Test
	public void testSearch() {
		SearchResponse searchResponse = client().prepareSearch("product").setTypes("product").setFrom(0).setSize(2).execute().actionGet();
		if(searchResponse != null) {
			for (SearchHit searchHit : searchResponse.getHits().getHits()) {
				System.out.println(searchHit.getSourceAsString());
			}

		}
	}

	@Test
	public void testGroovy() {
		try {
			GroovyClassLoader loader = new GroovyClassLoader();
			String scriptText = "a1 * 0.003 + (a2 > 10 ? Math.log10(a2) : a2 * 0.1) * 0.6";
			Class<?> newClazz = loader.parseClass(scriptText);
			Object obj = newClazz.newInstance();
			Script script = (Script) obj;
			long start = System.currentTimeMillis();
			System.out.println(start);
			for (int i=0; i<10000; i++) {

				Binding binding = new Binding();
				binding.setProperty("a1", 100);
				binding.setProperty("a2", 100);
				script.setBinding(binding);
				Object run = script.run();
				if(i%1000 == 0) {

					System.out.println(run);
				}
			}
			System.out.println(System.currentTimeMillis() - start);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}


class TestIndex {
	String id;
	String name;

	public TestIndex() {
	}

	public TestIndex(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}