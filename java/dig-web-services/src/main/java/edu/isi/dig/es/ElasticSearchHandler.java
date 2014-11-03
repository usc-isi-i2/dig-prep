package edu.isi.dig.es;

import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;

import edu.isi.dig.webservices.SearchFieldsES;

public class ElasticSearchHandler {
	
	
	final static String SEARCH_RESULTS="results";
	final static String CLUSTER_NAME = "cluster.name";
	final static String CLUSTER_NAME_VALUE = "dig_isi";
	final static String ELASTICSEARCH_HOST = "karma-dig-service.cloudapp.net"; 
	final static int ELASTICSEARCH_PORT = 55309;
	
	static Client esClient=null;
	static TransportClient ts =null;
	static MultiSearchResponse multiResp = null;
	
			
	static Settings settings = null;
	
	
	
	public static String GetImagesURLs(String sha, String epoch,String indexName, String indexType){
		
		try{
			settings = ImmutableSettings.settingsBuilder()
					 					.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
			
			SearchRequestBuilder srbSha = esClient.prepareSearch()
				  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
				  .setIndices(indexName)
				  .setTypes(indexType)
				  .addField(SearchFieldsES.NATIVE_URL)
				  .addField(SearchFieldsES.CACHE_URL)
				  .addField(SearchFieldsES.CONTENT_URL)
				  .addField(SearchFieldsES.MEMEX_URL)
				  .addField(SearchFieldsES.CONTENT_SHA1)
				  .addField(SearchFieldsES.EPOCH)
				  .addField(SearchFieldsES.SHA1)
				  .addField(SearchFieldsES.SOURCE);
			SearchRequestBuilder srbEpoch = esClient.prepareSearch()
					.setQuery(QueryBuilders.matchQuery(SearchFieldsES.EPOCH, epoch))
					.setIndices(indexName)
			  		.setTypes(indexType)
			  		.addField(SearchFieldsES.NATIVE_URL)
			  		.addField(SearchFieldsES.CACHE_URL)
			  		.addField(SearchFieldsES.CONTENT_URL)
			  		.addField(SearchFieldsES.MEMEX_URL)
			  		.addField(SearchFieldsES.CONTENT_SHA1)
			  		.addField(SearchFieldsES.EPOCH)
			  		.addField(SearchFieldsES.SHA1)
			  		.addField(SearchFieldsES.SOURCE);
			
			
			multiResp = esClient.prepareMultiSearch()
								.add(srbSha)
								.add(srbEpoch)
								.execute()
								.actionGet();
			
			MultiSearchResponse.Item item = multiResp.getResponses()[0];	
			
			SearchHit[] searchHit;
			
			Map<String,SearchHitField> map ;
			SearchResponse searchResp = item.getResponse();
			searchHit = searchResp.getHits().getHits();
			JSONObject parentObj= new JSONObject();
			JSONArray jArray = new JSONArray();

			if(searchHit.length > 0) {
				for(SearchHit sr : searchHit){
						map = sr.getFields();
						JSONObject obj = new JSONObject();
						
						obj.accumulate(map.get(SearchFieldsES.NATIVE_URL).getName(), map.get(SearchFieldsES.NATIVE_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.CONTENT_URL).getName(), map.get(SearchFieldsES.CONTENT_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.CACHE_URL).getName(), map.get(SearchFieldsES.CACHE_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.MEMEX_URL).getName(), map.get(SearchFieldsES.MEMEX_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.SHA1).getName(), map.get(SearchFieldsES.SHA1).getValue());
						obj.accumulate(map.get(SearchFieldsES.SOURCE).getName(), map.get(SearchFieldsES.SOURCE).getValue());
						obj.accumulate(map.get(SearchFieldsES.CONTENT_SHA1).getName(), map.get(SearchFieldsES.CONTENT_SHA1).getValue());
						obj.accumulate(map.get(SearchFieldsES.EPOCH).getName(), map.get(SearchFieldsES.EPOCH).getValue());
						
						jArray.add(obj);
				}
			}
			
			parentObj.accumulate(SEARCH_RESULTS, jArray);
			return parentObj.toString();
		}catch(Exception e){
			return e.toString();
		}finally{
			
			if(ts!=null)
				ts.close();
			if(esClient!=null)
				esClient.close();
		}
				
	}

	public static String GetImageURLsBySha(String sha, String indexName, String indexType){
	try{
			
			if(sha.trim() != "")
			{
				settings = ImmutableSettings.settingsBuilder()
	 										.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
				ts = new TransportClient(settings);
				esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
				
				SearchRequestBuilder srbSha = esClient.prepareSearch()
													  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
													  .setIndices(indexName)
													  .setTypes(indexType)
													  .addField(SearchFieldsES.NATIVE_URL)
													  .addField(SearchFieldsES.CACHE_URL)
													  .addField(SearchFieldsES.CONTENT_URL)
													  .addField(SearchFieldsES.MEMEX_URL)
													  .addField(SearchFieldsES.CONTENT_SHA1)
													  .addField(SearchFieldsES.EPOCH)
													  .addField(SearchFieldsES.SHA1)
													  .addField(SearchFieldsES.SOURCE)
													  .addSort(SearchFieldsES.EPOCH, SortOrder.DESC);
								
				
				multiResp = esClient.prepareMultiSearch()
									.add(srbSha)
									.execute()
									.actionGet();
				
				
				ts.close();
				
			}
			
			 MultiSearchResponse.Item item = multiResp.getResponses()[0]; //Can't figure out why would it have more Items than one 
			
			SearchResponse searchResp = item.getResponse();
			SearchHit[] searchHit = searchResp.getHits().getHits();
			
			JSONObject parentObj= new JSONObject();
			JSONArray jArray = new JSONArray();
			JSONObject obj = new JSONObject();
			
			if(searchHit.length > 0){
			
				SearchHit searchHitLatest = searchHit[0]; //we need the latest, get the first one, sorted by epoch in descending order
				Map<String,SearchHitField> map = searchHitLatest.getFields();
				obj.accumulate(map.get(SearchFieldsES.NATIVE_URL).getName(), map.get(SearchFieldsES.NATIVE_URL).getValue());
				obj.accumulate(map.get(SearchFieldsES.CONTENT_URL).getName(), map.get(SearchFieldsES.CONTENT_URL).getValue());
				obj.accumulate(map.get(SearchFieldsES.CACHE_URL).getName(), map.get(SearchFieldsES.CACHE_URL).getValue());
				obj.accumulate(map.get(SearchFieldsES.MEMEX_URL).getName(), map.get(SearchFieldsES.MEMEX_URL).getValue());
				obj.accumulate(map.get(SearchFieldsES.SHA1).getName(), map.get(SearchFieldsES.SHA1).getValue());
				obj.accumulate(map.get(SearchFieldsES.SOURCE).getName(), map.get(SearchFieldsES.SOURCE).getValue());
				obj.accumulate(map.get(SearchFieldsES.CONTENT_SHA1).getName(), map.get(SearchFieldsES.CONTENT_SHA1).getValue());
				obj.accumulate(map.get(SearchFieldsES.EPOCH).getName(), map.get(SearchFieldsES.EPOCH).getValue());
			}
			
			jArray.add(obj);
			
			parentObj.accumulate(SEARCH_RESULTS, jArray);
			
			return parentObj.toString();
	
		}
		catch(Exception e){
			return e.toString();
		}
		finally{
			
			if(ts!=null)
				ts.close();
			
			if(esClient!=null)
				esClient.close();
			
		}
		
	}
	
	public static String GetImagesAllEpochs(String sha, String indexName, String indexType){
		
		try{
			settings = ImmutableSettings.settingsBuilder()
					 					.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
			
			SearchRequestBuilder srbSha = esClient.prepareSearch()
				  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
				  .setIndices(indexName)
				  .setTypes(indexType)
				  .addField(SearchFieldsES.NATIVE_URL)
				  .addField(SearchFieldsES.CACHE_URL)
				  .addField(SearchFieldsES.CONTENT_URL)
				  .addField(SearchFieldsES.MEMEX_URL)
				  .addField(SearchFieldsES.CONTENT_SHA1)
				  .addField(SearchFieldsES.EPOCH)
				  .addField(SearchFieldsES.SHA1)
				  .addField(SearchFieldsES.SOURCE)
				  .addSort(SearchFieldsES.EPOCH, SortOrder.DESC);
			
			multiResp = esClient.prepareMultiSearch()
								.add(srbSha)
								.execute()
								.actionGet();
			
			MultiSearchResponse.Item item = multiResp.getResponses()[0];	//should be the first one, no point of getting more than responses.
																			// will check the ElasticCode later to understand it better
			
			SearchHit[] searchHit;
			
			Map<String,SearchHitField> map ;
			SearchResponse searchResp = item.getResponse();
			searchHit = searchResp.getHits().getHits();
			JSONObject parentObj= new JSONObject();
			JSONArray jArray = new JSONArray();

			if(searchHit.length > 0) {
				for(SearchHit sr : searchHit){
						map = sr.getFields();
						JSONObject obj = new JSONObject();
						
						obj.accumulate(map.get(SearchFieldsES.NATIVE_URL).getName(), map.get(SearchFieldsES.NATIVE_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.CONTENT_URL).getName(), map.get(SearchFieldsES.CONTENT_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.CACHE_URL).getName(), map.get(SearchFieldsES.CACHE_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.MEMEX_URL).getName(), map.get(SearchFieldsES.MEMEX_URL).getValue());
						obj.accumulate(map.get(SearchFieldsES.SHA1).getName(), map.get(SearchFieldsES.SHA1).getValue());
						obj.accumulate(map.get(SearchFieldsES.SOURCE).getName(), map.get(SearchFieldsES.SOURCE).getValue());
						obj.accumulate(map.get(SearchFieldsES.CONTENT_SHA1).getName(), map.get(SearchFieldsES.CONTENT_SHA1).getValue());
						obj.accumulate(map.get(SearchFieldsES.EPOCH).getName(), map.get(SearchFieldsES.EPOCH).getValue());
						
						jArray.add(obj);
				}
			}
			
			parentObj.accumulate(SEARCH_RESULTS, jArray);
			return parentObj.toString();
		}catch(Exception e){
			return e.toString();
		}finally{
			
			if(ts!=null)
				ts.close();
			if(esClient!=null)
				esClient.close();
		}
				
	}

	public static String GetPagesURLs(String sha, String epoch,String indexName, String indexType){
	
	try{
		settings = ImmutableSettings.settingsBuilder()
				 					.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
		ts = new TransportClient(settings);
		esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
		
		SearchRequestBuilder srbSha = esClient.prepareSearch()
			  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
			  .setIndices(indexName)
			  .setTypes(indexType)
			  .addField(SearchFieldsES.NATIVE_URL)
			  .addField(SearchFieldsES.CACHE_URL)
			  .addField(SearchFieldsES.EPOCH)
			  .addField(SearchFieldsES.SHA1)
			  .addField(SearchFieldsES.SOURCE)
			  .addField(SearchFieldsES.DOCUMENT_TYPE)
			  .addField(SearchFieldsES.PROCESS_STAGE);
			
		SearchRequestBuilder srbEpoch = esClient.prepareSearch()
				.setQuery(QueryBuilders.matchQuery(SearchFieldsES.EPOCH, epoch))
				.setIndices(indexName)
		  		.setTypes(indexType)
		  		.addField(SearchFieldsES.NATIVE_URL)
		  		.addField(SearchFieldsES.CACHE_URL)
		  		.addField(SearchFieldsES.EPOCH)
		  		.addField(SearchFieldsES.SHA1)
		  		.addField(SearchFieldsES.SOURCE)
		  		.addField(SearchFieldsES.DOCUMENT_TYPE)
		  		.addField(SearchFieldsES.PROCESS_STAGE);
		
		
		multiResp = esClient.prepareMultiSearch()
							.add(srbSha)
							.add(srbEpoch)
							.execute()
							.actionGet();
		
		MultiSearchResponse.Item item = multiResp.getResponses()[0];	
		
		SearchHit[] searchHit;
		
		Map<String,SearchHitField> map ;
		SearchResponse searchResp = item.getResponse();
		searchHit = searchResp.getHits().getHits();
		JSONObject parentObj= new JSONObject();
		JSONArray jArray = new JSONArray();

		if(searchHit.length > 0) {
			for(SearchHit sr : searchHit){
					map = sr.getFields();
					JSONObject obj = new JSONObject();
					
					obj.accumulate(map.get(SearchFieldsES.NATIVE_URL).getName(), map.get(SearchFieldsES.NATIVE_URL).getValue());
					obj.accumulate(map.get(SearchFieldsES.CACHE_URL).getName(), map.get(SearchFieldsES.CACHE_URL).getValue());
					obj.accumulate(map.get(SearchFieldsES.SHA1).getName(), map.get(SearchFieldsES.SHA1).getValue());
					obj.accumulate(map.get(SearchFieldsES.SOURCE).getName(), map.get(SearchFieldsES.SOURCE).getValue());
					obj.accumulate(map.get(SearchFieldsES.EPOCH).getName(), map.get(SearchFieldsES.EPOCH).getValue());
					obj.accumulate(map.get(SearchFieldsES.DOCUMENT_TYPE).getName(), map.get(SearchFieldsES.DOCUMENT_TYPE).getValue());
					obj.accumulate(map.get(SearchFieldsES.PROCESS_STAGE).getName(), map.get(SearchFieldsES.PROCESS_STAGE).getValue());
					
					jArray.add(obj);
			}
		}
		
		parentObj.accumulate(SEARCH_RESULTS, jArray);
		return parentObj.toString();
	}catch(Exception e){
		return e.getLocalizedMessage();
	}finally{
		
		if(ts!=null)
			ts.close();
		if(esClient!=null)
			esClient.close();
	}
			
}

	public static String GetPageURLsBySha(String sha, String indexName, String indexType){
try{
		
		if(sha.trim() != "")
		{
			settings = ImmutableSettings.settingsBuilder()
 										.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
			
			SearchRequestBuilder srbSha = esClient.prepareSearch()
												  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
												  .setIndices(indexName)
												  .setTypes(indexType)
												  .addField(SearchFieldsES.NATIVE_URL)
												  .addField(SearchFieldsES.CACHE_URL)
												  .addField(SearchFieldsES.EPOCH)
												  .addField(SearchFieldsES.SHA1)
												  .addField(SearchFieldsES.SOURCE)
												  .addField(SearchFieldsES.DOCUMENT_TYPE)
												  .addField(SearchFieldsES.PROCESS_STAGE)
												  .addSort(SearchFieldsES.EPOCH, SortOrder.DESC);
							
			
			multiResp = esClient.prepareMultiSearch()
								.add(srbSha)
								.execute()
								.actionGet();
			
			
			ts.close();
			
		}
		
		 MultiSearchResponse.Item item = multiResp.getResponses()[0]; //Can't figure out why would it have more Items than one 
		
		SearchResponse searchResp = item.getResponse();
		SearchHit[] searchHit = searchResp.getHits().getHits();
		
		JSONObject parentObj= new JSONObject();
		JSONArray jArray = new JSONArray();
		JSONObject obj = new JSONObject();
		
		if(searchHit.length > 0){
		
			SearchHit searchHitLatest = searchHit[0]; //we need the latest, get the first one, sorted by epoch in descending order
			Map<String,SearchHitField> map = searchHitLatest.getFields();
			obj.accumulate(map.get(SearchFieldsES.NATIVE_URL).getName(), map.get(SearchFieldsES.NATIVE_URL).getValue());
			obj.accumulate(map.get(SearchFieldsES.CACHE_URL).getName(), map.get(SearchFieldsES.CACHE_URL).getValue());
			obj.accumulate(map.get(SearchFieldsES.SHA1).getName(), map.get(SearchFieldsES.SHA1).getValue());
			obj.accumulate(map.get(SearchFieldsES.SOURCE).getName(), map.get(SearchFieldsES.SOURCE).getValue());
			obj.accumulate(map.get(SearchFieldsES.EPOCH).getName(), map.get(SearchFieldsES.EPOCH).getValue());
			obj.accumulate(map.get(SearchFieldsES.DOCUMENT_TYPE).getName(), map.get(SearchFieldsES.DOCUMENT_TYPE).getValue());
			obj.accumulate(map.get(SearchFieldsES.PROCESS_STAGE).getName(), map.get(SearchFieldsES.PROCESS_STAGE).getValue());
			
		}
		
		jArray.add(obj);
		
		parentObj.accumulate(SEARCH_RESULTS, jArray);
		
		return parentObj.toString();

	}
	catch(Exception e){
		return e.toString();
	}
	finally{
		
		if(ts!=null)
			ts.close();
		
		if(esClient!=null)
			esClient.close();
		
	}
	
}

	public static String GetPagesAllEpochs(String sha, String indexName, String indexType){
	
	try{
		settings = ImmutableSettings.settingsBuilder()
				 					.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
		ts = new TransportClient(settings);
		esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
		
		SearchRequestBuilder srbSha = esClient.prepareSearch()
			  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
			  .setIndices(indexName)
			  .setTypes(indexType)
			  .addField(SearchFieldsES.NATIVE_URL)
			  .addField(SearchFieldsES.CACHE_URL)
			  .addField(SearchFieldsES.EPOCH)
			  .addField(SearchFieldsES.SHA1)
			  .addField(SearchFieldsES.SOURCE)
			  .addField(SearchFieldsES.DOCUMENT_TYPE)
			  .addField(SearchFieldsES.PROCESS_STAGE)
			  .addSort(SearchFieldsES.EPOCH, SortOrder.DESC);
		
		multiResp = esClient.prepareMultiSearch()
							.add(srbSha)
							.execute()
							.actionGet();
		
		MultiSearchResponse.Item item = multiResp.getResponses()[0];	//should be the first one, no point of getting more than responses.
																		// will check the ElasticCode later to understand it better
		
		SearchHit[] searchHit;
		
		Map<String,SearchHitField> map ;
		SearchResponse searchResp = item.getResponse();
		searchHit = searchResp.getHits().getHits();
		JSONObject parentObj= new JSONObject();
		JSONArray jArray = new JSONArray();

		if(searchHit.length > 0) {
			for(SearchHit sr : searchHit){
					map = sr.getFields();
					JSONObject obj = new JSONObject();
					
					obj.accumulate(map.get(SearchFieldsES.NATIVE_URL).getName(), map.get(SearchFieldsES.NATIVE_URL).getValue());
					obj.accumulate(map.get(SearchFieldsES.CACHE_URL).getName(), map.get(SearchFieldsES.CACHE_URL).getValue());
					obj.accumulate(map.get(SearchFieldsES.SHA1).getName(), map.get(SearchFieldsES.SHA1).getValue());
					obj.accumulate(map.get(SearchFieldsES.SOURCE).getName(), map.get(SearchFieldsES.SOURCE).getValue());
					obj.accumulate(map.get(SearchFieldsES.EPOCH).getName(), map.get(SearchFieldsES.EPOCH).getValue());
					obj.accumulate(map.get(SearchFieldsES.DOCUMENT_TYPE).getName(), map.get(SearchFieldsES.DOCUMENT_TYPE).getValue());
					obj.accumulate(map.get(SearchFieldsES.PROCESS_STAGE).getName(), map.get(SearchFieldsES.PROCESS_STAGE).getValue());
					
					
					jArray.add(obj);
			}
		}
		
		parentObj.accumulate(SEARCH_RESULTS, jArray);
		return parentObj.toString();
	}catch(Exception e){
		e.printStackTrace();
		return e.toString();
	}finally{
		
		if(ts!=null)
			ts.close();
		if(esClient!=null)
			esClient.close();
	}
			
}

	public static String GetElasticSearchAds(String sha, String epoch, String indexName, String indexType){
		
		try {
			settings = ImmutableSettings.settingsBuilder()
						.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
			
			StringBuffer sbURI = new StringBuffer();
			sbURI.append("http://memex.zapto.org/data/page/");
			sbURI.append(sha.trim());
			sbURI.append("/");
			sbURI.append(epoch);
			sbURI.append("/");
			sbURI.append("processed");
			
			SearchRequestBuilder srbURI = esClient.prepareSearch()
												  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.URI, sbURI.toString().trim()))
												  .setIndices(indexName)
												  .setTypes(indexType)
												  .setSize(1);//best match should be returned
												  
			
			multiResp = esClient.prepareMultiSearch()
					.add(srbURI)
					.execute()
					.actionGet();
			
			
			MultiSearchResponse.Item item = multiResp.getResponses()[0];	//should be the first one, no point of getting more than responses.
																			// will check the ElasticCode later to understand it better

			SearchHit[] searchHit;
			
			SearchResponse searchResp = item.getResponse();
			searchHit = searchResp.getHits().getHits();
			
			if(searchHit.length > 0 && searchHit.length == 1) {
				return searchHit[0].getSourceAsString();
			}
			else{
				
				throw new Exception("Web page not found or multiple web pages found");
			}
	
		}catch(Exception e){
			e.printStackTrace();
			return e.toString();
		}finally{
			
			if(ts!=null)
				ts.close();
			if(esClient!=null)
				esClient.close();
		}
	}
	
public static String GetElasticSearchFeatureCollection(String sha, String epoch, String indexName, String indexType){
		
		try {
			settings = ImmutableSettings.settingsBuilder()
						.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress(ELASTICSEARCH_HOST, ELASTICSEARCH_PORT));
			
			StringBuffer sbURI = new StringBuffer();
			sbURI.append("http://memex.zapto.org/data/page/");
			sbURI.append(sha.trim());
			sbURI.append("/");
			sbURI.append(epoch);
			sbURI.append("/");
			sbURI.append("processed");
			
			SearchRequestBuilder srbURI = esClient.prepareSearch()
												  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.URI, sbURI.toString().trim()))
												  .setIndices(indexName)
												  .setTypes(indexType)
												  .setSize(1);//best match should be returned
												  
			
			multiResp = esClient.prepareMultiSearch()
					.add(srbURI)
					.execute()
					.actionGet();
			
			
			MultiSearchResponse.Item item = multiResp.getResponses()[0];	//should be the first one, no point of getting more than responses.
																			// will check the ElasticCode later to understand it better

			SearchHit[] searchHit;
			
			SearchResponse searchResp = item.getResponse();
			searchHit = searchResp.getHits().getHits();
						
			if(searchHit.length > 0 && searchHit.length == 1) {
				JSONObject jObj = (JSONObject) JSONSerializer.toJSON(searchHit[0].getSourceAsString());
				return jObj.getString("hasFeatureCollection");
			}
			else{
				
				throw new Exception("Web page not found or multiple web pages found");
			}
	
		}catch(Exception e){
			e.printStackTrace();
			return e.toString();
		}finally{
			
			if(ts!=null)
				ts.close();
			if(esClient!=null)
				esClient.close();
		}
	}
	
}
