package edu.isi.dig.webservices;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.elasticsearch.action.search.MultiSearchRequestBuilder;
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

@Path("/isi")
public class DereferenceURIServlet {
	
	public static String PARAM_NOT_FOUND = "Not_Found";
	
	final String INDEX_IMAGES = "images";
	final String INDEX_TYPE_IMAGE = "image";
	final String SEARCH_RESULTS="results";
	final String CLUSTER_NAME = "cluster.name";
	final String CLUSTER_NAME_VALUE = "dig_isi";
	
	Client esClient=null;
	TransportClient ts =null;
	MultiSearchResponse multiResp = null;
	MultiSearchRequestBuilder msrb = null;
			
	Settings settings = null;
	
	@GET
	@Path("/images/{sha}/{epoch}/processed")
	public String GetImagesURLs(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
		
		try{
			settings = ImmutableSettings.settingsBuilder()
					 					.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress("karma-dig-service.cloudapp.net", 55309));
			
			SearchRequestBuilder srbSha = esClient.prepareSearch()
				  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
				  .setIndices(INDEX_IMAGES)
				  .setTypes(INDEX_TYPE_IMAGE)
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
					.setIndices(INDEX_IMAGES)
			  		.setTypes(INDEX_IMAGES)
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

			if(searchHit.length > 1) {
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
	
	@GET
	@Path("/images/{sha}/latest/processed")
	public String GetImageURLsBySha(@PathParam("sha") String sha){
		try{
				
				if(sha.trim() != "")
				{
					settings = ImmutableSettings.settingsBuilder()
		 										.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
					ts = new TransportClient(settings);
					esClient = ts.addTransportAddress(new InetSocketTransportAddress("karma-dig-service.cloudapp.net", 55309));
					
					SearchRequestBuilder srbSha = esClient.prepareSearch()
														  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
														  .setIndices(INDEX_IMAGES)
														  .setTypes(INDEX_TYPE_IMAGE)
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
				
				if(searchHit.length > 1){
				
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
	
	
	@GET
	@Path("/images/{sha}/processed")
	public String GetImagesAllEpochs(@PathParam("sha") String sha){
		
		try{
			settings = ImmutableSettings.settingsBuilder()
					 					.put(CLUSTER_NAME, CLUSTER_NAME_VALUE).build();
			ts = new TransportClient(settings);
			esClient = ts.addTransportAddress(new InetSocketTransportAddress("karma-dig-service.cloudapp.net", 55309));
			
			SearchRequestBuilder srbSha = esClient.prepareSearch()
				  .setQuery(QueryBuilders.matchQuery(SearchFieldsES.SHA1, sha))
				  .setIndices(INDEX_IMAGES)
				  .setTypes(INDEX_TYPE_IMAGE)
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

			if(searchHit.length > 1) {
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
		
}
