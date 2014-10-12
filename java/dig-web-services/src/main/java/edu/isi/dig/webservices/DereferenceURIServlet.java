package edu.isi.dig.webservices;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

import edu.isi.dig.db.DBManager;

@Path("/isi")
public class DereferenceURIServlet {
	
	public static String PARAM_NOT_FOUND = "Not_Found";
	
	
	@GET
	@Path("/ads/ad/{id}")
	public String GetAd(@PathParam("id") String id ){
		
		DBManager dbM = new DBManager("memex_small", "sqluser", "sqlpassword", "karma-dig-db.cloudapp.net:3306");
		return dbM.getAd(id);
		}
	
	@GET
	@Path("/imagesf/imagef/{id}")
	public String GetImage(@PathParam("id") String id ){
		
		DBManager dbM = new DBManager("memex_small", "sqluser", "sqlpassword", "karma-dig-db.cloudapp.net:55354");
		return dbM.getImage(id);
		}
	
	@GET
	@Path("/images/{sha}/{epoch}/processed")
	public String GetImages(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
		
			
		Client esClient=null;
		TransportClient ts =null;
		MultiSearchResponse multiResp = null;
		SearchResponse searchResp=null;
		final String INDEX_IMAGES = "images";
		final String INDEX_TYPE_IMAGE = "image";
		
		try{
			
			if(sha.trim() != "" && epoch.trim() != "")
			{
				Settings settings = ImmutableSettings.settingsBuilder()
				        .put("cluster.name", "dig_isi").build();
				
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
												  		.setTypes(INDEX_TYPE_IMAGE)
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
				
				
				ts.close();
				
			}
			
			int i = multiResp.getResponses().length;
			return String.valueOf(i);
			
			
			//return sha + "-" + epoch;
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

}
