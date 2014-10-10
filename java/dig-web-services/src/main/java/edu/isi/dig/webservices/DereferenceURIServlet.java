package edu.isi.dig.webservices;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import edu.isi.dig.db.DBManager;

@Path("/istr")
public class DereferenceURIServlet {
	
	public static String PARAM_NOT_FOUND = "Not_Found";
	
	
	@GET
	@Path("/ads/ad/{id}")
	public String GetAd(@PathParam("id") String id ){
		
		DBManager dbM = new DBManager("memex_small", "sqluser", "sqlpassword", "karma-dig-db.cloudapp.net:3306");
		return dbM.getAd(id);
		}
	
	@GET
	@Path("/images/image/{id}")
	public String GetImage(@PathParam("id") String id ){
		
		DBManager dbM = new DBManager("memex_small", "sqluser", "sqlpassword", "karma-dig-db.cloudapp.net:55354");
		return dbM.getImage(id);
		}
	
	@GET
	@Path("/testg")
	public String GetSingleID(@QueryParam("s") String sourceName){
		
			
		Client esClient=null;
		TransportClient ts =null;
		
		try{
			
			if(sourceName.trim() != "" || sourceName!=null)
			{
				Settings settings = ImmutableSettings.settingsBuilder()
				        .put("cluster.name", "dig_isi").build();
	
				ts = new TransportClient(settings);
				esClient = ts.addTransportAddress(new InetSocketTransportAddress("karma-dig-service.cloudapp.net", 55309));
				
				
				
				SearchResponse sr = esClient.prepareSearch("urls")
											.setTypes("url")
											.setSearchType(SearchType.QUERY_AND_FETCH)
											.setQuery(QueryBuilders.termQuery("source", sourceName))
											.execute()
											.actionGet();
			
				//GetResponse getResp = esClient.prepareGet("urls","url","52F9763341A9D08C2BC76B6DF16106A15386C26F")
				//					 	      .execute()
				//					 	      .actionGet();
				ts.close();
				return sr.toString();
				//return sourceName;
			}
			
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
		
		return "";
	}

}
