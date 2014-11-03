package edu.isi.dig.webservices;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import edu.isi.dig.db.DBHandler;
import edu.isi.dig.es.ElasticSearchHandler;

@Path("/isi")
public class DereferenceURIServlet {
	
	public static String PARAM_NOT_FOUND = "Not_Found";
	
	final String INDEX_IMAGES = "images";
	final String INDEX_TYPE_IMAGE = "image";
	final String INDEX_PAGES = "pages";
	final String INDEX_TYPE_PAGE = "page";
	final String INDEX_WEBPAGES = "webpages";
	final String INDEX_TYPE_WEBPAGE = "webpage";
	
	DBHandler dbh ;
	
	@GET
	@Path("/images/{sha}/{epoch}/raw")
	public String GetElasticSearchImages(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
		
			return ElasticSearchHandler.GetImagesURLs(sha, epoch, INDEX_IMAGES, INDEX_TYPE_IMAGE);
	}
	
	@GET
	@Path("/images/{sha}/latest/raw")
	public String GetElasticSearchImagesBySha(@PathParam("sha") String sha){
			
			return ElasticSearchHandler.GetImageURLsBySha(sha, INDEX_IMAGES, INDEX_TYPE_IMAGE);
		}
	
	
	@GET
	@Path("/images/{sha}/raw")
	public String GetElasticSearchImageAllEpochs(@PathParam("sha") String sha){
		
			return ElasticSearchHandler.GetImagesAllEpochs(sha, INDEX_IMAGES, INDEX_TYPE_IMAGE);
	}
	
	@GET
	@Path("/pages/{sha}/{epoch}/raw")
	public String GetElasticSearchPages(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
			return ElasticSearchHandler.GetPagesURLs(sha, epoch, INDEX_PAGES, INDEX_TYPE_PAGE);
	}
	
	@GET
	@Path("/pages/{sha}/latest/raw")
	public String GetElasticSearchPagesBySha(@PathParam("sha") String sha){
			return ElasticSearchHandler.GetPageURLsBySha(sha, INDEX_PAGES, INDEX_TYPE_PAGE);
		}
	
	
	@GET
	@Path("/pages/{sha}/raw")
	public String GetElasticSearchAllEpochs(@PathParam("sha") String sha){
			return ElasticSearchHandler.GetPagesAllEpochs(sha, INDEX_PAGES, INDEX_TYPE_PAGE);
	}
	
	@GET
	@Path("/pages/{sha}/{epoch}/processed")
	public String GetProcessedAds(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
		
		return ElasticSearchHandler.GetElasticSearchAds(sha,epoch,INDEX_WEBPAGES,INDEX_TYPE_WEBPAGE);
	}
	
	@GET
	@Path("/pages/{sha}/{epoch}/featurecollection")
	public String GetElasticSearchFeatureCollection(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
		return ElasticSearchHandler.GetElasticSearchFeatureCollection(sha,epoch,INDEX_WEBPAGES,INDEX_TYPE_WEBPAGE);
		
	}
	
	@GET
	@Path("/db/{dbName}/{tableName}/{sha}/{epoch}")
	public String QueryDB(@PathParam("dbName") String dbName,@PathParam("tableName") String tableName,
					     @PathParam("sha") String sha,@PathParam("epoch") String epoch){
		
			String fileName = "config.properties";
			Properties prop = new Properties();
			InputStream input = DereferenceURIServlet.class.getClassLoader().getResourceAsStream(fileName);
			try{
				prop.load(input);
				
				dbh = new DBHandler(prop.getProperty("username"), prop.getProperty("password"), prop.getProperty("databaseurl"));
				return dbh.getResultsJson(dbName, tableName, sha, epoch);
			}catch(IOException ioe){
				ioe.printStackTrace();
				return "Exception:" + ioe.getMessage();
			}
			
		
	}
			
}
