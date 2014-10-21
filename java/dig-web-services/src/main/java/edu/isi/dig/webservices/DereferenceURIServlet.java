package edu.isi.dig.webservices;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import edu.isi.dig.es.ElasticSearchHandler;

@Path("/isi")
public class DereferenceURIServlet {
	
	public static String PARAM_NOT_FOUND = "Not_Found";
	
	final String INDEX_IMAGES = "images";
	final String INDEX_TYPE_IMAGE = "image";
	final String INDEX_PAGES = "pages";
	final String INDEX_TYPE_PAGE = "page";
	
	ElasticSearchHandler esh;
	
	@GET
	@Path("/images/{sha}/{epoch}/raw")
	public String GetElasticSearchImages(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
		
			esh = new ElasticSearchHandler();
			return esh.GetImagesURLs(sha, epoch, INDEX_IMAGES, INDEX_TYPE_IMAGE);
	}
	
	@GET
	@Path("/images/{sha}/latest/raw")
	public String GetElasticSearchImagesBySha(@PathParam("sha") String sha){
			
			esh = new ElasticSearchHandler();
			return esh.GetImageURLsBySha(sha, INDEX_IMAGES, INDEX_TYPE_IMAGE);
		}
	
	
	@GET
	@Path("/images/{sha}/raw")
	public String GetElasticSearchImageAllEpochs(@PathParam("sha") String sha){
		
			esh = new ElasticSearchHandler();
			return esh.GetImagesAllEpochs(sha, INDEX_IMAGES, INDEX_TYPE_IMAGE);
	}
	
	@GET
	@Path("/pages/{sha}/{epoch}/raw")
	public String GetElasticSearchPages(@PathParam("sha") String sha, @PathParam("epoch") String epoch){
			esh = new ElasticSearchHandler();
			return esh.GetPagesURLs(sha, epoch, INDEX_PAGES, INDEX_TYPE_PAGE);
	}
	
	@GET
	@Path("/pages/{sha}/latest/raw")
	public String GetElasticSearchPagesBySha(@PathParam("sha") String sha){
			esh = new ElasticSearchHandler();
			return esh.GetPageURLsBySha(sha, INDEX_PAGES, INDEX_TYPE_PAGE);
		}
	
	
	@GET
	@Path("/pages/{sha}/raw")
	public String GetElasticSearchAllEpochs(@PathParam("sha") String sha){
			esh = new ElasticSearchHandler();
			return esh.GetPagesAllEpochs(sha, INDEX_PAGES, INDEX_TYPE_PAGE);
	}
			
}
