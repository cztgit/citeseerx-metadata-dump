package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

/**
 * @author cornelia
 * 
 */


/**
 * Runs queries against a back-end database
 */
public class CSXCitegraph {
	
	private String dburl;
	private String uname;
	private String upwd;

	// DB Connection
	private Connection _csx;
	
	private String _db_sql  = "use csx_citegraph";
	private PreparedStatement _db_statement;
	
	private String _search_citing_sql = 
		"SELECT distinct citing " +
		"FROM citegraph ";
		//"FROM citegraph LIMIT 0,5 ";
	private PreparedStatement _search_citing_statement;
	
	private String _search_cited_sql = 
		"SELECT cited " +
		"FROM citegraph WHERE citing = ? ";
	private PreparedStatement _search_cited_statement;

	private String _search_cited_v2_sql = 
		"SELECT cited, firstContext " +
		"FROM citegraph WHERE citing = ? ";
	private PreparedStatement _search_cited_v2_statement;


	public CSXCitegraph(String _url, String _uname, String _pass) {
		
		dburl = _url;
		uname = _uname;
		upwd = _pass;
		
		_csx = null;
	}

    /**********************************************************/

	public void openConnection() throws Exception {

		/* load jdbc drivers */
		Class.forName("com.mysql.jdbc.Driver");

		/* open connection to citeseerx database */
		_csx = DriverManager.getConnection(dburl, uname, upwd);			

		if (_csx == null){
			System.err.println("Could not open DB connection");
			System.exit(1);
		}
	}

	public void closeConnection() throws Exception {
		_csx.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		_db_statement = _csx.prepareStatement(_db_sql);
		_search_citing_statement = _csx.prepareStatement(_search_citing_sql);
		_search_cited_statement = _csx.prepareStatement(_search_cited_sql);
		_search_cited_v2_statement = _csx.prepareStatement(_search_cited_v2_sql);
	}

    /**********************************************************/
    /* main functions in this project: */
	
	/*
	private String _db_sql  = "use csx_citegraph";
	private PreparedStatement _db_statement;
	
	private String _search_citing_sql = 
		"SELECT distinct citing " +
		"FROM citegraph LIMIT 0,5 ";
	private PreparedStatement _search_citing_statement;
	
	private String _search_cited_sql = 
		"SELECT cited " +
		"FROM citegraph WHERE citing = ? ";
	private PreparedStatement _search_cited_statement;

	private String _search_cited_v2_sql = 
		"SELECT cited, firstContext " +
		"FROM citegraph WHERE citing = ? ";
	private PreparedStatement _search_cited_v2_statement;
	
	 */

	public void search(String path, boolean addContext) throws Exception {
		
		_db_statement.executeQuery();
		
        Document document = new Document();
        Element root = new Element("document");
        document.setContent(root);
        
        ResultSet citing_set = _search_citing_statement.executeQuery();
		while (citing_set.next()) {
			int citing = citing_set.getInt(1);
			System.out.println("ID: " + citing);
			
	        root.addContent(new Element("doicluster").setText(Integer.toString(citing)));
	        
	        Element child1 = new Element("citations");
	        root.addContent(child1);
	        
	        ResultSet citations_set;
			/* now you need to retrieve the citations, in the same manner */
	        if (addContext) {
	        	_search_cited_v2_statement.clearParameters();
	        	_search_cited_v2_statement.setInt(1, citing);
	        	
	        	citations_set = _search_cited_v2_statement.executeQuery();
	        	
	        } else {
	        	_search_cited_statement.clearParameters();
	        	_search_cited_statement.setInt(1, citing);
	        	
	        	citations_set = _search_cited_statement.executeQuery();
	        }
			while (citations_set.next()) {
				int cited = citations_set.getInt(1);
				
				Element child2 = new Element("citation");
				
				child2.addContent(new Element("clusterid").setText(Integer.toString(cited)));
				if(addContext)
					child2.addContent(new Element("contexts").setText(citations_set.getString(2)));
				
				child1.addContent(child2);
			}
			citations_set.close();
			
	        try {
	            FileWriter writer = new FileWriter(path + citing + ".xml");
	            XMLOutputter outputter = new XMLOutputter();
	            
	            outputter.setFormat(Format.getPrettyFormat());
	            
	            outputter.output(document, writer);
	            //outputter.output(document, System.out);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
		}
		citing_set.close();
		System.out.println();
	}
	
	
	public void search(String path) throws Exception {
		
		_db_statement.executeQuery();
		
		PrintWriter pw;
        
        ResultSet citing_set = _search_citing_statement.executeQuery();
		while (citing_set.next()) {
			int citing = citing_set.getInt(1);
			System.out.println("ID: " + citing);
			
			pw = new PrintWriter(new FileOutputStream(path + "p" + citing + ".txt"), true);
			
			pw.println(citing + ":");
	        
			/* now you need to retrieve the citations, in the same manner */

	        _search_cited_statement.clearParameters();
	        _search_cited_statement.setInt(1, citing);
	        ResultSet citations_set = _search_cited_statement.executeQuery();
	        while (citations_set.next()) {
				int cited = citations_set.getInt(1);
				pw.println(cited);
			}
			citations_set.close();
			pw.close();
		}
		citing_set.close();
		System.out.println();
	}
	
	public void search_v2(String path) throws Exception {
		
		_db_statement.executeQuery();
		
		PrintWriter pw = new PrintWriter(new FileOutputStream(path + "citegraph.txt"), true);
        
        ResultSet citing_set = _search_citing_statement.executeQuery();
		while (citing_set.next()) {
			int citing = citing_set.getInt(1);
			System.out.println("ID: " + citing);
			
			pw.println(citing + ":");
	        
			/* now you need to retrieve the citations, in the same manner */

	        _search_cited_statement.clearParameters();
	        _search_cited_statement.setInt(1, citing);
	        ResultSet citations_set = _search_cited_statement.executeQuery();
	        while (citations_set.next()) {
				int cited = citations_set.getInt(1);
				pw.println(cited);
			}
			citations_set.close();
		}
		citing_set.close();
		
		pw.close();
		System.out.println();
	}
	
	public static void main(String[] args) throws Exception{
		
		String dbConnectUrl="jdbc:mysql://csxdb02.ist.psu.edu";
		String dbUser="csx-prod";
		String dbPass="csx-prod";

	
		String outPath = "/data/2016_merge-csx-dblp/CSXCitegraph-result/";
		
		//boolean addContext = false;
		
		CSXCitegraph cxs = new CSXCitegraph(dbConnectUrl,dbUser,dbPass);
		
		cxs.openConnection();
		cxs.prepareStatements();
		//cxs.search(outPath, addContext);
		//cxs.search(outPath);
		cxs.search_v2(outPath);
		cxs.closeConnection();
	}
}
