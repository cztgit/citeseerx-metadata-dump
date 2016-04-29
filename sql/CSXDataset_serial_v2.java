package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Verifier;
import org.jdom.Content;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

/**
 * @author cornelia
 * 
 */

/**
 * Runs queries against a back-end database
 */
public class CSXDataset_serial_v2 {
	
	private String dburl;
	private String uname;
	private String upwd;

	// DB Connection
	private Connection _csx;

	// Canned queries
	private String _db_sql  = "use citeseerx";
	//private String _db_sql  = "select count(*) from citeseerx.papers";
	private PreparedStatement _db_statement;

	private String _search_sql =
		"SELECT id, cluster, title, abstract, year, venue " +
		"FROM papers where public=1 order by id";
	private PreparedStatement _search_statement;

	private String _authors_id_sql = 
		"SELECT name " +
		"FROM authors " +
		"WHERE paperid = ? ";
	private PreparedStatement _authors_id_statement;
	
	private String _citations_id_sql = 
		"SELECT id, cluster, raw " +
		"FROM citations " +
		"WHERE paperid = ? ";
	private PreparedStatement _citations_id_statement;
	
	private String _contexts_sql = 
		"SELECT context " +
		"FROM citationContexts " +
		"WHERE citationid = ? ";
	private PreparedStatement _contexts_statement;

	public CSXDataset_serial_v2(String _url, String _uname, String _pass) {
		
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
		
		_search_statement = _csx.prepareStatement(_search_sql);
		_authors_id_statement = _csx.prepareStatement(_authors_id_sql);
		_citations_id_statement = _csx.prepareStatement(_citations_id_sql);
		_contexts_statement = _csx.prepareStatement(_contexts_sql);
	}

    /**********************************************************/
    /* main functions in this project: */
	
	/*
	 * 		
	private String _db_sql  = "use citeseerx";
	private PreparedStatement _db_statement;

	private String _search_sql = 
		"SELECT id, cluster, title, abstract, year, venue " +
		"FROM papers ";
	private PreparedStatement _search_statement;

	private String _authors_id_sql = 
		"SELECT name " +
		"FROM authors " +
		"WHERE paperid = ? ";
	private PreparedStatement _authors_id_statement;
	
	private String _citations_id_sql = 
		"SELECT id, cluster, raw " +
		"FROM citations " +
		"WHERE paperid = ? ";
	private PreparedStatement _citations_id_statement;
	
	private String _contexts_sql = 
		"SELECT context " +
		"FROM citationContexts " +
		"WHERE citationid = ? ";
	private PreparedStatement _contexts_statement;
	 */

	public void search(String path) throws Exception {
		
		System.out.println("executing db statement...");
		ResultSet num_set =_db_statement.executeQuery();
		//System.out.println("#papers: "+num_set.getString(1));
		System.out.println("finish db statement");
		
		int _NumPapers = 0;

		//"SELECT id, cluster, title, abstract, year, venue " +
		System.out.println("searching database...");
		ResultSet papers_set = _search_statement.executeQuery();
		System.out.println("Finished searching database.");

		while (papers_set.next()) {
			_NumPapers++;
			
			String pid = papers_set.getString(1);
			// check if this [pid].xml is already in the result directory
			File f = new File(path + pid + ".xml");
			if (f.exists()) {
			    System.out.println("result exist: "+path+pid+".xml");
			    continue;
			}
			System.out.println( _NumPapers + ": " + pid);
			
	        	Document document = new Document();
	        	Element root = new Element("document");
	        	document.setContent(root);
			
	        	root.addContent(new Element("doi").setText(pid));
	        	root.addContent(new Element("doicluster").setText(papers_set.getString(2)));
	        
		        if(papers_set.getString(3) != null) {
		        	String title = checkXML(papers_set.getString(3));
		        	root.addContent(new Element("title").setText(title));
		        } else {
		        	root.addContent(new Element("title").setText(papers_set.getString(3)));
		        }
		        
		        if(papers_set.getString(4) != null) {
		        	String abstr = checkXML(papers_set.getString(4));
		        	root.addContent(new Element("abstract").setText(abstr));
		        } else {
		        	root.addContent(new Element("abstract").setText(papers_set.getString(4)));
		        }
	        
			/* do a dependent join with authors */
	        //"SELECT name " +
			_authors_id_statement.clearParameters();
			_authors_id_statement.setString(1, pid);
			ResultSet authors_set = _authors_id_statement.executeQuery();
			String auths = new String();
			while (authors_set.next()) {
				auths += authors_set.getString(1) + ", ";
			}
			authors_set.close();
			if(auths.length() > 2)
				auths = auths.substring(0, auths.length()-2);
			
			if(auths != null){
				String auth = checkXML(auths);
				root.addContent(new Element("authors").setText(auth));
			} else {
				root.addContent(new Element("authors").setText(auths));
			}
			
			if(papers_set.getString(5) != null) {
				String year = checkXML(papers_set.getString(5));
				root.addContent(new Element("year").setText(year));
			} else {
				root.addContent(new Element("year").setText(papers_set.getString(5)));
			}
			
			if(papers_set.getString(6) != null) {
				String venue = checkXML(papers_set.getString(6));
				root.addContent(new Element("venue").setText(venue));
			} else {
				root.addContent(new Element("venue").setText(papers_set.getString(6)));
			}
			
			
	        	Element child1 = new Element("citations");
	        	root.addContent(child1);
	        
			/* now you need to retrieve the citations, in the same manner */
	        //"SELECT id, cluster, raw " +
			_citations_id_statement.clearParameters();
			_citations_id_statement.setString(1, pid);
			ResultSet citations_set = _citations_id_statement.executeQuery();
			while (citations_set.next()) {
				int cid = citations_set.getInt(1);
				int clusterid = citations_set.getInt(2);
				
				Element child2 = new Element("citation");
				
				if(citations_set.getString(3) != null) {
					String r = checkXML(citations_set.getString(3));
					child2.addContent(new Element("raw").setText(r));
				} else {
					child2.addContent(new Element("raw").setText(citations_set.getString(3)));
				}
				
				//"SELECT context " +
				_contexts_statement.clearParameters();
				_contexts_statement.setInt(1, cid);
				ResultSet contexts_set = _contexts_statement.executeQuery();
				String contexts = new String();
				while(contexts_set.next()) {
					contexts += contexts_set.getString(1) + " ";
				}
				contexts_set.close();
				if(contexts.length() > 1)
					contexts = contexts.substring(0, contexts.length()-1);
				
				if(contexts != null) {
					String ctx = checkXML(contexts);
					child2.addContent(new Element("contexts").setText(ctx));
				} else {
					child2.addContent(new Element("contexts").setText(contexts));
				}
				
				child2.addContent(new Element("clusterid").setText(Integer.toString(clusterid)));
				
				child1.addContent(child2);
			}
			citations_set.close();
			
	        	try {
	            		FileWriter writer = new FileWriter(path + pid + ".xml");
	            		XMLOutputter outputter = new XMLOutputter();

			        // Set the XLMOutputter to pretty formatter. This formatter
			        // use the TextMode.TRIM, which mean it will remove the
			        // trailing white-spaces of both side (left and right)
			        //
			        outputter.setFormat(Format.getPrettyFormat());
			            
			        //
			        // Write the document to a file and also display it on the
			        // screen through System.out.
			        //
			        outputter.output(document, writer);
			        //outputter.output(document, System.out);
	        	} catch (IOException e) {
	            		e.printStackTrace();
	        	}
		}
		papers_set.close();
		System.out.println();
	}
	
	private String checkXML(String content){
		
		String s = new String();
		
		for (int i = 0; i < content.length(); i ++){
			
			if(!(Verifier.isXMLCharacter(content.charAt(i))))
				continue;
			s += content.charAt(i);
		}
		
		return s;
	}
	
	public static void main(String[] args) throws Exception{
		
		String dbConnectUrl="jdbc:mysql://csxdb02.ist.psu.edu:3306";
		String dbUser="csx-prod";
		String dbPass="csx-prod";
		// bug. should not need to add "/" at the end of outPath
		String outPath = "/data/2016_merge-csx-dblp/CSXDataset_serial_v2-result/";
		
		CSXDataset_serial_v2 cxs = new CSXDataset_serial_v2(dbConnectUrl,dbUser,dbPass);
		
		System.out.println("Openning connection...");
		cxs.openConnection();

		System.out.println("Preparing statement...");
		cxs.prepareStatements();
		
		long timeStart = 0, timeElapsed = 0;
    		timeStart = System.currentTimeMillis();
		System.out.println(new Time(System.currentTimeMillis()));
		
		System.out.println("Searching data...");
		cxs.search(outPath);
		
		timeElapsed = System.currentTimeMillis() - timeStart;
		System.out.println(new Time(System.currentTimeMillis()));
        	System.out.println("\nTime taken: " 
        		+ (timeElapsed / 1000.0) + " seconds\n\n");
		
		cxs.closeConnection();
	}
	
}
