package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 * @author cornelia
 * 
 */

/**
 * Runs queries against a back-end database
 */
public class CSXClusters {
	
	private String dburl;
	private String uname;
	private String upwd;

	// DB Connection
	private Connection _csx;
	
	private String _db_sql  = "use citeseerx";
	private PreparedStatement _db_statement;
	
	private String _search_clusters_sql = 
		"SELECT distinct cluster " +
		"FROM papers where public=1";
		//"FROM papers LIMIT 0,5 ";
	private PreparedStatement _search_clusters_statement;
	
	private String _search_paperids_sql = 
		"SELECT id " +
		"FROM papers WHERE cluster = ? and public=1";
	private PreparedStatement _search_paperids_statement;
	

	public CSXClusters(String _url, String _uname, String _pass) {
		
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
		_search_clusters_statement = _csx.prepareStatement(_search_clusters_sql);
		_search_paperids_statement = _csx.prepareStatement(_search_paperids_sql);
	}

    /**********************************************************/
    /* main functions in this project: */
	
	/*
	private String _db_sql  = "use citeseerx";
	private PreparedStatement _db_statement;
	
	private String _search_clusters_sql = 
		"SELECT distinct cluster " +
		"FROM papers ";
		//"FROM papers LIMIT 0,5 ";
	private PreparedStatement _search_clusters_statement;
	
	private String _search_paperids_sql = 
		"SELECT id " +
		"FROM papers WHERE cluster = ? ";
	private PreparedStatement _search_paperids_statement;
	
	 */
	
	public void search(String path) throws Exception {
		
		_db_statement.executeQuery();
		
		PrintWriter pw;
        
        ResultSet cluster_set = _search_clusters_statement.executeQuery();
		while (cluster_set.next()) {
			int cluster = cluster_set.getInt(1);
			System.out.println("ID: " + cluster);
			
			pw = new PrintWriter(new FileOutputStream(path + "c" + cluster + ".txt"), true);
			
			pw.println(cluster + ":");
	        
			/* now you need to retrieve the citations, in the same manner */

			_search_paperids_statement.clearParameters();
			_search_paperids_statement.setInt(1, cluster);
	        ResultSet papersids_set = _search_paperids_statement.executeQuery();
	        while (papersids_set.next()) {
				String id = papersids_set.getString(1);
				pw.println(id);
			}
	        papersids_set.close();
			pw.close();
		}
		cluster_set.close();
		System.out.println();
	}
	
	public void search_v2(String path) throws Exception {
		
		_db_statement.executeQuery();
		
		PrintWriter pw = new PrintWriter(new FileOutputStream(path + "clusters.txt"), true);
        
        ResultSet cluster_set = _search_clusters_statement.executeQuery();
		while (cluster_set.next()) {
			int cluster = cluster_set.getInt(1);
			System.out.println("ID: " + cluster);
			
			pw.println(cluster + ":");
	        
			/* now you need to retrieve the citations, in the same manner */

			_search_paperids_statement.clearParameters();
			_search_paperids_statement.setInt(1, cluster);
	        ResultSet papersids_set = _search_paperids_statement.executeQuery();
	        while (papersids_set.next()) {
				String id = papersids_set.getString(1);
				pw.println(id);
			}
	        papersids_set.close();
		}
		cluster_set.close();
		
		pw.close();
		System.out.println();
	}
	
	public static void main(String[] args) throws Exception{
		
		String dbConnectUrl="jdbc:mysql://csxdb02.ist.psu.edu:3306";
		String dbUser="csx-prod";
		String dbPass="csx-prod";

		String outPath = "/data/2016_merge-csx-dblp/CSXClusters-result/";
		
		//boolean addContext = false;
		
		CSXClusters cxs = new CSXClusters(dbConnectUrl,dbUser,dbPass);
		
		cxs.openConnection();
		cxs.prepareStatements();
		//cxs.search(outPath);
		cxs.search_v2(outPath);
		cxs.closeConnection();
	}
}
