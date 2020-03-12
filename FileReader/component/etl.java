package component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Date;

public class etl {
	
	Connection connection = null;
	
	public void connectDatabase() {
		try {
          
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/taller_1","postgres", "FOXfox7913");
            boolean valid = connection.isValid(50000);
            System.out.println(valid ? "TEST OK" : "TEST FAIL");
            
        } catch (java.sql.SQLException sqle) {
            System.out.println("Error: " + sqle);
        }
	}
	
	public void DBQuery(String id,String userId,String date,String musicbrainzArtistId,String artistName,String musicbrainzTrackId,String trackName) throws SQLException {
		
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "INSERT INTO public.first_app_registry (id,\"userId\", date,\"musicbrainzArtistId\",\"artistName\",\"musicbrainzTrackId\",\"trackName\")\n" + 
				"									 VALUES ('"+id+"','"+userId+"','"+date+"','"+musicbrainzArtistId+"','"+artistName+"','"+musicbrainzTrackId+"','"+trackName+"');";
		try {
			st.executeQuery(querty);
		}
		
		catch(Exception e) {
			//System.out.println("************ERROR********** "+e);
			//System.out.println("************querty********** "+querty);
		}
		
		/**
		while(rs.next()) {
			System.out.println(rs.getString(1)+"-"+rs.getString(2)+"-"+rs.getString(3));
		}
		**/
	}
	
	public static void main(String [] args) throws IOException, SQLException {
		
		
		etl etl = new etl();
		etl.connectDatabase();
			
		File archivo = new File ("/Users/juan/Documents/SR/Talleres/Taller1/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv");
		FileReader fr = new FileReader (archivo);
		BufferedReader br = new BufferedReader(fr);
		
		String linea;
		int con = 0;
		
        //while((linea=br.readLine())!=null) {
		while((linea=br.readLine())!=null && con < 1600000) {
        	String[] parts = linea.split("\\t");
        	System.out.println(Arrays.asList(parts));
        	try{
        		etl.DBQuery("0"+con,parts[0].equals("")?null:parts[0].replace("'", ""),parts[1].equals("")?null:parts[1].replace("'", ""),parts[2].equals("")?null:parts[2].replace("'", ""),parts[3].equals("")?null:parts[3].replace("'", ""),parts[4].equals("")?null:parts[4].replace("'", ""),parts[5].equals("")?null:parts[5].replace("'", ""));
        		System.out.println("************NUMERO DE REGISTROS INSERTADOS********** "+con);
        	}
        	catch(Exception e) {
        		//System.out.println("***********************Error****************"+e);
        		//System.out.println(Arrays.asList(parts));
        	}
        	con++;
        }
		
	}

}
