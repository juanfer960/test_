package component;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

public class BuildGradeChartf {
	
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
	
	public ResultSet getUsersQuery() throws SQLException {
		
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "SELECT  DISTINCT \"userId\" FROM public.first_app_registry;";
		
		try {
			return st.executeQuery(querty);
		}
		catch(Exception e) {
			System.out.println("************ERROR********** "+e);
			System.out.println("************querty********** "+querty);
		}
		return null;
		
	}
	
	public ResultSet getArtistsQuery() throws SQLException {
		
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "SELECT  DISTINCT \"musicbrainzArtistId\" FROM public.first_app_registry";
		
		try {
			return st.executeQuery(querty);
			
		}
		catch(Exception e) {
			System.out.println("************ERROR********** "+e);
			System.out.println("************querty********** "+querty);
		}
		return null;
		
	}
	
	public void addRecord (String user, String artist) throws SQLException {
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "INSERT INTO public.\"COUNT_LIKES\"(\"userId\",\"artistId\") VALUES ('"+user+"','"+artist+"');";
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
	
	public void loadArtistsPerUser (String user, String artist) throws SQLException {
			
		try{
			addRecord(user,artist);
		}
		catch(Exception e) {
			System.out.println("Error" + e);
		}		
	}
	
	public static void main(String [] args) throws IOException, SQLException {
		
		BuildGradeChartf BuildGradeChartf = new BuildGradeChartf();
		BuildGradeChartf.connectDatabase();
		ResultSet resultSetUsers = BuildGradeChartf.getUsersQuery();
		ResultSet resultSetArtists = BuildGradeChartf.getArtistsQuery();
		
		
		ArrayList <String>listUsers = new ArrayList();
		ArrayList <String>listArtist = new ArrayList();
		
		while(resultSetUsers.next()) {
			
			String user = resultSetUsers.getString(1);
			listUsers.add(user);
					
		}
		
		while(resultSetArtists.next()) {
			
			String artist = resultSetArtists.getString(1);
			listArtist.add(artist);
					
		}
		
		System.out.println("Artistas" + listArtist.size());
		System.out.println("Usuarios" + listUsers.size());
		
		Iterator itr = listUsers.iterator();
		int con = 0;
		
		while(itr.hasNext()) {
			
			String user = (String) itr.next();
			con++;
			Iterator itrArtist = listArtist.iterator();
			int con_ = 0;
			
			while(itrArtist.hasNext()){
				
				try{
				String artist = (String) itrArtist.next();
				BuildGradeChartf.loadArtistsPerUser(user,artist);
				System.out.println("Usuario: "+ con +"Artista: " + con_++);
				}
				catch(Exception e) {
					System.out.println("Error" + e);
				}
			}
			
		}
		
	}

}
