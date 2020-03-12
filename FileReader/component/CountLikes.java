package component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class CountLikes {
	
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
	
	public ResultSet getArtistsQuery(String user) throws SQLException {
		
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "SELECT  DISTINCT \"musicbrainzArtistId\" FROM public.first_app_registry\n" + 
						"where \"userId\"='"+user+"';";
		
		try {
			return st.executeQuery(querty);
		}
		catch(Exception e) {
			System.out.println("************ERROR********** "+e);
			System.out.println("************querty********** "+querty);
		}
		return null;
		
	}
	
	public ResultSet getcountLikesQuery(String user, String artist) throws SQLException {
		
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "SELECT  COUNT(1) FROM public.first_app_registry\n" + 
						"WHERE \"musicbrainzArtistId\" = '" + artist + "' \n" + 
						"AND \"userId\"='"+user+"';";
		
		
		try {
			return st.executeQuery(querty);
		}
		catch(Exception e) {
			System.out.println("************ERROR********** "+e);
			System.out.println("************querty********** "+querty);
		}
		return null;
		
	}
	
	public void setQualify(String qualify, String user, String artist) throws SQLException {
		
		Statement st = connection.createStatement();
		//ResultSet rs = 
		
		String querty = "UPDATE public.\"COUNT_LIKES\"\n" + 
						"SET \"like\" = '"+qualify+"'\n" + 
						"where \"userId\" = '"+user+"' and  \"artistId\" = '"+artist+"';";
		
		try {
			st.executeQuery(querty);
		}
		catch(Exception e) {
			//System.out.println("************ERROR********** "+e);
			//System.out.println("************querty********** "+querty);
		}
		
	}
	
	public String getValue(String value) {
		
		int val = 0;
		
		try {
			val = Integer.parseInt(value);
			System.out.println("************CALIFICACION********** "+ val);
		}
		catch(Exception e) {
			//System.out.println("************ERROR asignando calificacion********** "+ "valor casteado" + val + e);
		}
		
		if(val == 0) {return "1";}
		if(val >= 1 && val <= 4) {return "2";}
		if(val == 5 ) {return "3";}
		if(val >= 6 && val <= 10) {return "4";}
		if(val >= 10) {return "5";}
				
		return "1";
	}
	
	
	
	public static void main(String [] args) throws IOException, SQLException {
		
		CountLikes countLikes  = new CountLikes();
		countLikes.connectDatabase();
		ResultSet resultSetUsers = countLikes.getUsersQuery();
		ResultSet resultSetArtists = null;
			
		/***
		
		ResultSet getcountLikesQuery = countLikes.getcountLikesQuery("user_000001", "05fd92f7-60d7-4fe9-95e2-7ff597e755db");
		
		while(resultSetUsers.next()) {
			System.out.println("************USUARIO********** " + resultSetUsers.getString(1));
		}
		
		while(resultSetArtists.next()) {
			System.out.println("************ARTISTAS********** " + resultSetArtists.getString(1));
		}
		
		while(getcountLikesQuery.next()) {
			System.out.println("************USUARIO********** " + getcountLikesQuery.getString(1));
		}
		
		**/
		
		int con = 0;
		while(resultSetUsers.next()) {
			
			System.out.println("************USUARIO********** " + con++);
			int con_ = 0;
			resultSetArtists = countLikes.getArtistsQuery(resultSetUsers.getString(1));
			
			try {
				while(resultSetArtists.next()) {
					ResultSet getcountLikesQuery = countLikes.getcountLikesQuery(resultSetUsers.getString(1), resultSetArtists.getString(1));
					
					while(getcountLikesQuery.next()) {
												
						System.out.println("USUARIO: " + con + " ARTISTA: "+ con_++);
						try {
							countLikes.setQualify(countLikes.getValue(getcountLikesQuery.getString(1)), resultSetUsers.getString(1),resultSetArtists.getString(1));
						}
						catch(Exception e) {
							//System.out.println("************ERROR CALIFICANDO********** " + e);
						}	
					}
					//break;
				}
			}
			catch(Exception e) {
				//System.out.println("error" + e);
			}
			
			
		}
		
	}

}
