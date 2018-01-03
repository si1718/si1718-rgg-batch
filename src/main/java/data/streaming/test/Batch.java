package data.streaming.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import dbase.MongoConnection;
import dbase.Utils;


public class Batch {
	
	private final static MongoClientURI uri = new MongoClientURI(Utils.URL_DATABASE);
    private static MongoClient client;
    private static MongoDatabase database;
    private static DB db;
    private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    
    public static List <Document> collectionGroupsToList (MongoDatabase database) {
    		List <Document> res = new ArrayList <> ();
    		MongoCollection<org.bson.Document> collectionGroups = database.getCollection("groups");
    		
    		// Paso los grupos de la coleccion a una lista
    		FindIterable<Document> findIter = collectionGroups.find();
    		MongoCursor<Document> cursor = findIter.iterator();
    		while (cursor.hasNext()) {
    			res.add(cursor.next());
    		}
    		
    		return res;
    }
    
	public static void ratingGroups () /*throws MalformedURLException*/ {
		// TODO Auto-generated method stub
		/*client = new MongoClient(uri);
		database = client.getDatabase("si1718-rgg-groups");
		db = client.getDB("si1718-rgg-groups");*/
		
		Set <DBObject> ratingsList = new HashSet <> ();
		List <Document> groupList = collectionGroupsToList (database);
		//int soncero = 0;
		//int nosoncero = 0;
		
		/*for (int i = 0; i < groupList.size(); i++) {
			Object nameLeader = groupList.get(i).get("components");
			for (int a = 0; a < nameLeader.toString().split(", ").length; a++) {
				System.out.println(nameLeader.toString().split(", ")[a]);
			}
			
		}*/
		//URL url = new URL("https://si1718-dfr-researchers.herokuapp.com/api/v1/researchers?search=");
		
		// Comparo parejas de keywords y creo rating según las coincidencias
		for (int i = 0; i < groupList.size(); i++) {
			Object doc1 = groupList.get(i).get("keywords");
			String[] newdoc1 = doc1.toString().split(", ");
			/*System.out.println("....................................");
			System.out.println(doc1.toString().replace("[", "").replace("]", "") + " length=" + newdoc1.length);*/
			
			for (int j = i+1; j < groupList.size(); j++) {
				DBObject document = new BasicDBObject();
				
				Object doc2 = groupList.get(j).get("keywords");
				String[] newdoc2 = doc2.toString().split(", ");
				//System.out.println(doc2.toString().replace("[", "").replace("]", "") + " length=" + newdoc2.length);
				
				document.put("id1", groupList.get(i).get("idGroup").toString());
				document.put("id2", groupList.get(j).get("idGroup").toString());
				
				int cont = 0;
				for (int k = 0; k < newdoc1.length; k++) {
					for (int l = 0; l < newdoc2.length; l++) {
						String elem1 = newdoc1[k].replace("[", "").replace("]", "");
						String elem2 = newdoc2[l].replace("[", "").replace("]", "");
						/*if ((newdoc1[k].replace("[", "").replace("]", "")).equals(newdoc2[l].replace("[", "").replace("]", "")) == true) {
							System.out.println("::::::::::::::::::::::::");
							System.out.println("newdoc1:" + elem1 + " ....... " + groupList.get(i).get("idGroup").toString());
							System.out.println("newdoc2:" + elem2 + " ....... " + groupList.get(j).get("idGroup").toString());
							System.out.println((newdoc1[k].replace("[", "").replace("]", "")).equals(newdoc2[l].replace("[", "").replace("]", "")));
							System.out.println("::::::::::::::::::::::::");
							System.out.println("....................................");
						}*/
						
						if (( elem1 ).equals( elem2 )) {
							cont++;
						}
					}
				}
				
				if (cont != 0) {
					//nosoncero++;
					double var = 5.0; // The size of the arrays are the same
					
					if ( (newdoc1.length) != (newdoc2.length) ) {
						if ((newdoc1.length) > (newdoc2.length)) {
							var = (cont * var) / newdoc1.length;
						}
						else {
							var = (cont * var) / newdoc2.length;
						}
					}
					else {
						if (cont != newdoc1.length) {
							var = (cont * var) / newdoc1.length;
						}
					}
					
					document.put("rating", Double.toString(var));
					ratingsList.add(document);
					//System.out.println(document.toString());
				}
				else{
					//soncero++;
					//System.out.println(document.toString());
				}
			}
		}
		
		/* PRUEBAS */
		/*System.out.println("+++++++++++++++++++++++++++++");
		System.out.println("+++++++++++++++++++++++++++++");
		System.out.println("son cero: " + soncero);
		System.out.println("no son cero: " + nosoncero);
		System.out.println("+++++++++++++++++++++++++++++");
		System.out.println("+++++++++++++++++++++++++++++");*/
		/* ********* */
		
		// Inserto los ratios en la base de datos
		//MongoCollection<org.bson.Document> collectionRatings = database.getCollection("ratings");
		DBCollection collectionRatings = db.getCollection("ratings");
		WriteResult documentsRemoved = collectionRatings.remove(new BasicDBObject());
		System.out.println("(RATINGS) Number of documents are deleted: " + documentsRemoved.getN());
		
		collectionRatings.insert(new ArrayList<>(ratingsList));
		System.out.println("INFORMATION: New ratings inserted into the database");
    }
	
	
	public static void createDocChart(List <Document> dataList, Object date, String keyword) {
		int tam = dataList.size();
		
		for (int j = 0; j < tam; j++) {
			String existentDate = dataList.get(j).get("creationDate").toString();
			
			if (existentDate.equals(date.toString()) && (dataList.get(j).get("keyword").toString()).equals(keyword)) {
				Object numt = Integer.parseInt(dataList.get(j).get("numTweets").toString()) + 1;
				Document actDoc = new Document("creationDate", existentDate)
						.append("keyword", dataList.get(j).get("keyword")).append("numTweets", numt);
				dataList.remove(j);
				dataList.add(j, actDoc);
			}
			else if (j == tam-1){
				Document doc = new Document("creationDate", date)
						.append("keyword", keyword).append("numTweets", 1);
				dataList.add(doc);
				
				if ((dataList.get(0).get("creationDate").toString()).equals("00-00-0000")) {
					dataList.remove(0);
				}
			}
		}
	}
	
	
	public static void grantsData () {
		/*client = new MongoClient(uri);
		database = client.getDatabase("si1718-rgg-groups");
		db = client.getDB("si1718-rgg-groups");*/
		MongoCollection<org.bson.Document> collectionTweets = database.getCollection("tweets");
		
		// Conexion y obtencion de keywords
		MongoConnection mongoConnect = new MongoConnection();
		SortedSet<String> keywords = mongoConnect.getKeywords();
		//final String[] stringKEYWORDS = keywords.toArray(new String[keywords.size()]);
		
		Set <String> stringKEYWORDS = new HashSet <> ();
		
		for (String s:keywords) {
			if (s.contentEquals("biotech") || s.contentEquals("science") || s.contentEquals("biology")
					|| s.contentEquals("technology") || s.contentEquals("tic") || s.contentEquals("comunicacion")
					|| s.contentEquals("health") || s.contentEquals("energy") || s.contentEquals("humanities")) {
				stringKEYWORDS.add(s);
			}
		}
		
		final String[] keywordsToSearch = stringKEYWORDS.toArray(new String[stringKEYWORDS.size()]);
		
		List <Document> tweetsList = new ArrayList <> ();
		List <Document> dataList = new ArrayList <> ();
		
		// Paso los grupos de la coleccion a una lista
		FindIterable<Document> findIter = collectionTweets.find();
		MongoCursor<Document> cursor = findIter.iterator();
		while (cursor.hasNext()) {
			tweetsList.add(cursor.next());
		}
		
		if (dataList.isEmpty()) {
			Document dIni = new Document("creationDate", "00-00-0000")
					.append("numTweets", 0);
			dataList.add(dIni);
		}
		
		for (int i = 0; i < keywordsToSearch.length; i++) {
			
			for (int j = 0; j < tweetsList.size(); j++) {
				Object doc1 = tweetsList.get(j).get("creationDate");
				String[] newdoc1 = doc1.toString().split(" ");
				String year = newdoc1[newdoc1.length-1];
				String month = newdoc1[1];
				String day = newdoc1[2];
				Object date = day + "-" + month + "-" + year;
				
				if (tweetsList.get(j).get("text").toString().contains(keywordsToSearch[i])) {
					String keyword = keywordsToSearch[i];
					createDocChart(dataList, date, keyword);
				}
			}
		}
		
		/*System.out.println(":::::::::::::::::::::::::::::::::::::::::");
		System.out.println("LIST OF DATA FOR THE CHARTS::");
		for (int k = 0; k < dataList.size(); k++) {
			System.out.println(dataList.get(k));
		}
		System.out.println(":::::::::::::::::::::::::::::::::::::::::");*/
		
		// Database insertion
		if (dataList.size() > 0) {
			MongoCollection collectionChartsData = database.getCollection("chartsData");
			collectionChartsData.insertMany(dataList);
			dataList.clear();
			System.out.println("INFORMATION: Documents with charts data inserted");
		}
		
		DBCollection collectionTweetsRemoved = db.getCollection("tweets");
		WriteResult tweetsRemoved = collectionTweetsRemoved.remove(new BasicDBObject());
		System.out.println("Number of old tweets are deleted: " + tweetsRemoved.getN());
	}
	
	
	static final Comparator <org.bson.Document> RATINGS_ORDER = new Comparator<org.bson.Document>() {
		
		public int compare(org.bson.Document e1, org.bson.Document e2) {
			String rating1 = e1.get("rating").toString();
			String rating2 = e2.get("rating").toString();
			int dateCmp = (rating2).compareTo(rating1);
			
			if (dateCmp != 0)
				return dateCmp;
			
			return (Double.parseDouble(rating1) < Double.parseDouble(rating2) ? -1 :
				(Double.parseDouble(rating1) == Double.parseDouble(rating2) ? 0 : 1));
		}
		
	};
	
	
	/*public static List <String> keywordsGroup (List <Document> groupList, String idGroup, String id2) {
		System.out.println("-------------------------------------");
		List <String> res = new ArrayList <> ();
		String [] keywordsId1 = null;
		String [] keywordsId2 = null;
		
		for (Document d:groupList) {
			if (d.containsValue(idGroup)) {
				System.out.println("id1:"+idGroup);
				keywordsId1 = d.get("keywords").toString().split(", ");
				
				for (String s : keywordsId1) { s = s.replace("[", "").replace("]", ""); System.out.println(s); }
				System.out.println(keywordsId1.length);
			}
			if (d.containsValue(id2)) {
				System.out.println("id2:"+id2);
				keywordsId2 = d.get("keywords").toString().split(", ");
				for (String t : keywordsId2) { t = t.replace("[", "").replace("]", ""); System.out.println(t); }
				System.out.println(keywordsId2.length);
			}
		}
		
		for (String s2 : keywordsId2) {
			int aaa = 0;
			for (String s1 : keywordsId1) {
				if (s2.contains(s1)) {
					aaa = 1;
				}
			}
			if (aaa == 0) {
				res.add(s2);
			}
		}
		System.out.println("res:"+res.toString());
		System.out.println("-------------------------------------");
		return res;
	}*/
	
	
	@SuppressWarnings("unchecked")
	public static void recommendations () {
		/*client = new MongoClient(uri);
		database = client.getDatabase("si1718-rgg-groups");
		db = client.getDB("si1718-rgg-groups");*/
		MongoCollection<org.bson.Document> collectionRatings = database.getCollection("ratings");

		List <DBObject> recommendationsList = new ArrayList <> ();
		List <Document> groupList = collectionGroupsToList (database);
		List <Document> ratingsList = new ArrayList <> ();
		
		// Paso los ratings de la coleccion a una lista
		FindIterable<Document> findIter1 = collectionRatings.find();
		MongoCursor<Document> cursor1 = findIter1.iterator();
		while (cursor1.hasNext()) {
			ratingsList.add(cursor1.next());
		}
		
		System.out.println("Getting recommendations");
		for (int i = 0; i < groupList.size(); i++) {
			String idGroup = groupList.get(i).get("idGroup").toString();
			DBObject document = new BasicDBObject();
			document.put("idGroup", idGroup);
			Set <String> recommendationsIds = new HashSet <> ();
			//String idGroup = "fqm202"; //String idGroup = "hum245";
			List <Document> ratings = new ArrayList <> ();
			//System.out.println("/////////////////////////////");
			for (Document d : ratingsList) {
				if ( (d.get("id1").toString()).contentEquals(idGroup)
					|| (d.get("id2").toString()).contentEquals(idGroup) ) {
					ratings.add(d);
				}
			}
			
			Collections.sort(ratings, RATINGS_ORDER);
			
			for (int j = 0; j < 4; j++) {
				//System.out.println(ratings.get(j).toString());
				String id1 = ratings.get(j).getString("id1");
				String id2 = ratings.get(j).getString("id2");
				if (!(id1.equals(id2)) && id1.equals(idGroup)) {
					recommendationsIds.add(id2);
					//keywordsGroup(groupList, id1, id2);
				}
				else if (!(id1.equals(id2)) && id2.equals(idGroup)) {
					recommendationsIds.add(id1);
					//keywordsGroup(groupList, id2, id1);
				}
			}
			
			document.put("groupsRecommended", recommendationsIds);
			recommendationsList.add(document);
			/*System.out.println("RECOMMENDATIONS:");
			for (String rI : recommendationsIds) {
				System.out.println(rI);
			}
			
			System.out.println("/////////////////////////////");*/
		}
		
		/*for (DBObject rL:recommendationsList) {
			System.out.println(rL.toString());
		}*/
		
		// Inserto las recomendaciones en la base de datos
		DBCollection collectionRecommendations = db.getCollection("recommendations");
		WriteResult documentsRemoved = collectionRecommendations.remove(new BasicDBObject());
		System.out.println("(RECOMMENDATIONS) Number of documents are deleted: " + documentsRemoved.getN());
		
		collectionRecommendations.insert(recommendationsList);
		System.out.println("INFORMATION: New recommendations inserted into the database");
	}
	
	
	public static void executor() {
		final Runnable beeper = new Runnable () {
			public void run () {
				ratingGroups();
				grantsData();
				recommendations();
			}
		};
		final ScheduledFuture<?> beeperHandle =
				scheduler.scheduleAtFixedRate(beeper, 1, 12, TimeUnit.HOURS);
	}
	
	
	public static void main(String... args) throws Exception{
		client = new MongoClient(uri);
		database = client.getDatabase("si1718-rgg-groups");
		db = client.getDB("si1718-rgg-groups");
		
		executor();
		
		client.close();
	}

}
