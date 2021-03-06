package data.streaming.test;

import java.util.ArrayList;
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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import dbase.MongoConnection;


public class Batch {
	private static MongoConnection mongoConnect;
    private static MongoDatabase database;
    private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    
    static void connectionDB() {
    		MongoClientURI uri = new MongoClientURI("mongodb://rafa:rafa@ds159845.mlab.com:59845/si1718-rgg-groups");
    		mongoConnect = new MongoConnection();
		MongoClient client = new MongoClient(uri);
		database = client.getDatabase("si1718-rgg-groups");
    } 
    
    
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
    
    
    public static List <Document> collectionTweetsToList (MongoDatabase database) {
		List <Document> res = new ArrayList <> ();
		MongoCollection<org.bson.Document> collectionTweets = database.getCollection("tweets");
		
		// Paso los grupos de la coleccion a una lista
		FindIterable<Document> findIter = collectionTweets.find();
		MongoCursor<Document> cursor = findIter.iterator();
		while (cursor.hasNext()) {
			res.add(cursor.next());
		}
		
		return res;
    }
    
    
    public static List <Document> collectionChartsDataToList (MongoDatabase database) {
		List <Document> res = new ArrayList <> ();
		MongoCollection<org.bson.Document> collectionChartsData = database.getCollection("chartsData");
		
		// Paso los grupos de la coleccion a una lista
		FindIterable<Document> findIter = collectionChartsData.find();
		MongoCursor<Document> cursor = findIter.iterator();
		while (cursor.hasNext()) {
			res.add(cursor.next());
		}
		
		return res;
    }
    
    
	public static void ratingGroups () {
		List <Document> ratingsList = new ArrayList <> ();
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
				//DBObject document = new BasicDBObject();
				Document document = new Document ();
				
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
		MongoCollection<org.bson.Document> collectionRatings = database.getCollection("ratings");
		collectionRatings.drop();
		System.out.println("INFORMATION: RatingsCollection have been dropped");
		
		//collectionRatings.insert(new ArrayList<>(ratingsList));
		collectionRatings.insertMany(ratingsList);
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
	
	
	public static List <Document> optimizeChartsDataCollection (List <Document> dataList) {
		List <Document> res = new ArrayList <> ();
		List <Document> listChartsData =  collectionChartsDataToList (database);
		/*System.out.println("----------CHARTSDATA----------");
		for (Document d : listChartsData) {
			System.out.println(d);
		}
		System.out.println("------------------------------");
		System.out.println("------------------------------");*/
		
		//Para sumar el doc nuevo a otro existente en la coleccion chartsData o insertarlo si no existe
		for (int i = 0; i < dataList.size(); i++) {
			String creationD = dataList.get(i).get("creationDate").toString();
			String keyD = dataList.get(i).get("keyword").toString();
			Integer tweetsD = Integer.parseInt( dataList.get(i).get("numTweets").toString() );
			
			Integer exist = 0;
			String creationDChartsData = "";
			String keyDChartsData = "";
			Integer tweetsChartsData = 0;
			for (int j = 0; j < listChartsData.size(); j++) {
				Document doc = new Document ();
				creationDChartsData = listChartsData.get(j).get("creationDate").toString();
				keyDChartsData = listChartsData.get(j).get("keyword").toString();
				tweetsChartsData = Integer.parseInt( listChartsData.get(j).get("numTweets").toString() );
				
				if ( (creationDChartsData).contentEquals(creationD)
						&& (keyDChartsData).contentEquals(keyD)) {
					tweetsChartsData = tweetsD + tweetsChartsData;
					doc.append("creationDate", creationDChartsData).append("keyword", keyDChartsData)
					.append("numTweets", tweetsChartsData);
					res.add(doc);
					exist = 1;
				}
			}
			
			if (exist == 0) { //There is a new date
				Document doc1 = new Document ("creationDate", creationD)
						.append("keyword", keyD).append("numTweets", tweetsD);
				res.add(doc1);
			}
		}
		
		Set <String> fechs = new HashSet <> (); // Fechas de dataList
		for (int z = 0; z < dataList.size(); z++) {
			fechs.add( dataList.get(z).get("creationDate").toString() );
		}
		
		List <String> keysfech1 = new ArrayList <> (); // Penultimo dia
		List <String> keysfech2 = new ArrayList <> (); // Ultimo dia
		List <String> f = new ArrayList <> ();
		for (int k = 0; k < dataList.size(); k++) { // Para ver las keywords nuevas que se han obtenido de cada fecha
			for (String g : fechs) { f.add(g); }
			if ( (dataList.get(k).get("creationDate").toString()).contentEquals( f.get(0).toString() ) ) {
				keysfech1.add( dataList.get(k).get("keyword").toString() );
			}
			else if ( (dataList.get(k).get("creationDate").toString()).contentEquals( f.get(1).toString() ) ) {
				keysfech2.add( dataList.get(k).get("keyword").toString() );
			}
		}
		
		for (int l = 0; l < listChartsData.size(); l++) {
			String creationDChartsData = listChartsData.get(l).get("creationDate").toString();
			String keyDChartsData = listChartsData.get(l).get("keyword").toString();
			Integer tweetsDChartsData = Integer.parseInt( listChartsData.get(l).get("numTweets").toString() );
			
			for (int m = 0; m < fechs.size(); m++) {
				if ( (creationDChartsData).contentEquals(f.get(m).toString())) {
					if (m == 0) {
						if ( !(keysfech1.contains(keyDChartsData)) ) { // Si la clave no esta en keysfech1, se agrega el doc antiguo a res
							Document doc2 = new Document ("creationDate", creationDChartsData)
									.append("keyword", keyDChartsData).append("numTweets", tweetsDChartsData);
							res.add(doc2);
						}
					}
					else if ( !(keysfech2.contains(keyDChartsData)) ) { // Si la clave no esta en keysfech2, se agrega el doc antiguo a res
							Document doc3 = new Document ("creationDate", creationDChartsData)
									.append("keyword", keyDChartsData).append("numTweets", tweetsDChartsData);
							res.add(doc3);
					}
					
				}
			}
			
			if ((fechs.size() == 1) && !((creationDChartsData).contentEquals(f.get(0).toString()))) {
				Document doc4 = new Document ("creationDate", creationDChartsData)
						.append("keyword", keyDChartsData).append("numTweets", tweetsDChartsData);
				res.add(doc4);
			}
			
			if ((fechs.size() == 2) && !((creationDChartsData).contentEquals(f.get(0).toString())) 
					&& !((creationDChartsData).contentEquals(f.get(1).toString())) ) {
				Document doc5 = new Document ("creationDate", creationDChartsData)
						.append("keyword", keyDChartsData).append("numTweets", tweetsDChartsData);
				res.add(doc5);
			}
		}
		
		return res;
	}
	
	
	public static void chartsData () {
		MongoCollection<org.bson.Document> collectionTweets = database.getCollection("tweets");
		
		// Conexion y obtencion de keywords
		System.out.println("Getting keywords to charts data");
		SortedSet<String> keywords = mongoConnect.getKeywords();
		
		Set <String> stringKEYWORDS = new HashSet <> ();
		
		for (String s:keywords) {
			if (s.contentEquals("biotech") || s.contentEquals("science") || s.contentEquals("biology")
					|| s.contentEquals("technology") || s.contentEquals("tic") || s.contentEquals("comunicacion")
					|| s.contentEquals("health") || s.contentEquals("energy") || s.contentEquals("humanities")) {
				stringKEYWORDS.add(s);
			}
		}
		
		final String[] keywordsToSearch = stringKEYWORDS.toArray(new String[stringKEYWORDS.size()]);
		
		List <Document> tweetsList = collectionTweetsToList (database);
		List <Document> dataList = new ArrayList <> ();
		
		if (dataList.isEmpty()) {
			Document dIni = new Document("creationDate", "00-00-0000").append("keyword", "prueba")
					.append("numTweets", 0);
			dataList.add(dIni);
		}
		
		System.out.println("Getting charts data");
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
		System.out.println("Charts data have been obtained");
		
		List <Document> listRes = optimizeChartsDataCollection(dataList);
		/*System.out.println("//////DATALIST//////");
		for (Document docu:dataList) {
			System.out.println(docu);
		}
		System.out.println("////////////////////");
		System.out.println(":::::::::::::::::::::::::::::::::::::::::");
		System.out.println("LIST OF DATA FOR THE CHARTS::");
		for (Document docu:listRes) {
			System.out.println(docu);
		}
		System.out.println(listRes.size());
		System.out.println(":::::::::::::::::::::::::::::::::::::::::");*/
		
		collectionTweets.drop();
		
		// Database insertion
		if (dataList.size() > 0) {
			MongoCollection<org.bson.Document> collectionChartsData = database.getCollection("chartsData");
			collectionChartsData.drop();
			collectionChartsData.insertMany(listRes);
			dataList.clear();
			System.out.println("INFORMATION: Documents with charts data inserted");
		}
		
		//database.createCollection("tweets");
		System.out.println("INFORMATION: tweetsCollection have been dropped and created again");
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
	
	
	public static void recommendations () {
		MongoCollection<org.bson.Document> collectionRatings = database.getCollection("ratings");

		//List <DBObject> recommendationsList = new ArrayList <> ();
		List <Document> recommendationsList = new ArrayList <> ();
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
			//DBObject document = new BasicDBObject();
			Document document = new Document ();
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
		
		// Insert recomendations into the database
		MongoCollection<org.bson.Document> collectionRecommendations = database.getCollection("recommendations");
		collectionRecommendations.drop();
		System.out.println("INFORMATION: RecommendationsCollection have been dropped");
		
		collectionRecommendations.insertMany(recommendationsList);
		System.out.println("INFORMATION: New recommendations inserted into the database");
	}
	
	
	public static void executor() {
		final Runnable beeper = new Runnable () {
			public void run () {
				
				System.out.println("Starting to run the applications in the executor batch");
				ratingGroups();
				chartsData();
				recommendations();
				System.out.println("Executor batch finished");
			}
		};
		final ScheduledFuture<?> beeperHandle =
				scheduler.scheduleAtFixedRate(beeper, 0, 2, TimeUnit.HOURS);
	}
	
	
	public static void main(String... args) throws Exception{
		connectionDB();
		executor();
	}

}
