package dbase;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.SortedSet;
import java.util.TreeSet;

public class MongoConnection {

    //private final MongoClientURI uri = new MongoClientURI(Utils.URL_DATABASE);
    private MongoClient client1;
    private MongoDatabase database;

    public SortedSet<String> getKeywords() {
    	this.openDB();
    	MongoCollection<org.bson.Document> collection = database.getCollection("groups");
    	SortedSet<String> keywordsRaw = collection.distinct("keywords", String.class).into(new TreeSet<>());
    	
    	SortedSet<String> keywords = new TreeSet<>();
    	
    	for(String i : keywordsRaw) {
    		String[] raw = i.split(",");
    		
    		for(String e : raw) {
    			if(!e.isEmpty()) {
    				keywords.add(e.trim());
    			}
    		}
    		
    	}
    	client1.close();
    	return keywords;
    }
    	
    public void openDB() {
    		MongoClientOptions.Builder options = MongoClientOptions.builder();
		options.connectTimeout(50*1000);
		MongoClientURI uri = new MongoClientURI("mongodb://rafa:rafa@ds159845.mlab.com:59845/si1718-rgg-groups", options);
    		client1 = new MongoClient(uri);
		database = client1.getDatabase("si1718-rgg-groups");
    }
}
