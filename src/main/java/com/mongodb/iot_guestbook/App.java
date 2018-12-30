package com.mongodb.iot_guestbook;

import com.mongodb.embedded.client.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;
import org.bson.Document;

import java.util.Date;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * iot_guestbook!
 *
 */
public class App {
  public static void main( String[] args ) {
    String name;
    String message;

    /*
     * If they've specified their name as the first arg, let's use it in the doc
     */
    if( args.length > 0 ) {
      if( args[0].equals( "--help" ) || args.length > 2 ) {
        System.out.println( "Usage: iot_guestbook [name] [message]" );
        return;
      }

      name = args[0];
    } else {
      name = "anonymous";
    }
   
    /*
     * If they've specified a message as the second arg, let's use it in the doc
     */
    if( args.length > 1 ){
      message = args[1];
    } else {
      message = "Hello IoT World";
    }

    try {
      /*
       * Initialize the MongoDB Embedded library and create a local storage instance
       *
       * libraryPath is necessary if the MongoDB Embedded SDK is not in the default
       * JNI path, or otherwise explicitly specified using java.library.path
       */
      MongoEmbeddedSettings esettings = MongoEmbeddedSettings.builder()
          .libraryPath( "/Users/matt/mongo-embedded-sdk/lib" )
          .logLevel( MongoEmbeddedLogLevel.NONE )
          .build();
      MongoClients.init( esettings );

      // Let's make the driver logging quiet 
      Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
      mongoLogger.setLevel( Level.OFF ); 

      // Get a client object that's using the Embedded MongoDB database
      MongoClientSettings csettings = MongoClientSettings.builder()
          .dbPath( "/tmp" )
          .build();
      MongoClient mongoClient = MongoClients.create( csettings );

     /*
      * We will insert a new document in the collection and then print out
      * all existing documents in the collection
      */
      MongoDatabase db = mongoClient.getDatabase( "iot_test" );
      MongoCollection<Document> coll = db.getCollection( "sensor_data" );

      Document newDoc = new Document( "message", message )
          .append( "from", name )
          .append( "date", new Date() );
      coll.insertOne( newDoc );

      MongoCursor<Document> cursor = coll.find()
          .projection( fields( include( "message", "from", "date" ), excludeId() ) )
          .sort( orderBy( ascending( "date" ) ) )
          .iterator();
       
      while( cursor.hasNext() ) {
        System.out.println( cursor.next().toJson() );
      }
            
      mongoClient.close();
    } catch( Exception e ) {
      System.err.println( "An error has occurred: " + e );
    } finally {
      MongoClients.close();
    }

    return;
  }
}
