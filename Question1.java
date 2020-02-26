package com.mycompany.saurabh_assg5;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 *
 * @author Saurabh
 */
public class Question1 {

    public static void main(String args[]) throws InterruptedException {

        //Question 1:
        //Turning off the Log
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //Spark Configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("Twitter")
                .setMaster("local[4]").set("spark.executor.memory", "1g");

        //Twitter Credentials
        String consumerKey = "DauLQPNrBPbnUbImVtvow";
        String consumerSecret = "LUgmW1mj8VjvZCv45wzi94QMBIRfsO0lnFIbKEte0c";
        String accessToken = "88361481-Hgm0cN0DRVPgI3UTF72UTb48ilA0dJfc9XzcCF0sr";
        String accessTokenSecret = "b4qGRdEnJdOAjDug2JWodUduB0vndwpQAU1eDswct4jmA";

        //Generatinge OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        //Creating Java Streaming Context
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        
        //Generating Twitter Stream
        JavaDStream<Status> stream = TwitterUtils.createStream(jssc);
        
        //Getting the tweets
        JavaDStream<String> statuses = stream.map(s -> s.getText());
        statuses.print();
        
        //Question 1 Ends ---------------------------------------
    
        
        //Question 2
        //Counting the number of Characters:
        JavaDStream<Integer> charlen = statuses.map(s -> s.split("").length);
        charlen.print();

        //Question 2
        //Counting the number of Words by 
        JavaDStream<Integer> words = statuses.map(s -> {
            String[] wordsarray = s.split(" ");
            int a = wordsarray.length;
            return a;
        });
        words.print();

        //Wxtracting the Hashtags by filtering the splitting the string and getting the words starting with #
        JavaDStream<String> hashTags = statuses.flatMap(f -> Arrays.asList(f.split(" ")).iterator())
                .filter(s -> s.startsWith("#"));
        hashTags.print();

        //Question 2 Ends -------------------------------
        
        
        
        //Question 3.a:
        //Counting Average Number of Characters and words by first splitting the words 
        //and characters and mapping for sum to get the count
        statuses.foreachRDD(f -> {
            
            //Getting the tweet count anc checking if 0 or not to avoid error.
            long countTweets = f.count() == 0 ? 1 : f.count();
            //Getting the Number of words by splitting the tweets by space and mapping them to get the count.
            long countWords = f.map(a -> a.split(" ")).map(a -> a.length).fold(0, Integer::sum);
            //Counting the character by splitting and mapping them 
            long countChar = f.map(b -> b.split("")).map(a -> a.length).fold(0, Integer::sum);
            
            //Printing the results 
            System.out.println("Average Number of Characters: " + countChar / countTweets);
            System.out.println("Average Number of Words: " + countWords / countTweets);

        });
        
        //Question 3.b
        //Counting the Top 10 Hashtags
        //Mapping the hashtags to key value pair to count them
        JavaPairDStream<String, Integer> top10hastags = hashTags.mapToPair(f -> new Tuple2(f, 1));
        //Foreach
        top10hastags.reduceByKey(Integer::sum).foreachRDD(a -> a.sortByKey(false).take(10)
                .forEach(b -> System.out.println(b)));

        
        //Question 3.c
        //Counting top 10 Hashags for the 5minute Window
        //reducebykey for 5minute window and for every 30seconds
        top10hastags.reduceByKeyAndWindow(Integer::sum, new Duration(5 * 1000 * 60), new Duration(30 * 1000))
                .foreachRDD(a -> {
                    System.out.println("Hastags for every 30 seconds window: ");
                    //Sorting in descending 
                    a.sortByKey(false).take(10)
                .forEach(b -> System.out.println(b));
                            });

        //Question 3.c 
        //Calculating the Average Characters and Words for the 5 minute window
        statuses.window(new Duration(5 * 1000 * 60), new Duration((1000 * 30)))
          .foreachRDD(f -> {
            //Counting the tweets and handeling 0 tweets to avoid error
            long countTweets = f.count() == 0 ? 1 : f.count();
            Double countWordsAverage = f.map(a -> a.split(" ")).mapToDouble(a -> a.length).mean();
            Double countCharAverage = f.map(b -> b.split("")).mapToDouble(a -> a.length).mean();

            System.out.println("Average Characters for every 30 seconds is: " + countCharAverage);
            System.out.println("Average Words for every 30 seconds is: " + countWordsAverage);
        });

        
        //Terminting the context 
        jssc.start();
        jssc.awaitTermination();

    }
}
