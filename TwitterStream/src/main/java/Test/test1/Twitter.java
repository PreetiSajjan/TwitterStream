package Test.test1;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

/**
 * Creating Twitter class 
 * extracting tweets from it and analyzing them
 * by performing various arithematic operations
 * @author Preeti Sajjan
 *
 */
public class Twitter 
{
	public static void main(String[] args)
	{
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		System.setProperty("hadoop.home.dir", "C:/winutils");
		// Setting configuration for Spark to use 4 cores of processor and 4 Giga bytes of memory
		SparkConf sparkConf = new SparkConf()
				.setAppName("TwitterTwettsAnalysis")
				.setMaster("local[4]").set("spark.executor.memory", "4g");

		// Set the system properties so that Twitter4j library used by Twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", "uxDvzBQtcTar6CWJTD7QKCuRd");
		System.setProperty("twitter4j.oauth.consumerSecret", "G2L7CIusnuzScCXUl1VLXUiTxk8PpA7qGofilRklS3QndrGDsF");
		System.setProperty("twitter4j.oauth.accessToken", "1195840617185722374-Ow6KOXD8u5YU11PliqbX69igsf83ZY");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "ENqiUAwYA9QSp76xxhuaBaznToZC2yBRToxJhWQLA07Y0");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		
		// Establishing the Twitter stream connection
		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc);
		
		// Printing sample (10 messages) tweets it 
		// received from Twitter every second
		JavaDStream<String> messages = tweets.map(Status::getText);
		messages.print();
		
		// Printing total number of words in each tweet
		// Format: (Tweet: <Message>,Total Words: <no of words>)
		JavaPairDStream<String, String> totalWords = messages.mapToPair( f -> new Tuple2<String, String> ("Tweet: " + f,
				"Total Words in tweet: " + Arrays.asList(f.split(" ")).size()));
		totalWords.print();

		// Printing total number of characters in each tweet
		// Format: (Tweet: <Message>,Total Characters: <no of characters>)
		JavaPairDStream<String, String> totalCharacters = messages.mapToPair((String s) -> new Tuple2<String, String>(
				"Tweet: " + s, " Total Characters in Tweet: " + Arrays.asList(s.split("")).size()));
		totalCharacters.print();

		// Printing total number of hashtags in each tweet
		// Format: (Tweet: <Message> Hastags: ,[])
		JavaPairDStream<String, List<String>> hashtags =  messages
				.mapToPair((String s) -> new Tuple2<String, List<String>>(
						"Tweet: " + s + " Hastags: ",
						Arrays.asList(s.split(" ")).stream()
							.filter(x -> x.startsWith("#"))
							.collect(Collectors.toList())));
		hashtags.print();		
				
		// Creating an instance of counting class 
		// performing all counting operations
		Counting count = new Counting();
		
		// Calculating and Printing average number of words per tweet
		count.averagOfWords(messages);

		// Calculating and Printing average number of characters per tweet
		count.averagOfCharacters(messages);

		// Printing top 10 hashtags
		int noOfHastags = 10;
		count.getTopNHashtags(hashtags, noOfHastags);

		// Repeat computation for the last 5 minutes of tweets
		// every 30sec.
		int interval = 30 * 1000;
		int windowSize = 5 * 60 * 1000;
		count.windowedOperations(messages, hashtags, noOfHastags, windowSize,
			interval);		
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}