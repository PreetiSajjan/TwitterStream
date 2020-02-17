package Test.test1;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

/**
 * Creating class counting 
 * to perform all counting related tasks
 * @author Preeti Sajjan
 *
 */
public class Counting {

	long wordCount = 0L;
    long characterCount = 0L;
    long tweetCount = 0L;
	
    /**
     * Prints average number of words per tweet
     * @param messages with list of tweets
     */
	public void averagOfWords(JavaDStream<String> messages)
	{
		// Counting number of tweets
		messages.foreachRDD(x -> {
			tweetCount += x.count();
		});		
		messages.flatMap(t -> Arrays.asList(t.split(" ")).iterator())
		.foreachRDD(w -> {
			wordCount += w.count();
			if (tweetCount > 0L)
			{
				System.out.println();
				System.out.println("Total Words: " + wordCount);
				System.out.println("Total Tweets: " + tweetCount);
				System.out.println("Average number	of words per tweet : "
								+ (Double.valueOf(wordCount)/ Double.valueOf(tweetCount)));
				wordCount = 0L;
				tweetCount = 0L;
			}
		});
	}

	/**
	 * Prints average number of characters per tweet
	 * @param messages with list of tweets
	 */
	public void averagOfCharacters(JavaDStream<String> messages)
	{
		messages.foreachRDD(x -> {
			tweetCount += x.count();
		});
		messages.flatMap(t -> Arrays.asList(t.split("")).iterator())
		.foreachRDD(w -> {
			characterCount += w.count();
			if (characterCount > 0L)
			{
				System.out.println();
				System.out.println("Total Characters: " + characterCount);
				System.out.println("Total Tweets: " + tweetCount);
				System.out.println("Average number	of characters per tweet : "
								+ (Double.valueOf(characterCount)
										/ Double.valueOf(tweetCount)));
				characterCount = 0L;
				tweetCount = 0L;
			}
		});
	}

	/**
	 * Printing top 10 hashtags
	 * @param hashTags Tuple of <Tweet, List of Hashtags>
	 * @param n number of hashtags to display
	 */
	public void getTopNHashtags(JavaPairDStream<String, List<String>> hashTags, int n)
	{
		JavaPairDStream<Integer, String> hastagsSorted = hashTags
				.map(tuple -> tuple._2()).flatMap(hastag -> hastag.iterator())
				.mapToPair(hastag -> new Tuple2<String, Integer>(hastag, 1))
				.reduceByKey((x, y) -> x + y).mapToPair(tuple -> tuple.swap())
				.transformToPair(tuple -> tuple.sortByKey(false));

		hastagsSorted.foreachRDD(new VoidFunction<JavaPairRDD<Integer, String>>()
		{
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaPairRDD<Integer, String> rdd)
			{
				String output = "\nTop 10 hashtags:\n";
				for (final Tuple2<Integer, String> t : rdd.take(n))
				{
					output = output + t.toString() + "\n";
				}
				System.out.println(output);
			}
		});
	}

	/**
	 * Repeat computation for the window of windowSize length
	 * @param messages with list of tweets
	 * @param n number of hashtags
	 * @param windowSize is the size for displaying total hashtags
	 * @param interval time interval for displaying hashtags
	 */
	public void windowedOperations(JavaDStream<String> messages, JavaPairDStream<String, List<String>> hashtags, int n,
			long windowSize, long interval)
	{
		JavaDStream<String> windowedMessages = messages
				.window(new Duration(windowSize), new Duration(interval));
		averagOfWords(windowedMessages);
		averagOfCharacters(windowedMessages);
		getTopNHashtags(hashtags, n);
	}
}