import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Reading from socket and writing to kafka
 */
public class SSWritingToKafka {

    public static void main(String args[]) throws StreamingQueryException {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KafkaWritingJOB")
                .master("local[*]")
                .getOrCreate();


        // Turn off logging

        Dataset<Row> ds = sparkSession.readStream().format("socket")
                .option("host","localhost")
                .option("port", "1234").load();


        StreamingQuery query = ds
                .selectExpr("CAST(value AS STRING) AS key","CAST(value AS STRING) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "ETMS-TOPIC")
                .option("checkpointLocation", "/Users/dharshekthvel/ac/code/")
                .start();

        query.awaitTermination();



    }

}
