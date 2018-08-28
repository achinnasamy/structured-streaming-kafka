import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredStreamingKafka {


    public static void main(String args[]) throws StreamingQueryException {


        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KafkaJOB")
                .master("local[*]")
                .getOrCreate();


        Dataset<Row> df = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "ETMS-TOPIC")
                .load();

//        StreamingQuery query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream().outputMode("append")
//                .format("console").start();

        StreamingQuery query = df.writeStream().outputMode("append")
                .format("console").start();


        query.awaitTermination();


    }


}
