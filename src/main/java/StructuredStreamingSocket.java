import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class StructuredStreamingSocket {


    public static void main(String args[]) throws StreamingQueryException {


        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SocketJOB")
                .master("local[*]")
                .getOrCreate();


        // Turn off logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        Dataset<Row> ds = sparkSession.readStream().format("socket")
                                    .option("host","localhost")
                                    .option("port", "1234").load();

        StreamingQuery query = ds.writeStream().outputMode("append")
                                    .format("console").start();


        query.awaitTermination();



    }
}
