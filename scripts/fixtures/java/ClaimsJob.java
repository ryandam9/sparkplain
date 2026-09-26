// A small Java Spark job for sparkplain's fixtures: a JVM application whose
// event log records user stack frames (file and line) for its actions, which
// PySpark jobs mostly do not.
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class ClaimsJob {
    private ClaimsJob() {}

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("claims_java_fixture").getOrCreate();
        Dataset<Row> claims = loadClaims(spark);
        long total = claims.count();
        System.out.println("claims: " + total);
        Dataset<Row> byProvider = claims.groupBy(col("provider")).count();
        byProvider.collectAsList();
        writeTotals(byProvider, args[0]);
        spark.stop();
    }

    static Dataset<Row> loadClaims(SparkSession spark) {
        return spark.range(0, 200_000).withColumn("provider", col("id").mod(50));
    }

    static void writeTotals(Dataset<Row> totals, String out) {
        totals.write().mode("overwrite").parquet(out);
    }
}
