// A Spark job that reads and writes HBase on the same EMR cluster, built to
// give sparkplain real HBase log lines. Each mode is its own application:
//
//   connector <rows>  write sp_orders and sp_events with the hbase-spark
//                     connector, read sp_orders back, aggregate, write sp_totals
//   rdd               read sp_orders with TableInputFormat, write sp_totals
//                     with TableOutputFormat
//   missing           write to a table that does not exist (fails)
//   badquorum         point the client at a ZooKeeper port nothing listens on (fails)
//   slow              scan sp_events slowly with TableInputFormat so scanner
//                     leases expire
//   hot <rows>        write every row into sp_hot's one region (see hot.hbase)
//
// Every job sets a description so the report can name it.
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

public class HBaseJob {
  static final String FORMAT = "org.apache.hadoop.hbase.spark";
  static final String ORDERS = "id STRING :key, region STRING d:region, day STRING d:day, amount DOUBLE d:amount, qty INT d:qty";
  static final String TOTALS = "key STRING :key, orders LONG d:orders, amount DOUBLE d:amount";
  static final String EVENTS = "id STRING :key, kind STRING d:kind, payload STRING d:payload";
  static final byte[] D = Bytes.toBytes("d");

  public static void main(String[] args) throws Exception {
    String mode = args[0];
    SparkSession spark = SparkSession.builder().appName("sparkplain_hbase_" + mode).getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    Configuration conf = HBaseConfiguration.create();  // hbase-site.xml, shipped with --files
    try {
      switch (mode) {
        case "connector": connector(spark, jsc, conf, Long.parseLong(args[1])); break;
        case "rdd": rdd(jsc, conf); break;
        case "missing": missing(spark, jsc, conf); break;
        case "badquorum": badquorum(spark, jsc, conf); break;
        case "slow": slow(jsc, conf); break;
        case "hot": hot(spark, jsc, conf, Long.parseLong(args[1])); break;
        default: throw new IllegalArgumentException("unknown mode " + mode);
      }
    } finally {
      spark.stop();
    }
  }

  static Dataset<Row> read(SparkSession spark, String table, String mapping) {
    return spark.read().format(FORMAT)
        .option("hbase.table", table).option("hbase.columns.mapping", mapping)
        .option("hbase.spark.pushdown.columnfilter", "false").load();
  }

  static void write(Dataset<Row> df, String table, String mapping) {
    df.write().format(FORMAT)
        .option("hbase.table", table).option("hbase.columns.mapping", mapping).save();
  }

  static void connector(SparkSession spark, JavaSparkContext jsc, Configuration conf, long rows) {
    new HBaseContext(jsc.sc(), conf, null);
    jsc.setJobDescription("write sp_orders through the hbase-spark connector");
    Dataset<Row> orders = spark.range(0, rows, 1, 10)
        .withColumn("id", concat(expr("cast(id % 10 as string)"), lit("-"), lpad(col("id").cast("string"), 10, "0")))
        .withColumn("region", element_at(array(lit("north"), lit("north"), lit("north"), lit("south"), lit("east"), lit("west")),
            expr("cast(hash(id) % 6 + 7 as int) % 6 + 1")))
        .withColumn("day", date_format(expr("date_add(date'2026-09-01', cast(abs(hash(id)) % 28 as int))"), "yyyy-MM-dd"))
        .withColumn("amount", round(rand(7).multiply(100), 2))
        .withColumn("qty", expr("cast(abs(hash(id, 1)) % 7 + 1 as int)"));
    write(orders, "sp_orders", ORDERS);

    jsc.setJobDescription("write sp_events through the hbase-spark connector");
    Dataset<Row> events = spark.range(0, 20000, 1, 4)
        .withColumn("id", concat(expr("cast(id % 9 as string)"), lit("-"), lpad(col("id").cast("string"), 8, "0")))
        .withColumn("kind", expr("element_at(array('view','click','buy'), cast(id % 3 as int) + 1)"))
        .withColumn("payload", sha2(col("id"), 256));
    write(events, "sp_events", EVENTS);

    jsc.setJobDescription("read sp_orders and total by region and day");
    Dataset<Row> managers = spark.createDataFrame(java.util.Arrays.asList(
        org.apache.spark.sql.RowFactory.create("north", "Asha"), org.apache.spark.sql.RowFactory.create("south", "Ben"),
        org.apache.spark.sql.RowFactory.create("east", "Chen"), org.apache.spark.sql.RowFactory.create("west", "Dana")),
        new org.apache.spark.sql.types.StructType().add("region", "string").add("manager", "string"));
    Dataset<Row> totals = read(spark, "sp_orders", ORDERS)
        .filter(col("amount").gt(5))
        .join(managers, "region")
        .groupBy("region", "manager", "day")
        .agg(count(lit(1)).as("orders"), round(sum("amount"), 2).as("amount"))
        .withColumn("key", concat_ws("|", lit("df"), col("region"), col("day")))
        .select("key", "orders", "amount");
    write(totals, "sp_totals", TOTALS);

    jsc.setJobDescription("count sp_totals rows written by the connector");
    System.out.println("sp_totals rows: " + read(spark, "sp_totals", TOTALS).count());
  }

  static void rdd(JavaSparkContext jsc, Configuration conf) throws Exception {
    Configuration in = new Configuration(conf);
    in.set(TableInputFormat.INPUT_TABLE, "sp_orders");
    in.set(TableInputFormat.SCAN_COLUMNS, "d:region d:qty");
    in.set(TableInputFormat.SCAN_CACHEDROWS, "1000");
    JavaPairRDD<ImmutableBytesWritable, Result> rows =
        jsc.newAPIHadoopRDD(in, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

    jsc.setJobDescription("scan sp_orders with TableInputFormat and total qty by region");
    JavaPairRDD<String, Long> qty = rows
        .mapToPair(t -> new Tuple2<>(Bytes.toString(t._2.getValue(D, Bytes.toBytes("region"))),
            (long) Bytes.toInt(t._2.getValue(D, Bytes.toBytes("qty")))))
        .reduceByKey(Long::sum, 4);

    Job job = Job.getInstance(new Configuration(conf));
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "sp_totals");
    job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp/sparkplain-hbase-rdd");
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    jsc.setJobDescription("write qty totals to sp_totals with TableOutputFormat");
    qty.mapToPair(t -> {
      Put p = new Put(Bytes.toBytes("rdd|" + t._1));
      p.addColumn(D, Bytes.toBytes("qty"), Bytes.toBytes(t._2));
      return new Tuple2<>(new ImmutableBytesWritable(), p);
    }).saveAsNewAPIHadoopDataset(job.getConfiguration());
  }

  static void missing(SparkSession spark, JavaSparkContext jsc, Configuration conf) {
    new HBaseContext(jsc.sc(), conf, null);
    jsc.setJobDescription("write to sp_missing, a table that does not exist");
    Dataset<Row> df = spark.range(0, 1000, 1, 4).select(col("id").cast("string").as("key"),
        col("id").as("orders"), col("id").cast("double").as("amount"));
    write(df, "sp_missing", TOTALS);
  }

  static void badquorum(SparkSession spark, JavaSparkContext jsc, Configuration conf) throws Exception {
    Configuration bad = new Configuration(conf);
    bad.set("hbase.zookeeper.property.clientPort", "2182");  // nothing listens here
    bad.set("zookeeper.recovery.retry", "2");
    bad.set("zookeeper.recovery.retry.intervalmill", "500");
    bad.set("hbase.client.retries.number", "2");
    new HBaseContext(jsc.sc(), bad, null);
    jsc.setJobDescription("read sp_orders through a ZooKeeper port nothing listens on");
    ExecutorService ex = Executors.newSingleThreadExecutor();
    Future<Long> n = ex.submit(() -> read(spark, "sp_orders", ORDERS).count());
    try {
      System.out.println("rows: " + n.get(6, TimeUnit.MINUTES));
    } finally {
      ex.shutdownNow();
    }
  }

  static void slow(JavaSparkContext jsc, Configuration conf) {
    Configuration in = new Configuration(conf);
    in.set(TableInputFormat.INPUT_TABLE, "sp_events");
    in.set(TableInputFormat.SCAN_CACHEDROWS, "2000");       // 2000 rows at 40 ms each outlives
    in.set("hbase.client.scanner.timeout.period", "60000");  // the 60 s scanner lease
    JavaPairRDD<ImmutableBytesWritable, Result> rows =
        jsc.newAPIHadoopRDD(in, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
    jsc.setJobDescription("scan sp_events slowly with TableInputFormat");
    long n = rows.map(t -> {
      Thread.sleep(40);
      return Bytes.toString(t._2.getValue(D, Bytes.toBytes("kind")));
    }).filter(k -> "buy".equals(k)).count();
    System.out.println("buy events: " + n);
  }

  static void hot(SparkSession spark, JavaSparkContext jsc, Configuration conf, long rows) {
    new HBaseContext(jsc.sc(), conf, null);
    jsc.setJobDescription("write sp_hot: every row key shares one prefix, so one region takes all writes");
    Dataset<Row> df = spark.range(0, rows, 1, 16)
        .select(concat(lit("hot-"), lpad(col("id").cast("string"), 10, "0")).as("id"),
            lit("buy").as("kind"), repeat(sha2(col("id").cast("string"), 256), 8).as("payload"));
    write(df, "sp_hot", EVENTS);
  }
}
