package ma.salma.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Two Apache Spark RDD lab exercises: sales aggregation and access-log analysis. */
public final class Main {
    private static final Pattern ACCESS_LOG = Pattern.compile(
            "^(\\S+) \\S+ \\S+ \\[([^\\]]+)] \\\"(\\S+) (\\S+)(?: \\S+)?\\\" (\\d{3}) (\\S+)(?: \\\"([^\\\"]*)\\\" \\\"([^\\\"]*)\\\")?.*$"
    );

    private Main() {}

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                .setAppName("RDD Programming Lab")
                .set("spark.ui.enabled", "false");
        if (!conf.contains("spark.master")) conf.setMaster("local[2]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.setLogLevel("ERROR");
            String exercise = args.length == 0 ? "all" : args[0].toLowerCase(Locale.ROOT);
            switch (exercise) {
                case "sales" -> runSales(sc, args.length > 1 ? args[1] : "data/ventes.txt");
                case "logs" -> runLogs(sc, args.length > 1 ? args[1] : "data/access.log");
                case "all" -> {
                    runSales(sc, args.length > 1 ? args[1] : "data/ventes.txt");
                    runLogs(sc, args.length > 2 ? args[2] : "data/access.log");
                }
                default -> throw new IllegalArgumentException(
                        "Usage: java -jar target/rdd-programming-with-apache-spark-1.0.0.jar [all|sales|logs] [salesFile] [logFile]");
            }
        }
    }

    private static void runSales(JavaSparkContext sc, String path) {
        JavaRDD<String> lines = sc.textFile(path);
        JavaRDD<Sale> sales = lines.map(Main::parseSale).filter(sale -> sale != null);
        JavaPairRDD<String, BigDecimal> cityTotals = sales
                .mapToPair(sale -> new Tuple2<>(sale.city(), sale.amount()))
                .reduceByKey(BigDecimal::add);
        JavaPairRDD<String, BigDecimal> cityYearTotals = sales
                .mapToPair(sale -> new Tuple2<>(sale.city() + " | " + sale.date().substring(0, 4), sale.amount()))
                .reduceByKey(BigDecimal::add);

        System.out.println("=== EXERCISE 1: SALES ANALYSIS ===");
        System.out.printf("Input: %s | Valid transactions: %d%n", path, sales.count());
        System.out.println("Total sales by city:");
        cityTotals.sortByKey().collect().forEach(row -> printAmount(row._1, row._2));
        System.out.println("Total sales by city and year:");
        cityYearTotals.sortByKey().collect().forEach(row -> printAmount(row._1, row._2));
    }

    private static Sale parseSale(String line) {
        if (line == null || line.isBlank() || line.stripLeading().startsWith("#")) return null;
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 4 || !fields[0].matches("\\d{4}-\\d{2}-\\d{2}")) return null;
        try {
            return new Sale(fields[0], fields[1], fields[2], new BigDecimal(fields[3]));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static void printAmount(String key, BigDecimal amount) {
        System.out.printf(Locale.ROOT, "  %-24s %s%n", key, amount.setScale(2, RoundingMode.HALF_UP).toPlainString());
    }

    private static void runLogs(JavaSparkContext sc, String path) {
        JavaRDD<LogEntry> entries = sc.textFile(path).map(Main::parseLog).filter(entry -> entry != null).cache();
        long total = entries.count();
        long errors = entries.filter(entry -> entry.status() >= 400).count();
        double errorRate = total == 0 ? 0.0 : 100.0 * errors / total;

        JavaPairRDD<String, Integer> ipCounts = entries
                .mapToPair(entry -> new Tuple2<>(entry.ip(), 1)).reduceByKey(Integer::sum);
        JavaPairRDD<String, Integer> resourceCounts = entries
                .mapToPair(entry -> new Tuple2<>(entry.resource(), 1)).reduceByKey(Integer::sum);
        JavaPairRDD<Integer, Integer> statusCounts = entries
                .mapToPair(entry -> new Tuple2<>(entry.status(), 1)).reduceByKey(Integer::sum);
        Comparator<Tuple2<String, Integer>> countThenName = new CountThenName();

        System.out.println("=== EXERCISE 2: APACHE ACCESS LOG ANALYSIS ===");
        System.out.printf("Input: %s | Parsed requests: %d%n", path, total);
        System.out.println("Basic statistics:");
        System.out.printf(Locale.ROOT, "  Total HTTP requests: %d%n  HTTP errors (status >= 400): %d%n  Error rate: %.2f%%%n",
                total, errors, errorRate);
        System.out.println("Top 5 client IP addresses (count):");
        ipCounts.takeOrdered(5, countThenName).forEach(row -> System.out.printf("  %-15s %d%n", row._1, row._2));
        System.out.println("Top 5 requested resources (count):");
        resourceCounts.takeOrdered(5, countThenName).forEach(row -> System.out.printf("  %-24s %d%n", row._1, row._2));
        System.out.println("HTTP status code distribution:");
        statusCounts.sortByKey().collect().forEach(row -> System.out.printf("  %d: %d%n", row._1, row._2));
        entries.unpersist();
    }

    private static LogEntry parseLog(String line) {
        Matcher matcher = ACCESS_LOG.matcher(line);
        if (!matcher.matches()) return null;
        try {
            return new LogEntry(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4),
                    Integer.parseInt(matcher.group(5)), matcher.group(6), matcher.group(7), matcher.group(8));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private record Sale(String date, String city, String product, BigDecimal amount) implements Serializable {}

    private record LogEntry(String ip, String timestamp, String method, String resource, int status,
                            String responseSize, String referrer, String userAgent) implements Serializable {}

    private static final class CountThenName implements java.util.Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> left, Tuple2<String, Integer> right) {
            int countOrder = Integer.compare(right._2, left._2);
            return countOrder != 0 ? countOrder : left._1.compareTo(right._1);
        }
    }
}
