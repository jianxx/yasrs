package com.yasrs.offline.spark.featureeng;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.format_number;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.stddev;

import java.util.logging.Logger;

import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class FeatureEngineering {

    private static final Logger LOG = Logger.getLogger(FeatureEngineering.class.getCanonicalName());

    private static final int NUMBER_PRECISION = 2;

    private SQLContext sqlContext = null;

    public FeatureEngineering(SparkSession spark) {
        this.sqlContext = spark.sqlContext();
    }

    /**
     * One-hot encoding function
     * 
     * @param samples movie samples dataframe
     */
    public Dataset<Row> oneHotEncoder(Dataset<Row> samples) {
        Dataset<Row> samplesWithIdNumber = samples.withColumn("movieIdNumber",
                samples.col("movieId").cast(DataTypes.IntegerType));
        OneHotEncoderEstimator oneHotEncoder = new OneHotEncoderEstimator()
                .setInputCols(new String[] { "movieIdNumber" }).setOutputCols(new String[] { "movieIdVector" })
                .setDropLast(false);

        return oneHotEncoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber);
    }

    /**
     * Multi-hot encoding function
     * 
     * @param samples movie samples dataframe
     */
    public void multiHotEncoder(Dataset<Row> samples) {
    }

    public Dataset<Row> generateMovieFeatures(Dataset<Row> movies, Dataset<Row> ratings) {
        registerExtractReleaseYearUdf();
        registerExtractTitleUdf();
        Dataset<Row> movieWithRatingAndTitles = ratings.join(movies, "movieId")
                .withColumn("releaseYear", callUDF("releaseYear", col("title")))
                .withColumn("title", callUDF("extractTitleUdf", col("title"))).drop("title")
                .withColumn("movieGenre1", split(col("genres"), "\\|"))
                .withColumn("movieGenre2", split(col("genres"), "\\|"))
                .withColumn("movieGenre3", split(col("genres"), "\\|")).groupBy(col("movieId"))
                .agg(count(lit(1)), format_number(avg(col("rating")), NUMBER_PRECISION).as("movieAvgRating"),
                        stddev(col("rating")).as("movieRatingStddev"))
                .na().fill(0)
                .withColumn("movieRatingStddev", format_number(col("movieRatingStddev"), NUMBER_PRECISION));

        Dataset<Row> movieRatingFeatures = null;
        return movieRatingFeatures;
    }

    public void splitAndSaveTrainingData(Dataset<Row> samples, String savePath) {
        // generate a smaller sample set for demo
        Dataset<Row> smallSamples = samples.sample(0.1);
        // split training and test set by 8:2
        Dataset<Row>[] splitResults = smallSamples.randomSplit(new double[] { 0.8, 0.2 });

        splitResults[0].repartition(1).write().option("header", "true").mode(SaveMode.Overwrite)
                .csv(savePath + "/trainingSamples");
    }

    private void registerExtractReleaseYearUdf() {
        this.sqlContext.udf().register("extractReleaseYear", (UDF1<String, Integer>) (title) -> {
            if (null == title || title.trim().length() < 6) {
                return 1990;
            } else {
                return Integer.parseInt(title.trim().substring(title.length() - 5, title.length() - 1));
            }
        }, DataTypes.IntegerType);
    }

    private void registerExtractTitleUdf() {
        this.sqlContext.udf().register("extractTitle", (UDF1<String, String>) (title) -> {
            return title.trim().substring(0, title.trim().length() - 6).trim();
        }, DataTypes.StringType);
    }
}