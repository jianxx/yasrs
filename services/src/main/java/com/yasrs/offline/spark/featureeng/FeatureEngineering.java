package com.yasrs.offline.spark.featureeng;

import java.util.logging.Logger;

import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

public class FeatureEngineering {

    private static final Logger LOG = Logger.getLogger(FeatureEngineering.class.getCanonicalName());

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

}