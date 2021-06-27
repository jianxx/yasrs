package com.yasrs.offline.spark.featureeng;

import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FeatureEngineering {

    private static final Logger LOG = Logger.getLogger(FeatureEngineering.class.getCanonicalName());

    /**
     * One-hot encoding example function
     * 
     * @param samples movie samples dataframe
     */
    public void oneHotEncoder(Dataset<Row> samples) {
    }

}