import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

public class FilenamesJob {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Filenames")
                .config("spark.speculation","false")
                .getOrCreate();

        spark.udf().register("getOnlyFileName",
                (String fullPath) -> {
                    int lastIndex = fullPath.lastIndexOf("/");
                    return fullPath.substring(lastIndex + 1, fullPath.length() - 1);
                }, DataTypes.StringType);

        Dataset<Row> initialDF = spark.read().text("s3a://path_to_the_common_root_folder/*/*/*")
                .withColumn("filename", functions.callUDF("getOnlyFileName", functions.input_file_name()));

        List<String> foo = initialDF.select("filename").distinct().as(Encoders.STRING()).collectAsList();

        System.out.println(Arrays.toString(foo.toArray()));

    }
}
