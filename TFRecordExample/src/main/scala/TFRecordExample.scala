import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



object TFRecordExample {
  def main(args: Array[String]) {
    println("Hello, World!")
    val path = "test-output.tfrecord"
    val testRows: Array[Row] = Array(
      new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, List(1.0, 2.0), "r1")),
      new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, List(2.0, 2.0), "r2")))
    val schema = StructType(List(StructField("id", IntegerType),
      StructField("IntegerCol", IntegerType),
      StructField("LongCol", LongType),
      StructField("FloatCol", FloatType),
      StructField("DoubleCol", DoubleType),
      StructField("VectorCol", ArrayType(DoubleType, true)),
      StructField("StringCol", StringType)))

    // initialise spark context
    val conf = new SparkConf().setMaster("local[2]").setAppName(TFRecordExample.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    //  val spark = SparkSession.builder.appName("Simple Application").getOrCreate
    val rdd = spark.sparkContext.parallelize(testRows)

    //Save DataFrame as TFRecords
    val df: DataFrame = spark.createDataFrame(rdd, schema)
    df.show()

    df.write.format("tfrecords").option("recordType", "Example").save(path)
//
//    //Read TFRecords into DataFrame.
//    //The DataFrame schema is inferred from the TFRecords if no custom schema is provided.
//    val importedDf1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(path)
//    importedDf1.show()
//
//    //Read TFRecords into DataFrame using custom schema
//    val importedDf2: DataFrame = spark.read.format("tfrecords").schema(schema).load(path)
//    importedDf2.show()

  }
}