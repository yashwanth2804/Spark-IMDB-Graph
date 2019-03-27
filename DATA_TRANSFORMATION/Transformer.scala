import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._	
import org.apache.spark.sql.functions._


object Transformer {


  def main(args: Array[String]) {
    var CastsFile = sc.textFile("/home/hasura/Desktop/IMDB_CODING/DATA/MoviesCast.csv");
	val df_ = CastsFile.map(f => {
		val splitArr = f.split(",") // reading line and splitting with ','
		val _Tuple_pair = for( a <- splitArr ; b <- splitArr ;if(a != b) )  yield(a,b)
		//(a,b),(a,c),(a,d)......(g,a)...
		_Tuple_pair //returning tuple pairs of possible combination 
	})
	.flatMap(f => f);

	val relationsDF =	spark.createDataFrame(df_).toDF("src","dst");

	relationsDF.coalesce(1).write.format("com.databricks.spark.csv").save("/home/hasura/Desktop/IMDB_CODING/DATA/Relations.csv")
	  
}
  }
// scalastyle:on println