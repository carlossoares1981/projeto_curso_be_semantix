import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._
object ElasticWrite {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().appName("Write-Elastic").master("local[*]").getOrCreate()
    //val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val df = sc.read.json("/user/carlos/painel_regioes_c")


    df.write
      .format("org.elasticsearch.spark.sql")
     // .option("es.port","9200")
      .option("es.nodes", "http://localhost:9200/")
      .mode("Overwrite")
      .save("index/covid_spark")

    df.show()
    sc.close()
  }
}
