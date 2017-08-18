package demy.geo;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import scala.collection.immutable.HashMap
import org.apache.spark.sql.functions
import org.apache.spark.sql.Column

case class AddressLocalizatorData(
        localities:Dataset[Locality]
        , streets: Dataset[Street]
        , postcodeIndex: HashMap[Int,(String, String)]
        , localityIndex: HashMap[String,List[(String, Int, String)]]
        )

case class AddressLocalizator(postcodeColumn:String, inseeCodeColumn:String, localityNameColumn:String, streetColumn:String, numberColumn:String, longitudeColumn:String, latitudeColumn:String, workingDirectory:String) {
    
    def geoLocalizeAddress(spark:SparkSession, index:AddressLocalizatorData, search:org.apache.spark.sql.DataFrame, addressColumn:String, idColumn:String, tableName:String, irisBase:DataFrame, irisGeoColumn:String, inseeCodeColumn:String, irisCodeColumn:String , destination:String,overwriteDestination:Boolean = true) = {
        import spark.implicits._
        import org.apache.spark.sql.functions

        search.select(addressColumn, idColumn)
            .map(r => ParsedAddress(r.getAs[Int](idColumn),tableName,r.getAs[String](addressColumn)))
            .map(a => a.setPostcodeFromPostcodeIndex(index.postcodeIndex))
            .map(a => a.postcode match {case Some(cp) => a case _ => a.setPostcodeFromLocalityIndex(a.splitAdressNumber(Some(a.inputSimplified)).addressNoNumber, index.localityIndex)}) 
            .joinWith(index.streets, functions.expr("_1.postcode=_2.postcode"))
            .flatMap(p => p._1.locateInStreet(p._2) match {case Some(address) => Array(address) case _ => Array[ParsedAddress]()})
            .groupByKey(p => (p.id, p.table))
            .reduceGroups((a1, a2) => a1.getBestStreetMatch(a2))
            .map(p => p._2)
            .joinWith(irisBase, functions.expr(s"_1.inseeCode=_2.${inseeCodeColumn}"))
            .map(p => p._1.setIrisCode(p._2.getAs[Array[Byte]](irisGeoColumn), p._2.getAs[String](irisCodeColumn) ))
            .groupByKey(p => (p.id, p.table))
            .reduceGroups((a1, a2) => a1.getBestIrisCode(a2))
            .map(p => p._2)
            .write.mode(overwriteDestination match {case true => "Overwrite" case _ => "Append"}).parquet(destination)
    }
 
    def getIndexes(spark:SparkSession, addressBase:DataFrame, reuseIndex:Boolean=false) = {
        import spark.implicits._
        if(!reuseIndex) {
           val base = addressBase
                .withColumn("_locality", functions.expr(s"concat(${postcodeColumn}, ' ', ${localityNameColumn})").alias("locality"))
                .withColumn("_number", foldString(addressBase(numberColumn)))
                .withColumn("_lon", foldDouble(addressBase(longitudeColumn)))
                .withColumn("_lat", foldDouble(addressBase(latitudeColumn)))
                .withColumn("_meanLon", addressBase(longitudeColumn))
                .withColumn("_meanLat", addressBase(latitudeColumn)) 
                .withColumn("_count", functions.lit(1))
                .filter(r => r.getAs[String](postcodeColumn) != null && r.getAs[String](postcodeColumn).length > 0)
                
           base
               .map(r => Locality(r.getAs[String]("_locality"),r.getAs[String](postcodeColumn).toInt,r.getAs[String](inseeCodeColumn),r.getAs[String](localityNameColumn), r.getAs[Double]("_meanLon"), r.getAs[Double]("_meanLat"), r.getAs[Int]("_count")))
               .filter(l => l.localityName != null && l.localityName.length > 0)
               .groupByKey(l => l.locality)
               .reduceGroups((l1, l2) => l1 + l2)
               .map(p => p._2)
               .write.mode("Overwrite")
               .parquet(s"${workingDirectory}/localities.parquet")
                       

           base
               .map(r => Street(r.getAs[String](streetColumn),r.getAs[String]("_locality"),r.getAs[String](postcodeColumn).toInt, r.getAs[Seq[String]]("_number"), r.getAs[Seq[Double]]("_lon"), r.getAs[Seq[Double]]("_lat"), r.getAs[Double]("_meanLon"), r.getAs[Double]("_meanLat"), r.getAs[Int]("_count"))) 
               .groupByKey(s => (s.street, s.locality))
               .reduceGroups((s1, s2) => s1 + s2)
               .map(s => s._2)
               .filter(s => s.street != null && s.locality != null)
               .write.mode("Overwrite")
//              .partitionBy(postcodeColumn)
               .parquet(s"${workingDirectory}/streets.parquet")
        }
        val localities = spark.read.parquet(s"${workingDirectory}/localities.parquet").as[Locality]
        val streets = spark.read.parquet(s"${workingDirectory}/streets.parquet").as[Street] 

        val postcodeIndex = scala.collection.immutable.HashMap(
           localities
            .map(s => (s.postcode, (s.locality, s.inseeCode)))
            .distinct
            .collect : _*
        )

        val localityIndex = scala.collection.immutable.HashMap (
           localities
            .flatMap(s => TextTools.splitText(s.localityName).map(w => (s.localityName, w, s.postcode, s.inseeCode)))
            .map(p => (p._1, p._2, p._3, p._4, 1))
            .withColumn("wCount", functions.expr("count(*) over (partition by _2)"))
            .withColumn("rn_loc", functions.expr("row_number() over (partition by _1 order by wCount asc)"))
            .where("rn_loc =1")
            .select("_1", "_2", "_3", "_4").map(r => (r.getAs[String]("_2"), (r.getAs[String]("_1"), r.getAs[Int]("_3"), r.getAs[String]("_4")) :: Nil))
            .groupByKey(p => p._1)
            .reduceGroups((p1, p2) => (p1._1, p1._2 ++ p2._2))
            .map(r => r._2)
            .collect : _*
        )
        AddressLocalizatorData(localities, streets, postcodeIndex, localityIndex)
    }
    
    val foldString =org.apache.spark.sql.functions.udf((number:String) => {
    Seq(number)
    })
    val foldDouble = org.apache.spark.sql.functions.udf((number:Double) => {
        Seq(number)
    })

}

