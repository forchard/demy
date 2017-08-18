package demy.geo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.Column

object Execute {
  def main(args: Array[String]) {
    val spark = SparkSession
     .builder()
     .appName("Address Localizator")
     .getOrCreate()

     val ban = spark.read.parquet("hdfs:///data/geo/BAN.parquet")
                     .select(functions.expr("concat(coalesce(numero, ''), coalesce(rep, ''))").alias("number")
                            , functions.expr("coalesce(nom_ld, nom_voie, '')").alias("street")
                            ,functions.col("code_post").alias("postcode")
                            ,functions.col("code_insee").alias("inseeCode")
                            , functions.expr("coalesce(libelle_acheminement, nom_commune)").alias("locality_name")
                            , functions.col("lon")
                            , functions.col("lat")
                            )
                     .filter(r => r.getAs[String]("postcode")!="PLURI")

     println("Base Adress defined")
     val localizator = AddressLocalizator("postcode", "inseeCode" ,"locality_name", "street", "number", "lon", "lat", "hdfs:///data/geo")
     val localizatorData = localizator.getIndexes(spark, ban, true)
     println("Address index created")
    
     //val addressesToMatch = spark.read.parquet("hdfs:///data/neoscope/apremas/prat.parquet")
     val addressesToMatch = spark.read.parquet("hdfs:///data/neoscope/apremas/assure.parquet")
     val irisBase = spark.read.parquet("hdfs:///data/geo/ContourIris.parquet")

     localizator.geoLocalizeAddress(spark, localizatorData, addressesToMatch, "adresse", "id_assure", "assure", irisBase, "GeometryBinary", "INSEE_COM", "CODE_IRIS" , "hdfs:///data/neoscope/apremas/assure.adress.parquet") 
     //localizator.geoLocalizeAddress(spark, localizatorData, addressesToMatch, "adresse", "id_prat", "prat", irisBase, "GeometryBinary", "INSEE_COM", "CODE_IRIS" , "hdfs:///data/neoscope/apremas/prat.adress.parquet") 
     println("Address gelocalized")

  }
}
