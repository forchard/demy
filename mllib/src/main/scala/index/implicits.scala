package demy.mllib.index;

import scala.collection.parallel.ForkJoinTaskSupport
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import demy.storage.Storage
import demy.util.log

object implicits {
  implicit class DatasetUtil(val left: Dataset[_]) {
    def luceneLookup(
      right:Dataset[_]
      , query:Column
      , text:Column
      , maxLevDistance:Int=0
      , indexPath:String
      , reuseExistingIndex:Boolean=false
      , leftSelect:Array[Column]=Array(col("*"))
      , rightSelect:Array[Column]=Array(col("*"))
      , popularity:Option[Column]=None
      , indexPartitions:Int = 1
      , maxRowsInMemory:Int=100
      , indexScanParallelism:Int = 2
      , tokenizeText:Boolean=true
      , minScore:Double=0.0
      , boostAcronyms:Boolean=false
      , termWeightsColumnName:Option[String]=None
      , strategy:String = "demy.mllib.index.StandardStrategy"
      , strategyParams: Map[String, String]=Map.empty[String,String]
    ) = {
      val rightApplied = right.select((
        Array(text.as("_text_")) 
        ++ (popularity match {case Some(c) => Array(c.as("_pop_")) case _ => Array[Column]()}) 
        ++ rightSelect
        ) :_*)

      //Building index if does not exists
      val sparkStorage = Storage.getSparkStorage

      val indexNode = sparkStorage.getNode(path = indexPath)
      val exists = indexNode.exists
      if(!reuseExistingIndex && exists)
        indexNode.delete(recurse = true)

      //writing the index with the right part dataset
      if(!exists || !reuseExistingIndex) {
        val rdd = rightApplied.rdd
        val partedRdd = 
          if(rdd.getNumPartitions<indexPartitions) rdd.repartition(indexPartitions)
          else rdd.coalesce(indexPartitions)
          
        val popPosition = popularity match {case Some(c) => Some(1) case _ => None }
        val popPositionSet = popularity match {case Some(c) => Set(1) case _ =>Set[Int]()}
        partedRdd.mapPartitions{iter => 
          //Index creation
          var index = SparkLuceneWriter(indexDestination=indexPath,  boostAcronyms=boostAcronyms)
          var createIndex = true
          var indexInfo:SparkLuceneWriterInfo = null
          var indexHDFSDest:String = null
          iter.foreach{row => 
            if(createIndex) {
                indexInfo = index.create
                createIndex = false
            }
            indexInfo.indexRow(
              row = row
              , textFieldPosition = 0
              , popularityPosition = popPosition
              , notStoredPositions = Set(0) ++ popPositionSet, tokenisation = tokenizeText 
            )
          }
          if(indexInfo != null) {
            indexInfo.push(deleteSource = true)
            Array(indexHDFSDest).iterator
          } else {
            Array[String]().iterator
          }
        }
        .collect
      }
      //Reading the index
      val indexFiles =  
        indexNode.list.toArray
        .map{node => 
            SparkLuceneReader(indexPartition=node.path
              , reuseSnapShot = true
              , useSparkFiles= false
              , usePopularity=popularity match {
                  case Some(c) => true 
                  case None => false
                }
              , indexStrategy = strategy
              , strategyParams = strategyParams)
        }
      //Preparing the results
      val leftApplied = left.select((Array(query.as("_text_")) ++ leftSelect) :_*)


      val isArrayJoin = 
        leftApplied.schema.fields(0).dataType
          match {
            case ArrayType(x:StringType, _) => true
            case x:StringType => false
            case _ => throw new Exception(s"Query must be a String or an array of strings")
          }

      val isArrayJoinWeights:Option[Boolean] =
        if (!termWeightsColumnName.isEmpty) {
          leftApplied.schema.fields(leftApplied.schema.fieldIndex(termWeightsColumnName.get)).dataType
            match {
              case ArrayType(x:DoubleType, _) => Some(true)
              case ArrayType(ArrayType(x:DoubleType, _), _) => Some(false)
              case _ => throw new Exception(s"TermWeights must be an array of Doubles")
            }
        }
        else None


      val leftOutFields = leftApplied.schema.fields.slice(1, leftApplied.schema.fields.size)
      val leftOutSchema = new StructType(leftOutFields)
      val rightRequestFields = rightApplied.schema.fields
        .slice(popularity match {case Some(c) => 2 case _ => 1}, rightApplied.schema.fields.size)
        .map(f => new StructField(name = f.name, dataType = f.dataType, nullable = true, metadata = f.metadata))

      val rightOutFields = (rightApplied.schema.fields
        .slice(popularity match {case Some(c) => 2 case _ => 1}, rightApplied.schema.fields.size)
        .map(f => new StructField(name = f.name, dataType = f.dataType, nullable = true, metadata = f.metadata)) 
         ++ Array(
           new StructField("_score_", FloatType)
           , new StructField("_tags_", ArrayType(StringType))
           ,new StructField("_startIndex_", IntegerType), new StructField("_endIndex_", IntegerType)
           ))

      val rightOutSchema = 
        if(!isArrayJoin) new StructType(rightOutFields)
        else new StructType(
          fields = Array(StructField(
            name = "array", 
            dataType = ArrayType(elementType=new StructType(rightOutFields) , containsNull = true)
          )))

      val resultRdd0 = leftApplied.rdd
        .mapPartitions(iter => {
          var indexLocation = ""
          var rInfo:IndexStrategy = null
          val noRows:Option[Array[Option[Row]]]=None
          iter.flatMap(r => {
            var collection = (scala.collection.mutable.ArrayBuffer((r, noRows)) 
               ++ (for(i <- 1 to maxRowsInMemory if iter.hasNext) yield (iter.next(), noRows)))
            var rowsChunk:scala.collection.GenSeq[(org.apache.spark.sql.Row, Option[Array[Option[org.apache.spark.sql.Row]]])] = (
              if(indexScanParallelism > 1 ) {
                val parCol = collection.par
                parCol.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(indexScanParallelism))
                parCol
              }
              else collection
              )
             indexFiles.foreach(iReader => {
               if(indexLocation != iReader.indexPartition) {
                 if(rInfo!=null) rInfo.close(false)
                 rInfo = iReader.open

                 indexLocation = iReader.indexPartition
               }
               rowsChunk = rowsChunk.map(elem => elem match{ case (leftRow, righResults) =>
                 (leftRow, {
                   val queries = if(isArrayJoin) leftRow.getSeq[String](0).toArray else Array(leftRow.getAs[String](0))
                   val termWeightsArray:Seq[Option[Seq[Double]]] = {
                      if(isArrayJoinWeights.isEmpty) 
                        queries.map(_ => None)
                      else if(isArrayJoinWeights.get) 
                        Seq(Some(leftRow.getAs[Seq[Double]](leftRow.fieldIndex(termWeightsColumnName.get))))
                      else 
                        leftRow.getAs[Seq[Seq[Double]]](leftRow.fieldIndex(termWeightsColumnName.get)).map(value => Some(value))
//                    else if(isArrayJoinWeights.get) 
//                      leftRow.getSeq[Seq[Double]](leftRow.fieldIndex(termWeightsColumnName.get)).map(value => Some(value.toArray))
//                    else 
//                      leftRow.getAs[Seq[Seq[Double]]](leftRow.fieldIndex(termWeightsColumnName.get)).map(value => Some(value.toArray))
                   }
                   val resultsArray = righResults match {case Some(array) => 
                     array case None => queries.map(q => None)
                   } //If first index then an array to contain the results
                   Some(
                     queries.zipWithIndex.map(p => p match {case (query, i) => {
                       // call search on SearchStrategy
                       val res:Array[GenericRowWithSchema] = 
                         rInfo.search(query=query, maxHits=1, filter = Row.empty, outFields=rightRequestFields,
                           maxLevDistance=maxLevDistance, minScore=minScore, boostAcronyms=boostAcronyms,
                           usePopularity = iReader.usePopularity, termWeights=termWeightsArray(i))

                       if(res.size == 0)
                         resultsArray(i)
                       else
                         resultsArray(i) match {case Some(row) => 
                           if(row.getAs[Float]("_score_")>res(0).getAs[Float]("_score_")) 
                             resultsArray(i) 
                           else Some(res(0)) case None => Some(res(0))
                         }
                       }})
                    )
                 })
               })
             })
             if(!iter.hasNext) rInfo.close(false)
             rowsChunk
          })
        })
        val resultRdd = resultRdd0
        .map{case (left, right) => 
          val leftRow = new GenericRowWithSchema(left.toSeq.slice(1, leftOutFields.size+1).toArray, leftOutSchema)
          val rightRow = 
            if(!isArrayJoin) 
              right.get(0) match { 
                case Some(row) => row
                case None => new GenericRowWithSchema(rightOutFields.map(f => null), rightOutSchema)
              }
            else
              new GenericRowWithSchema(
                Array(right.get.map(result => result match {
                  case Some(row) => row
                  case None => new GenericRowWithSchema(rightOutFields.map(f => null), rightOutSchema)
                }))
                , rightOutSchema)
          Row.merge(leftRow, rightRow)
        }
      right.sparkSession.createDataFrame(resultRdd, new StructType(leftOutFields ++ rightOutSchema.fields))
//        (resultRdd, new StructType(leftOutFields ++ rightOutSchema.fields))
    }
  }
}
