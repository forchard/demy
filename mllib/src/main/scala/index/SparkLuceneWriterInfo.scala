package demy.mllib.index;

import org.apache.lucene.index.{IndexWriter}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

case class SparkLuceneWriterInfo(writer:IndexWriter, tmpIndex:NIOFSDirectory){
    def indexRow(row:Row, textFieldPosition:Int, popularityPosition:Option[Int]=None, notStoredPositions:Set[Int]=Set[Int](), tokenisation:Boolean=true) {
        val doc = new Document();
        if(tokenisation)  doc.add(new TextField("_text_",row.getAs[String](textFieldPosition), Field.Store.NO))
        else doc.add(new StringField("_text_",row.getAs[String](textFieldPosition), Field.Store.NO))
        val schema = row.schema.fields.zipWithIndex.foreach(p => p match {case (field, i) => 
          if(!notStoredPositions.contains(i) && !row.isNullAt(i)) field.dataType match {
             case dt:StringType =>   doc.add(new StringField(field.name, row.getAs[String](i), Field.Store.YES))
             case dt:IntegerType => {doc.add(new IntPoint("_point_"+field.name, row.getAs[Int](i)))
                                     doc.add(new StoredField(field.name, row.getAs[Int](i)))}
             case dt:BooleanType => {doc.add(new BinaryPoint("_point_"+field.name, Array(if(row.getAs[Boolean](i)) 1.toByte else 0.toByte )))
                                     doc.add(new StoredField(field.name,  Array(if(row.getAs[Boolean](i)) 1.toByte else 0.toByte )))}
             case dt:LongType => {   doc.add(new LongPoint("_point_"+field.name, row.getAs[Long](i)))
                                     doc.add(new StoredField(field.name, row.getAs[Long](i)))}
             case dt:DoubleType => { doc.add(new DoublePoint("_point_"+field.name, row.getAs[Double](i)))
                                     doc.add(new StoredField(field.name, row.getAs[Double](i)))}
             case dt:FloatType => {  doc.add(new FloatPoint("_point_"+field.name, row.getAs[Float](i)))
                                     doc.add(new StoredField(field.name, row.getAs[Float](i)))
             }
             case _ => { //Storing field as a serialized object
               val serData=new ByteArrayOutputStream();
               val out=new ObjectOutputStream(serData);
               out.writeObject(row.get(i));
               out.close();
               serData.close();
               doc.add(new StoredField(field.name, serData.toByteArray()))
             }
        }})
        popularityPosition match {case Some(i) => doc.add(new DoubleDocValuesField("_pop_", row.getAs[Double](i)))
                                  case _ => {} 
        }

        this.writer.addDocument(doc);

    }
    def push(hdfsDest:String, deleteSource: Boolean = false) = {
        val src_str = tmpIndex.getDirectory().toString
        this.writer.commit
        this.writer.close()
        this.tmpIndex.close()
        val fs = FileSystem.get(new Configuration())
        fs.mkdirs(new org.apache.hadoop.fs.Path(hdfsDest))
        fs.setReplication(new org.apache.hadoop.fs.Path(hdfsDest),1)
        val dest = new org.apache.hadoop.fs.Path(hdfsDest)
        val src = new org.apache.hadoop.fs.Path(src_str)
        val finalDest = new org.apache.hadoop.fs.Path(hdfsDest+"/"+src.getName)
        if(fs.exists(finalDest))
            fs.delete(finalDest, true)
        fs.copyFromLocalFile(deleteSource,true, src ,dest)
    }
}
