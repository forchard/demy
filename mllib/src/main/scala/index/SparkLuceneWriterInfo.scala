package demy.mllib.index;

import org.apache.lucene.index.{IndexWriter}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.document.{Document, TextField, StringField, NumericDocValuesField, DoubleDocValuesField, StoredField, Field}
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

case class SparkLuceneWriterInfo(writer:IndexWriter, tmpIndex:NIOFSDirectory){
    def indexText(id:Long, text:String, popularity:Double = 1.0, categories:Map[String, String]=Map[String, String]()) {

        val doc = new Document();
        doc.add(new StoredField("id", id));
        //)
        doc.add(new TextField("text",text, Field.Store.NO))
        categories.toSeq.foreach(p => p match {case (catName, value) => doc.add(new TextField(catName, value, Field.Store.YES))})
        doc.add(new DoubleDocValuesField("pop", popularity));
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
