package demy.mllib.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.index.{DirectoryReader}
import org.apache.lucene.search.{IndexSearcher}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import java.nio.file.{Paths}




case class SparkLuceneReader(hdfsIndexPartition:String, tmpDir:String, reuseExisting:Boolean = false, useSparkFiles:Boolean = false) {
    def open = {
        if(!useSparkFiles) {
            val fs = FileSystem.get(new Configuration())
            val dest = new org.apache.hadoop.fs.Path(this.tmpDir+"/")
            val src = new org.apache.hadoop.fs.Path(this.hdfsIndexPartition)

            
            val lockFile = new java.io.File(this.tmpDir+"/"+src.getName+".lock")
            lockFile.createNewFile()
            val wr = new java.io.RandomAccessFile(lockFile, "rw")
            try {
                val lock = wr.getChannel().lock();
                try {
                    //Critical section for downloading index file
                    val exists = new java.io.File(this.tmpDir+"/"+src.getName).exists()
                    if(exists && !reuseExisting) {
                        this.deleteRecurse(this.tmpDir+"/"+src.getName)
                    }
                    if(!exists || !reuseExisting) {
                        fs.copyToLocalFile(false,src,dest)
                    }
                } finally {
                    lock.release();
                }
            } finally {
                wr.close();
            }

            val index = new NIOFSDirectory(Paths.get(s"${this.tmpDir}/${src.getName}"), org.apache.lucene.store.NoLockFactory.INSTANCE);
            val reader = DirectoryReader.open(index);
            val searcher = new IndexSearcher(reader);
            SparkLuceneReaderInfo(searcher, index, reader);
        } else {
            val index = new NIOFSDirectory(Paths.get(org.apache.spark.SparkFiles.get(this.hdfsIndexPartition)), org.apache.lucene.store.NoLockFactory.INSTANCE);
            val reader = DirectoryReader.open(index);
            val searcher = new IndexSearcher(reader);
            SparkLuceneReaderInfo(searcher, index, reader);
        }
    }
    def mergeWith(that:SparkLuceneReader) = {
        val analyzer = new StandardAnalyzer();
        val config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND)

        val thisInfo = this.open
        val thatInfo = that.open
        val writer = new IndexWriter(thisInfo.tmpIndex,config)
        writer.addIndexes(thatInfo.tmpIndex)
        val winfo = SparkLuceneWriterInfo(writer, thisInfo.tmpIndex)
        winfo.push((this.hdfsIndexPartition.split("/") match {case s => s.slice(0, s.size-1)}).mkString("/"), false)
        thatInfo.close(true)
        FileSystem.get(new Configuration()).delete(new org.apache.hadoop.fs.Path(that.hdfsIndexPartition), true)
        this
    }
    def deleteRecurse(path:String) {
        if(path!=null && path.length>1 && path.startsWith("/")) {
            val f = java.nio.file.Paths.get(path).toFile
            if(!f.isDirectory)
              f.delete
            else {
                f.listFiles.filter(ff => ff.toString.size > path.size).foreach(s => this.deleteRecurse(s.toString))
                f.delete
            }
        }
    }
}
