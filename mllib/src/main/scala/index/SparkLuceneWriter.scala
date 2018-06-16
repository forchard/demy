package demy.mllib.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.index.{IndexWriter,IndexWriterConfig}

case class SparkLuceneWriter(hdfsDest:String, tmpDir:String, partitionMaxSizeMb:Int) {
    def create = {
        val analyzer = new StandardAnalyzer();
        Files.createDirectories(Paths.get(s"${this.tmpDir}/"))
        val indexDir = Files.createTempDirectory(Paths.get(this.tmpDir), "lucIndex")
        val index = new NIOFSDirectory(indexDir);
        val config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        val writer = new IndexWriter(index, config);
        SparkLuceneWriterInfo(writer, index)
    }
}
