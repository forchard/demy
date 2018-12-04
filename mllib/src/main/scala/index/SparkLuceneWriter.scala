package demy.mllib.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.index.{IndexWriter,IndexWriterConfig}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.standard.StandardFilterFactory
import org.apache.lucene.analysis.standard.StandardTokenizerFactory 
import org.apache.lucene.analysis.core.StopFilterFactory 
import org.apache.lucene.analysis.TokenStream


case class SparkLuceneWriter(hdfsDest:String, tmpDir:String, boostAcronyms:Boolean = false) {
    def create = {
      val analyzer = 
        if(boostAcronyms == false) { 
          new StandardAnalyzer()
        } else {
          CustomAnalyzer.builder()
                        .withTokenizer(classOf[StandardTokenizerFactory])
                        .addTokenFilter(classOf[StandardFilterFactory])
                        .addTokenFilter(classOf[AcronymFilterFactory])
                        .addTokenFilter(classOf[StopFilterFactory])
                        .build();
      }
        Files.createDirectories(Paths.get(s"${this.tmpDir}/"))
        val indexDir = Files.createTempDirectory(Paths.get(this.tmpDir), "lucIndex")
        val index = new NIOFSDirectory(indexDir);
        val config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        val writer = new IndexWriter(index, config);
        SparkLuceneWriterInfo(writer, index)
    }
}
