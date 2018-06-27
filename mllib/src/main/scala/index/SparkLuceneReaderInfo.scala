package demy.mllib.index;

import org.apache.lucene.search.{IndexSearcher, TermQuery, BooleanQuery, FuzzyQuery}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource


case class SparkLuceneReaderInfo(searcher:IndexSearcher, tmpIndex:NIOFSDirectory, reader:DirectoryReader) {
    def search(query:String, hitsPerPage:Int, filterFields:Map[String, String] = Map[String, String](), outFields:Set[String]=Set[String]()) = {
        val terms = query.replaceAll("[^\\p{L}]+", ",").split(",").filter(s => s.length>0)
        if(terms.size == 0) 
            Array[TextIndexResult]()
        else {
            val qb = new BooleanQuery.Builder()
            terms.foreach(s => qb.add(new FuzzyQuery(new Term("text", s.toLowerCase), 1, 2), if(filterFields.size>0) Occur.MUST else Occur.SHOULD))
            filterFields.foreach(p => p match { case (category, value)=> qb.add(new TermQuery(new Term(category, value)), Occur.MUST)})
            val pop = new FunctionQuery(new DoubleFieldSource("pop"));
            val q = new org.apache.lucene.queries.CustomScoreQuery(qb.build, pop); 
            
            //query.replaceAll("[^\\p{L}\\-]+", ",").split(",").foreach(s => qb.add(new TermQuery(new Term("text", s)), Occur.SHOULD))
            val docs = searcher.search(q, hitsPerPage);
            val hits = docs.scoreDocs;
            hits.map(hit => {
                val doc = searcher.doc(hit.doc)
                TextIndexResult(id=doc.getField("id").numericValue.longValue, score = hit.score, outFields.map(field => (field, doc.getField(field).stringValue)).toMap)
            })
        }
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
    def close(deleteLocal:Boolean = false) {
        val dir = tmpIndex.getDirectory().toString
        if(new java.io.File(dir).exists()) {
            if(deleteLocal) 
                this.deleteRecurse(dir)
        }
        tmpIndex.close
        reader.close
    }
}