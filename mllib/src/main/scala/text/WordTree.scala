package demy.mllib.text

import demy.mllib.linalg.SemanticVector

case class WordTree(hierarchy:Seq[Int], var words:Seq[String], var children:Seq[WordTree], var size:Int, var name:String, var density:Double, var semantic:SemanticVector) {
    def addNode(hierarchy:Seq[Int], words:Seq[String], size:Int, name:String, density:Double, semantic:SemanticVector):Unit = {
        //is same or child
        //val sep = ","
        //println(s"${this.hierarchy.mkString(sep)} <-- ${hierarchy.mkString(sep)}")
        assert(this.hierarchy.size<=hierarchy.size && this.hierarchy == hierarchy.slice(0, this.hierarchy.size))
        //if same
        if(this.hierarchy == hierarchy) {
            this.name = name
            this.words = words
            this.size = size
            this.density = density
            this.semantic = semantic
        }
        //child 
        else 
        { 
            //Adding childes not already exists
            if(!this.children.map(t => t.hierarchy).contains(hierarchy.slice(0, this.hierarchy.size+1)))
                this.children = this.children :+ WordTree(hierarchy.slice(0, this.hierarchy.size+1), words, Seq[WordTree](), 0, "", 0.0, null)
            //adding word to child
            this.children.filter(t => t.hierarchy == hierarchy.slice(0, this.hierarchy.size+1)).head.addNode(hierarchy, words, size, name, density, semantic)
        }
    }

    def toJson(level:Integer=0, b:scala.collection.mutable.StringBuilder=null):String = {
        val builder = 
            if(b == null) new scala.collection.mutable.StringBuilder
            else b
        val margin = Range(0, 2*level).map(i => " ").mkString("")
        builder.append(margin)
        builder.append("{\"hierarchy\":["+this.hierarchy.mkString(",")+"], \"size\":"+this.size+ ", \"name\":\""+this.name+"\", \"density\":\""+f"${this.density}%2.2f"+"\"\n")
        builder.append(margin)
        builder.append(", \"words\":["+this.words.map(w => "\""+w+"\"").mkString(",")+"]\n")
        builder.append(margin)
        builder.append(", \"semantic\":["+this.semantic.coord.map(c => "{\"i\":"+c.index+", \"v\":"+c.value+"}").mkString(",")+"]\n")
        builder.append(margin)
        builder.append(",\"children\":[\n")
        this.children.zipWithIndex.map(p => {
            if(p._2 > 0) builder.append(",")
            p._1.toJson(level+1, builder)
            builder.append("\n")
        })
        builder.append(margin)
        builder.append("]}")
        if(b == null) builder.toString
        else ""
    }
}
