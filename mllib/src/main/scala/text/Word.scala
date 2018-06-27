package demy.mllib.text

import demy.mllib.linalg.SemanticVector

case class Doc(text:String, docId:Int
);case class Word(word:String, simplified:String, isWord:Boolean, index:Int, phraseId:Int, docId:Int, root:String=null, tags:Array[Short]=null
                    , wordType:String=null, semantic:SemanticVector=null,hierarchy:Seq[Int]=null, clusterCount:Int=1) {
    def updatePhrase(phraseId:Int) = Word(this.word, this.simplified, this.isWord, this.index, phraseId, this.docId, this.root, this.tags, this.wordType, this.semantic)
    def setSemantic(semantic:SemanticVector, defaultIfNull:Boolean = false, splitOnCharacters:Boolean = false) = {
        Word(this.word, this.simplified, this.isWord, this.index, this.phraseId, this.docId, this.root, this.tags, this.wordType
            , if(semantic != null) semantic 
                else if(defaultIfNull && !splitOnCharacters) SemanticVector.fromWord(this.word.toLowerCase) 
                else if(defaultIfNull && splitOnCharacters &&  this.word.replaceAll("\\s", "").size > 0) 
                  this.word.replaceAll("\\s", "").map(c => SemanticVector.fromWord(c.toString.toLowerCase)).reduce((v1, v2) => v1.sum(v2)) 
                else null
        )
    }
    def toSemanticPhrase() = {
        val w = this
        val combined = if(w.semantic == null /*|| stopWords.contains(w.root)*/)
            null
        else 
            w.semantic
        SemanticPhrase(w.docId, w.phraseId, Seq(w), if(combined == null) None else Some(combined))
    }
    def sumWords(that:Word) = Word(this.word, this.simplified, this.isWord, this.index, this.phraseId, this.docId, this.root
                                    , this.tags, this.wordType, this.semantic, this.hierarchy, this.clusterCount+ that.clusterCount) 
    def setHierarchy(hierarchy:Seq[Int]) = Word(this.word, this.simplified, this.isWord, this.index, this.phraseId, this.docId
                                                 , this.root, this.tags, this.wordType, this.semantic, hierarchy, this.clusterCount) 
    def setClusterCount(count:Int) = Word(this.word, this.simplified, this.isWord, this.index, this.phraseId, this.docId
                                           , this.root, this.tags, this.wordType, this.semantic, hierarchy, count) 
};object Word {
    def containsPhraseSep(s:String) = if(s==null) false else "[\\r\\n\\.;!?]".r.findFirstIn(s) match {case Some(s) => true case _ => false}
    def simplifyText(word:String):String = {
        val sb = new StringBuilder
        var i = 0
        while(i < word.length) {
            val nextChar = if(i+1<word.length) word(i+1).toString.toLowerCase else "X"
            sb.append(word(i).toString.toLowerCase match {
                case "à" => "a" case "á" => "a" case "â" => "a" case "ã" => "a" case "ä" => "a" case "å" => "a" case "æ" => "a"
                case "è" => "e" case "é" => "e" case "ê" => "e" case "ë" => "e" case "œ" => "e"
                case "ì" => "i" case "í" => "i" case "î" => "i" case "ï" => "i" 
                case "ð" => "o" case "ñ" => "o" case "ò" => "o" case "ó" => "o" case "ô" => "o" case "õ" => "o" case "ö" => "o" case "ø" => "o" 
                case "ù" => "u" case "ú" => "u" case "û" => "u" case "ü" => "u"   
                case "ç" => "c"
                case "-" => " "
                case "'" => " "
                case "l" => if(nextChar == "'") {" " } else "l"
                case "d" => if(nextChar == "'") {" " } else "d"
                case other => other
            })
            i =  i + 1
        }
        sb.toString
    }
    def containsAccents(word:String):Boolean = {
        var i = 0
        while(i < word.length) {
            if(word(i).toString.toLowerCase match {
                case "à" => true case "á" => true case "â" => true case "ã" => true case "ä" => true case "å" => true case "æ" => true
                case "è" => true case "é" => true case "ê" => true case "ë" => true case "œ" => true
                case "ì" => true case "í" => true case "î" => true case "ï" => true 
                case "ð" => true case "ñ" => true case "ò" => true case "ó" => true case "ô" => true case "õ" => true case "ö" => true case "ø" => true 
                case "ù" => true case "ú" => true case "û" => true case "ü" => true   
                case "ç" => true
                case _ => false
            }) return true
            i =  i + 1
        }
        return false;
    }
    val sqlSimplify = org.apache.spark.sql.functions.udf((text: String) => {
      Word.simplifyText(text)
    })
    def linksAsBlanks(s:String) = {
        val urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        val p = java.util.regex.Pattern.compile(urlPattern,java.util.regex.Pattern.CASE_INSENSITIVE);
        val m = p.matcher(s);
        var sb:Option[StringBuilder] = None
        var i = 0
        while(m.find) {
            sb = sb match {case Some(b) => sb case _ => Some(new StringBuilder())}
            sb.get.append(s.substring(i, m.start))
            sb.get.append(Range(m.start, m.end).map(i => " ").mkString(""))
            i = m.end()
        }
        
        val ret =  sb match {case Some(b) => b.append(s.substring(i)).toString case _ => s}
        ret
    }
    def splitDoc(doc:Doc, linksAsSeparators:Boolean = true, splitPhrases:Boolean = true):Seq[Word] = {
        val orig = if(doc.text == null) "" else doc.text 
        val noLinks = if(linksAsSeparators) linksAsBlanks(orig) else orig
        val simpli = Word.simplifyText(noLinks).map(car => car.toString.replaceAll("[^(\\p{L})]|[\\(]|[\\)]", " ")).mkString("")
        val WStarts = simpli.zipWithIndex.map(p => if(p._1 != ' ' && (p._2 == 0 || simpli(p._2-1) == ' ')) p._2 else -1).filter(i => i >= 0)
        val WEnds = simpli.zipWithIndex.map(p => if(p._1 != ' ' && (p._2 == simpli.size-1 || simpli(p._2+1) == ' ')) p._2 else -1).filter(i => i >= 0)
        val wordIndex = WStarts.zipWithIndex.map(p => Word(orig.substring(p._1, WEnds(p._2)+1),simpli.substring(p._1, WEnds(p._2)+1), true, p._1 , -1, doc.docId))
        val NWStarts = simpli.zipWithIndex.map(p => if(p._1 == ' ' && (p._2 == 0 || simpli(p._2-1) != ' ')) p._2 else -1).filter(i => i >= 0)
        val NWEnds = simpli.zipWithIndex.map(p => if(p._1 == ' ' && (p._2 == simpli.size-1 || simpli(p._2+1) != ' ')) p._2 else -1).filter(i => i >= 0)
        val NWordIndex = NWStarts.zipWithIndex.map(p => Word(orig.substring(p._1, NWEnds(p._2)+1),simpli.substring(p._1, NWEnds(p._2)+1), false, p._1 , -1, doc.docId))
        var phraseId = 0
        (wordIndex ++ NWordIndex)
            .sortWith((a, b) => a.index < b.index)
            .zipWithIndex.map(p => {
                val w = Word(p._1.word, p._1.simplified, p._1.isWord, p._2:Int, phraseId, doc.docId)
                phraseId = phraseId + (if(Word.containsPhraseSep(w.word) && splitPhrases) 1 else 0)
                w
            })
    }
    def getSemanticUsage(tags:Array[Short]) = {
        if(tags == null || tags.size == 0) 
            "Autre"
        else {
            tags.map(t => t match {
                            case 1 => ("Connector", 0)//"Déterminant"
                            case 2 => ("Gender", 90)//"Femenin"
                            case 3 => ("Gender", 90)//Masculin"
                            case 4 => ("Arity", 80)//Singulier
                            case 5 => ("Arity", 80)//Pluriel
                            case 6 => ("Negation", 5)//"Negatif"
                            case 7 => ("Negation", 5)//"Negation"
                            case 8 => ("Characteristic", 1) //Adjetif
                            case 9 => ("Action", 2) //Verbe
                            case 10 => ("Entity", 3)//Patronyme
                            case 11 => ("Entity", 3)//Prénom"
                            case 12 => ("Entity", 3)//Nom", 3)
                            case 13 => ("Quality", 50)//("Adverbe"
                            case 14 => ("Connector", 0)//("Interjection"
                            case 15 => ("Connector", 0)//("Préposition"
                            case 16 => ("Quantity", 70)//("Nombre"
                            case 17 => ("Connector", 0)//("Conjonction"
                            case 18 => ("Connector", 0)//("Pronom")
                            case 19 => ("Entity", 3)//("NomPropre", 4)
                            case 20 => ("Connector", 0)//Sujet"
                            case 21 => ("Connector", 0)//AuxVerb
                            case _ => ("Autre", 100)
                        
            }).sortWith(_._2 < _._2).head._1
        }
    }
}
