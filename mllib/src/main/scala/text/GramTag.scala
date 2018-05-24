package demy.mllib.text


case class GramTag(id:Short) {
        def tagName() = {
        this match {
            case GramTag(1) => "Déterminant"
            case GramTag(2) => "Femenin"
            case GramTag(3) => "Masculin"
            case GramTag(4) => "Singulier"
            case GramTag(5) => "Pluriel"
            case GramTag(6) => "Negatif"
            case GramTag(7) => "Negation"
            case GramTag(8) => "Adjetif"
            case GramTag(9) => "Verbe"
            case GramTag(10) => "Patronyme"
            case GramTag(11) => "Prénom"
            case GramTag(12) => "Nom"
            case GramTag(13) => "Adverbe"
            case GramTag(14) => "Interjection"
            case GramTag(15) => "Préposition"
            case GramTag(16) => "Nombre"
            case GramTag(17) => "Conjonction"
            case GramTag(18) => "Pronom"
            case GramTag(19) => "NomPropre"
            case GramTag(20) => "Sujet"
            case GramTag(21) => "AuxVerb"
            case _ => "Not Found"
        }
    }
    def isWordType = {
        this match {
            case GramTag(1) => true//"Déterminant"
            case GramTag(2) => false//"Femenin"
            case GramTag(3) => false//"Masculin"
            case GramTag(4) => false//"Singulier"
            case GramTag(5) => false//"Pluriel"
            case GramTag(6) => true//"Negatif"
            case GramTag(7) => false//"Negation"
            case GramTag(8) => true//"Adjetif"
            case GramTag(9) => true//"Verbe"
            case GramTag(10) => true//"Patronyme"
            case GramTag(11) => true//"Prénom"
            case GramTag(12) => true//"Nom"
            case GramTag(13) => true//"Adverbe"
            case GramTag(14) => true//"Interjection"
            case GramTag(15) => true//"Préposition"
            case GramTag(16) => true//"Nombre"
            case GramTag(17) => true//"Conjonction"
            case GramTag(18) => true//"Pronom"
            case GramTag(19) => true//"NomPropre"
            case GramTag(20) => true//"Sujet"
            case GramTag(21) => true//"AuxVerb"
            case _ => false
        }
    }
    def isAbstract = {
        this match {
            case GramTag(1) => true//"Déterminant"
            case GramTag(2) => false//"Femenin"
            case GramTag(3) => false//"Masculin"
            case GramTag(4) => false//"Singulier"
            case GramTag(5) => false//"Pluriel"
            case GramTag(6) => true//"Negatif"
            case GramTag(7) => true//"Negation"
            case GramTag(8) => false//"Adjetif"
            case GramTag(9) => false//"Verbe"
            case GramTag(10) => false//"Patronyme"
            case GramTag(11) => false//"Prénom"
            case GramTag(12) => false//"Nom"
            case GramTag(13) => true//"Adverbe"
            case GramTag(14) => true//"Interjection"
            case GramTag(15) => true//"Préposition"
            case GramTag(16) => false//"Nombre"
            case GramTag(17) => true//"Conjonction"
            case GramTag(18) => true//"Pronom"
            case GramTag(19) => false//"NomPropre"
            case GramTag(20) => true//"Sujet"
            case GramTag(21) => true//"AuxVerb"
            case _ => false
        }
    }
}; object GramTag {
    def getTagsSet(lexiqueTag:String, auxVerb:Boolean) = {
          (
            (if(lexiqueTag.contains("det")) Some(GramTag.Déterminant) else None)
            :: (if(lexiqueTag.contains("fem")) Some(GramTag.Femenin) else None)
            :: (if(lexiqueTag.contains("mas")) Some(GramTag.Masculin) else None)
            :: (if(lexiqueTag.contains("sg")) Some(GramTag.Singulier) else None)
            :: (if(lexiqueTag.contains("pl")) Some(GramTag.Pluriel) else None)
            :: (if(lexiqueTag.contains("neg")) Some(GramTag.Negatif) else None)
            :: (if(lexiqueTag.contains("negadv")) Some(GramTag.Negation) else None)
            :: (if(lexiqueTag.contains("adj")) Some(GramTag.Adjetif) else None)
            :: (if(lexiqueTag.matches("^v[^\\s].*") && !lexiqueTag.contains("adj")) Some(GramTag.Verbe) else None)
            :: (if(lexiqueTag.matches("^patr[\\s].*")) Some(GramTag.Patronyme) else None)
            :: (if(lexiqueTag.matches("^prn[\\s].*")) Some(GramTag.Prénom) else None)
            :: (if(lexiqueTag.matches("^nom[\\s].*")) Some(GramTag.Nom) else None)
            :: (if(lexiqueTag.contains("adv")) Some(GramTag.Adverbe) else None)
            :: (if(lexiqueTag.contains("interj")) Some(GramTag.Interjection) else None)
            :: (if(lexiqueTag.contains("prep")) Some(GramTag.Préposition) else None)
            :: (if(lexiqueTag.contains("nb")) Some(GramTag.Nombre) else None)
            :: (if(lexiqueTag.contains("cj")) Some(GramTag.Conjonction) else None)
            :: (if(lexiqueTag.contains("pro")) Some(GramTag.Pronom) else None)
            :: (if(lexiqueTag.matches("^npr[\\s].*")) Some(GramTag.NomPropre) else None)
            :: (if(lexiqueTag.contains("propersuj")) Some(GramTag.Sujet) else None)
            :: (if(auxVerb) Some(GramTag.AuxVerb) else None)
            :: Nil
          ).flatMap(t => t).map(t => t.id).toArray.sortWith(_ < _)
    }
    
    def Déterminant = GramTag(1)
    def Femenin = GramTag(2)
    def Masculin = GramTag(3)
    def Singulier = GramTag(4)
    def Pluriel = GramTag(5)
    def Negatif = GramTag(6)
    def Negation = GramTag(7)
    def Adjetif = GramTag(8)
    def Verbe = GramTag(9)
    def Patronyme = GramTag(10)
    def Prénom = GramTag(11)
    def Nom = GramTag(12)
    def Adverbe = GramTag(13)
    def Interjection = GramTag(14)
    def Préposition = GramTag(15)
    def Nombre = GramTag(16)
    def Conjonction = GramTag(17)
    def Pronom = GramTag(18)
    def NomPropre = GramTag(19)
    def Sujet = GramTag(20)
    def AuxVerb = GramTag(21)
    
    implicit def Short2GramTag(v: Short) = GramTag(v)
    implicit def Int2GramTag(v: Int) = GramTag(v.toShort)
    val sqlGetGramTag = org.apache.spark.sql.functions.udf((text: String, auxVerb:Boolean) => {
      GramTag.getTagsSet(text, auxVerb)
    })
};case class LexiqueEntry(id:Int,Simplified:String, Flexion:String, Lemme:String, Sémantique:String, Étymologie:String, tags:Array[Short], globalFreq:Double=0.0
);object LexiqueEntry {
    def fromUnknownWord(w:String) = LexiqueEntry(-1,Word.simplifyText(w), w, w, null, null, Array[Short](), 0.0)
};case class GroupedLexiqueEntry(id:Int,Simplified:String, entries:Array[LexiqueEntry], EntryCount:Byte, FlexionCount:Byte) {
    def disambiguate(word:String) = {
        val perfectMatch = entries.filter(le => le.Flexion.toLowerCase == word.toLowerCase) 
        val excluded = entries.filter(le => le.Flexion.toLowerCase != word.toLowerCase) 

        val toForce = entries.filter(f => f.tags.filter(t => GramTag(t).isAbstract).size>0)
        val filteredEntries = if(toForce.size > 0 ) { toForce } else entries

        if(perfectMatch.size > 0 && excluded.size > 0 && Word.containsAccents(word) ) {
            GroupedLexiqueEntry(this.id, this.Simplified, perfectMatch, perfectMatch.size.toByte, perfectMatch.map(e => e.Flexion).distinct.size.toByte)
        } else if(filteredEntries.size < entries.size){
            val perfectMatch = filteredEntries.filter(le => le.Flexion.toLowerCase == word.toLowerCase) 
            val excluded = filteredEntries.filter(le => le.Flexion.toLowerCase != word.toLowerCase) 

            if(perfectMatch.size > 0 && excluded.size > 0 && ((1.0*perfectMatch.map(e => e.globalFreq).reduce(_ + _) / excluded.map(e => e.globalFreq).reduce(_ + _))>1000 ) ) 
                GroupedLexiqueEntry(this.id, this.Simplified, perfectMatch, perfectMatch.size.toByte, perfectMatch.map(e => e.Flexion).distinct.size.toByte)
            else 
                GroupedLexiqueEntry(this.id, this.Simplified, filteredEntries, filteredEntries.size.toByte, filteredEntries.map(e => e.Flexion).distinct.size.toByte)
        } else {             
                this
        }      
    }
    def reduceWith(that:GroupedLexiqueEntry) = {
        val allEntries = this.entries ++ that.entries
        GroupedLexiqueEntry(this.id, this.Simplified, allEntries, allEntries.size.toByte, allEntries.map(e => e.Flexion).distinct.size.toByte)
    }
};case class TagChallenge(tags:Array[Short], likehood:Double
);case class WordLexOptions(word:Word, options:GroupedLexiqueEntry, isTagged:Boolean=false, tags:Array[Short]=null, likehood:Double=0.0, tagScore:Double=0.0, tagBy:Option[String]=None, hrTags:Option[Array[String]]=None) {
    def getIsTagged = this.options !=null && this.options.entries.length == 1 || this.isTagged
    def applyTag(challenge:TagChallenge=null, logAs:Option[String]=None):WordLexOptions = {
        if(this.isTagged || (challenge == null && !this.isTagged && !this.getIsTagged))
            this
        else if(!this.isTagged && this.getIsTagged)
            WordLexOptions(this.word, this.options,  true, this.options.entries(0).tags, 1.0, 0.0, logAs)
        else if(challenge != null && this.options !=null && this.options.entries.length>1) {
            val scoredEntries = this.options.entries.flatMap(lexiqueOption => {
                var tagsFound = 0.0
                var iChallenge = 0
                var iOption = 0
                //All tags in challenge shoud be found on option for a match.
                while(iChallenge<challenge.tags.size && iOption<lexiqueOption.tags.size) {
                    if(lexiqueOption.tags(iOption) == challenge.tags(iChallenge)) {
                        tagsFound = tagsFound + 1
                        iOption = iOption + 1
                    }
                    else if(lexiqueOption.tags(iOption) < challenge.tags(iChallenge)) 
                        iOption = iOption + 1
                    else 
                        iChallenge = iChallenge + 1
                }
                if(tagsFound==challenge.tags.size) 
                    Some(lexiqueOption, tagsFound/lexiqueOption.tags.size)
                else
                    None
            }).sortWith(_._1.tags.size<_._1.tags.size).slice(0,1)
            if(scoredEntries.size == 0)
                this
            else
                WordLexOptions(this.word
                            , GroupedLexiqueEntry(this.options.id,this.options.Simplified, scoredEntries.map(p => p._1), scoredEntries.size.toByte, scoredEntries.map(p=>p._1.Flexion).distinct.size.toByte)
                            , true
                            , scoredEntries(0)._1.tags
                            , challenge.likehood
                            , scoredEntries(0)._2
                            , logAs
            )
        }
        else 
            this
    }
    def getTaggedWord = Word(this.word.word, this.word.simplified, this.word.isWord, this.word.index, this.word.phraseId, this.word.docId
                            , if(this.isTagged) this.options.entries(0).Lemme else this.word.word, this.tags
                            , Word.getSemanticUsage(this.tags))
    def chooseBest(that:WordLexOptions) = {
        (this.getIsTagged, that.getIsTagged) match {
            case (true, false) => this
            case (false, true) => that
            case (true, true) => (
                if((this.options.entries(0).tags.contains(GramTag.Pronom.id) || this.options.entries(0).tags.contains(GramTag.Déterminant.id)) && that.options.entries(0).tags.contains(GramTag.Nom.id))
                    this
                else if((that.options.entries(0).tags.contains(GramTag.Pronom.id) || that.options.entries(0).tags.contains(GramTag.Déterminant.id)) && that.options.entries(0).tags.contains(GramTag.Nom.id))
                    that
                else if(this.likehood>that.likehood)
                    this
                else
                    that
            )
            case _ => (   
                if(that.options== null || that.options.entries == null)
                    this
                else if(this.options== null  || this.options.entries == null)
                    that
                else if(this.options.entries.size < that.options.entries.size)
                    this
                else 
                    that
            )
        }
    }
    def setHumanReadableTags = 
            WordLexOptions(this.word, this.options, this.isTagged, this.tags, this.likehood, this.tagScore, this.tagBy, if(this.isTagged) Some(this.options.entries(0).tags.map(e => GramTag(e).tagName)) else None)
    def tagAny = {
        if(this.isTagged || this.options == null || this.options.entries.size == 0)
            this
        else
            WordLexOptions(this.word, this.options, true, this.options.entries(0).tags, this.likehood, this.tagScore, Some("Random"), Some(this.options.entries(0).tags.map(e => GramTag(e).tagName)))
    }
};case class TagTransition(from:Array[Short], to:Array[Short],count:Int, fromCount:Int, likehood:Double
);case class VerbText(docId:Int, words:Array[WordLexOptions]) {
    def reduceWords(that:VerbText) = {
        VerbText(this.docId, (this.words ++ that.words).sortWith(_.word.index<_.word.index))
    }
    def getTransitions = {
        var i=0
        this.words.filter(w => w.word.isWord).zipWithIndex.flatMap(p =>{
            if(p._2< this.words.size-1) {
                val thisWord = p._1
                val nextWord = this.words(p._2+1)
                if(thisWord.isTagged && nextWord.isTagged) {
                    Some(TagTransition(thisWord.options.entries(0).tags.filter(e=>GramTag(e).isWordType), nextWord.options.entries(0).tags.filter(e=>GramTag(e).isWordType), 1, 0, 0.0))
                }
                else None
            }
            else None
        })
    }
    
    def tagWords(frequences:Map[String,List[TagTransition]], direction:String="right") = {
        val iFrom = if(direction =="right") 1 else this.words.size-2
        val iTo = if(direction =="right") words.size-1 else 0 
        var iToTag = iFrom
        while(iToTag!=iTo && this.words.size>1) {
            if(this.words(iToTag).word.isWord) {
                val wordToTag = this.words(iToTag)
                var shift = if(direction =="right") 1 else -1
                var wordToUse = this.words(iToTag + shift)
                while(!wordToUse.word.isWord && iToTag + shift >=0 && iToTag + shift < this.words.size) {
                    var wordToUse = this.words(iToTag + shift)
                    shift = shift + (if(direction =="right") 1 else -1)
                }
                if(wordToUse.isTagged && !wordToTag.isTagged) {
                    this.words(iToTag) =
                                            frequences.get(wordToUse.tags.filter(t => GramTag(t).isWordType).mkString("-")) match {
                                                case Some(transitions) => {
                                                    transitions.map(tt => wordToTag.applyTag(TagChallenge(if(direction == "right") tt.to else tt.from,tt.likehood), Some(s"${direction} Match") ))
                                                            .reduce((wo1, wo2)=> wo1.chooseBest(wo2))
                                                }
                                                case None => wordToTag
                                            }
                }
            }
            iToTag = iToTag + (if(iToTag == iTo) 0 else if(direction=="right") 1 else -1)
        }
        this
    } 
    def setPhrases = {
        var i = 0
        var iPhrase = 0
        var phraseSize = 0

        while(i< this.words.size) {
            var newPhrase = false
            var isWord = this.words(i).word.isWord
            var afterPhraseSplit = (i > 0 && Word.containsPhraseSep(this.words(i - 1).word.word))
            var phraseSplitSize = phraseSize;
            var s = 1
            while(phraseSplitSize <= 20 && i + s < this.words.size && !Word.containsPhraseSep(this.words(i + s).word.word)) {s = s + 1; if(isWord) phraseSplitSize = phraseSplitSize + 1}
            var tags =  this.words(i).tags
            /*
            while(i - s >= 0 && !this.words(i - s).isWord) s = s - 1;
            var pTags =  if(i - s < 0 || this.words(i-s).tags == null) Array[Short]() else this.words(i-s).tags
            
            var s = 1
            while(i + s < this.words.size && !this.words(i + s).isWord) s = s + 1;
            var nTags = if(i+s==this.words.size || this.words(i+s).tags == null) Array[Short]() else this.words(i+s).tags
            */
            //If phrase separator simbols make a phrase smaller than 20 words we ignore
            if( isWord && i + 5 < this.words.size && phraseSize > 3
                && (
                    (afterPhraseSplit
                    ||
                    (phraseSplitSize < 20 && tags != null && tags.contains(GramTag.Sujet.id) && phraseSize>=10)
                    || 
                    phraseSize>=20
                )
            ))
            {
                newPhrase = true
                phraseSize=1
            }
            else if(isWord){
                phraseSize=phraseSize+1
            }
            
            if(newPhrase) iPhrase = iPhrase + 1
            val thisWord = this.words(i)
            this.words(i) = WordLexOptions(thisWord.word.updatePhrase(iPhrase), thisWord.options, thisWord.isTagged, thisWord.tags, thisWord.likehood, thisWord.tagScore, thisWord.tagBy, thisWord.hrTags)
            i = i + 1
        }
        this
    }
    
    def tagStatistics = (this.words.filter(c => c.isTagged).size, this.words.size)
}
