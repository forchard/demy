package demy.geo

object TextTools {
    def levensteinDistance(a: String, b: String) =
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
          case (h, ((d, v), y)) => Math.min(Math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
        }) last
    
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
                case "ç" => "s" case "z" => "s"
                case "v" => "b" case "w" => "b"
                case "k" => "c" 
                case "g" => "j"
                case "-" => " "
                case "l" => if(nextChar == "'") { i = i +1; "" } else if(nextChar == "l") "" else "l"
                case "d" => if(nextChar == "'") { i = i +1; "" } else "d"
                case "s" => if(nextChar == "s") "" else "s"
                case "q" => if(nextChar == "u") { i = i +1; "c" } else "c"
                case other => other
            })
            i =  i + 1
        }
        sb.toString
    }
    def splitText(text:String):Array[String] = {
    if(text == null || text.length == 0)
        Array[String]()
    else
        text.split("([^\\p{L}0-9])+").map(w =>simplifyText(w))
    }

    def replaceLast(base:String, search:String, replace:String) = {
        if(search == null || search.length == 0)
            base
        else {
            val b = new StringBuilder
            val parts = s"<<${base}>>".split(search)
            var i = parts.length - 1
            var alreadyReplaced = false
            while(i>= 0) {
                if(!alreadyReplaced && i < (parts.length-1) && (parts(i).last.toString.matches("([^\\p{L}0-9])+") && parts(i+1)(0).toString.matches("([^\\p{L}0-9])+"))) {
                    b.append(replace.reverse)
                    alreadyReplaced = true
                }
                else if(i+1<parts.length) {
                    b.append(search.reverse)
                }
                if(parts(i).length > 0) 
                    b.append(parts(i).substring(if(i==0) 2 else 0, parts(i).length-(if(i+1 == parts.length) 2 else 0)).reverse)
                i = i - 1
            }
            b.reverse.toString
        }
    }

    def replaceFirst(base:String, search:String, replace:String) = {
        if(search == null || search.length == 0)
            base
        else {
            val b = new StringBuilder
            val parts = s"<<${base}>>".split(search)
            var i = 0
            var alreadyReplaced = false
            while(i< parts.length) {
                if(parts(i).length > 0) b.append(parts(i).substring(if(i==0) 2 else 0, parts(i).length-(if(i+1 == parts.length) 2 else 0)))
                if(!alreadyReplaced && parts.length>1 && (parts(i).last.toString.matches("([^\\p{L}0-9])+") && parts(i+1)(0).toString.matches("([^\\p{L}0-9])+"))) {
                    b.append(replace)
                    alreadyReplaced = true
                }
                else if(i+1<parts.length) {
                    b.append(search)
                }
                i = i + 1
            }
            b.toString
        }
    }
    
    def textSimilarity(base:String, test:String, memoryIndex:Double=1, thresholdToZero:Double=0, simplifyStrings:Boolean=true):Double = {
        var totalSim = 0.0
        var totalWeight = 0.0
        val sWords = (if(simplifyStrings) simplifyText(base) else base).split("([^\\p{L}0-9])+")
        val tWords = (if(simplifyStrings) simplifyText(test) else test).split("([^\\p{L}0-9])+")

        var i = 0
        while(i<sWords.size) {
            var sim = 0.0
            var j = 0
            while(j<tWords.size) {
                val thisSim = 1-levensteinDistance(sWords(i), tWords(j)).toDouble/Math.max(sWords(i).length, tWords(j).length)
                if(thisSim > sim) 
                    sim = thisSim
                j = j +1
            }
            val thisWeight = sWords.size * this.memoryRatio(1.0*(i+1)/(sWords.size), memoryIndex)
            totalSim = totalSim + (if(sim<thresholdToZero) 0 else sim) * thisWeight 
            totalWeight = totalWeight + thisWeight
            i = i+1
        }
        totalSim / totalWeight
    }

    def memoryRatio(index:Double, mem:Double) = {
        val ret = Math.min(1,
            if(mem>0.5) {
                val i = (mem-0.5)*2
                i + (1-i)*index
            }
            else{
                val i1 = 1-2*mem
                val i2 = 2*mem
                if(index<i1) 0
                else (index-i1)/(1-i1)
            }
        )
//        println(s"index:${index}, mem:${mem}, ret:${ret} ")
        ret
    }

}

