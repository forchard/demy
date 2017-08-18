package demy.geo


case class AddressNumber(number:Option[String], addressNoNumber:String)
case class Locality(locality:String, postcode:Int, inseeCode:String, localityName:String, meanLon:Double, meanLat:Double, count:Int) {
    def + (l:Locality): Locality =
      Locality(locality, postcode, inseeCode, localityName
        ,(meanLon*count+l.meanLon*l.count)/(count+l.count)
        ,(meanLat*count+l.meanLat*l.count)/(count+l.count)
        ,count+l.count
      )
}
case class Street(street: String, locality: String, postcode:Int, numbers:Seq[String], lon:Seq[Double], lat:Seq[Double], meanLon:Double, meanLat:Double, count:Int) {
    def + (s:Street):Street = 
      Street(street,locality,postcode, numbers ++ s.numbers,lon ++ s.lon,lat ++ s.lat
        ,(meanLon*count+meanLon*s.count)/(count+s.count)
        ,(meanLat*count+meanLat*s.count)/(count+s.count)
        ,count+s.count
    )
}

case class ParsedAddress(
    val id:Int
    ,val table:String
    ,val input:String
    , val number:Option[String]=None
    , val postcode:Option[Int]=None
    , val inseeCode:Option[String]=None
    , val irisCode:Option[String]=None
    , val street:Option[String]=None
    , val locality:Option[String]=None
    , val localitySimilarity:Option[Double]=None
    , val localityMatchedBy:Option[String]=None
    , val streetSimilarity:Option[Double]=None
    , val streetSoftSimilarity:Option[Double]=None
    , val longitude:Option[Double]=None
    , val latitude:Option[Double]=None
    ) {
    
    def inputSimplified:String =  TextTools.simplifyText(this.input)

    def splitAdressNumber(addressToUse:Option[String]=None):AddressNumber = {
      val _input = addressToUse match { case Some(s) => s case _ => this.input }
      val n = 
        if(_input.contains(",")) 
            _input.split(",")(0).split("([^\\p{L}0-9])+").filter(s => s.matches(".*[0-9]+.*") && s.length <= Math.log10(Int.MaxValue).toInt) 
        else {
          val arr = _input.split("([^\\p{L}0-9])+")
          arr.zipWithIndex.filter(p => p._2 < arr.size/2).map(p => p._1).filter(s => s.matches(".*[0-9]+.*") && s.length <= Math.log10(Int.MaxValue).toInt) 
        }
        
      if(n.size>0) {
         AddressNumber(Some(n(0)), _input.replaceFirst(n(0), ""))
       } 
       else
         AddressNumber(None, _input)
    }
    

    def adressPostCode(addressToUse:Option[String]=None):Option[Int] = {
        val _iNoNumber = addressToUse match { case Some(s) => s case _ => this.input }

        if(_iNoNumber.matches(".*[0-9]{4,}.*")) 
          Some(_iNoNumber.split("([^0-9])+").map(s => if(s.length<=Math.log10(Int.MaxValue).toInt) s else s.substring(0, Math.log10(Int.MaxValue).toInt)).last.toInt) 
        else 
          None
    }
    
    def setPostcodeFromPostcodeIndex(index:scala.collection.immutable.HashMap[Int, (String, String)]) = {
        val parsedPostcode = this.adressPostCode(Some(this.inputSimplified))
        val localityFound = parsedPostcode match { case Some(cp) => index.get(cp) case _ => None }
        
        ParsedAddress(this.id, this.table, this.input, this.number, localityFound match {case Some(loc) => parsedPostcode case _ => None}, localityFound match {case Some(loc) => Some(loc._2) case _ => None}, None ,None, localityFound match {case Some(loc) => Some(loc._1) case _ => None}, None, localityFound match {case Some(loc) => Some("postcodeList") case _ => None}, None, None, None, None)
    }
    

    def setPostcodeFromLocalityIndex(address:String, index:scala.collection.immutable.HashMap[String, List[(String, Int, String)]]) = {
        var bestLocality:Option[String] = None 
        var bestPostCode:Option[Int] = None 
        var bestPostSimilarity:Option[Double] = None
        var bestInseeCode:Option[String] = None
        val addressPoctCode = this.adressPostCode(Some(address))
        val localityNoPostCode = addressPoctCode match {case Some(cp) => address.replace(cp.toString, "") case _ => address }

        address.split("([^\\p{L}0-9])+").foreach ( word => {
            index.get(word) match {
                case Some(matchingLocalities:List[(String, Int, String)]) => {
                    matchingLocalities.foreach (locPair => {

                        val thisSimilarity = addressPoctCode match {
                            case (Some(cp)) =>
                                (
                                  (1-1.0*Math.abs(cp-locPair._2)/Math.max(cp,locPair._2)
                                    +TextTools.textSimilarity(localityNoPostCode, locPair._1, 0.7, 0.7)
                                  )/2
                                )
                            case _ => TextTools.textSimilarity(localityNoPostCode, locPair._1, 0.7, 0.7)
                        }
                        bestPostSimilarity = 
                                bestPostSimilarity match {case Some(prevdSim) => {
                                                      if(thisSimilarity > prevdSim) {
                                                        bestPostCode = Some(locPair._2)
                                                        bestInseeCode = Some(locPair._3)
                                                        bestLocality = Some(locPair._1)
                                                        Some(thisSimilarity)
                                                      }
                                                      else bestPostSimilarity
                                                    }
                                                case None => {
                                                        bestPostCode = Some(locPair._2)
                                                        bestInseeCode = Some(locPair._3)
                                                        bestLocality = Some(locPair._1)
                                                        Some(thisSimilarity)
                                                    }
                                                }
                    })
                }
                case _ => {}
            }
        })
        ParsedAddress(this.id, this.table, this.input, this.number, bestPostCode, bestInseeCode, None, None, bestLocality, bestPostSimilarity, bestLocality match {case Some(s) => Some("Locality") case _ => None}, None, None, None, None)
    }

    def extractStreetName(addressNumber:AddressNumber, localityName:Option[String]=None, postcodeNumber:Option[Int]=None ):String = {
        val lName = localityName match { case Some(l) => localityName case _ => this.locality } 
        val locText = lName match { case Some(l) => Some(l) case _ => None }
        val pcode = postcodeNumber match { case Some(cp) => postcodeNumber case _ => this.postcode}
        val removed = new scala.collection.mutable.HashSet[String]()
        addressNumber.addressNoNumber.split("([^\\p{L}0-9])+").reverse.filter( word => {
            val removeWord = (
                    !removed.contains(word)
                && (
                    (pcode match {
                        case Some(cp) => { word.matches("[0-9]+") && word.length<=Math.log10(Int.MaxValue).toInt && word.toInt == cp }
                        case _ => false
                    })
                ||
                    (locText match {
                        case Some(lText) => { TextTools.textSimilarity(word,lText) >= 0.8 }
                        case _ => false
                    })
                )
            )
            if(removeWord) removed.add(word)
            !removeWord
        }).reverse.mkString(" ")
    }
    
    def locateInStreet(testStreet:Street):Option[ParsedAddress] = {
      val numAddress = this.splitAdressNumber(Some(this.inputSimplified))
      val parsedStreetName = this.extractStreetName(numAddress)
      
      val AddSimilarity = TextTools.textSimilarity(parsedStreetName, testStreet.street, 1, 0.7)
      val AddSoftSimilarity = TextTools.textSimilarity(parsedStreetName, testStreet.street, 1, 0.0)
      if(AddSimilarity>0) {
          numAddress.number match {
              case Some(numstr) => {
                  val numInt = numstr.replaceAll("[^0-9]", "").toInt
                  var numLett = numstr.replaceAll("[0-9]", "")
                  if(numLett.length == 0) numLett = "0"
                  var partialIndex = -1
                  var partialSimilarity = -1.0 
                  var i = 0
                  var matchFound = false
                  var partialMatchFound = false
                  while(i<testStreet.numbers.length && !matchFound) {
                      matchFound = TextTools.simplifyText(testStreet.numbers(i)) == TextTools.simplifyText(numstr) 
                      if(!matchFound) {
                        val addNum = testStreet.numbers(i).replaceAll("[^0-9]", "")
                        if(addNum.length > 0 && addNum.toInt == numInt) {
                            partialMatchFound = true
                            var addNumLett = testStreet.numbers(i).replaceAll("[0-9]", "")
                            if(addNumLett.length == 0) addNumLett = "1"
                            val thisSimilarity = TextTools.textSimilarity(addNumLett, numLett)
                            if(thisSimilarity>partialSimilarity) {
                                partialIndex = i
                                partialSimilarity = thisSimilarity
                                partialMatchFound = true
                            }
                            
                        }
                        i = i + 1
                      }
                  }
                  val index = if(matchFound) Some(i)
                              else if(partialMatchFound) Some(partialIndex)
                              else None
                  index match {
                    case Some(i) => {
                      Some(ParsedAddress(this.id, this.table, this.input, Some(testStreet.numbers(i)), this.postcode, this.inseeCode, None ,Some(testStreet.street), Some(testStreet.locality), this.localitySimilarity, this.localityMatchedBy, Some(AddSimilarity), Some(AddSoftSimilarity), Some(testStreet.lon(i)) , Some(testStreet.lat(i))))
                    }
                    case _ => {
                      Some(ParsedAddress(this.id, this.table, this.input, None, this.postcode, this.inseeCode, None, Some(testStreet.street), Some(testStreet.locality), this.localitySimilarity, this.localityMatchedBy, Some(AddSimilarity), Some(AddSoftSimilarity), Some(testStreet.meanLon) , Some(testStreet.meanLat)))
                    }
                  }
              }
              case _ => None
          }
          
      }
      else
        None
    }

    def getBestStreetMatch(that:ParsedAddress) = {
        (this.streetSimilarity, that.streetSimilarity) match {
            case (Some(thisSim), Some(thatSim)) => {
                if(thisSim>thatSim 
                        || thisSim == thatSim && this.streetSoftSimilarity.get > that.streetSoftSimilarity.get 
                        || thisSim == thatSim && this.streetSoftSimilarity.get == that.streetSoftSimilarity.get && !this.number.isEmpty) 
                    this 
                else 
                    that
            }
            case (Some(thisSim), None) => this
            case (None, Some(thatSim)) => that
            case _ => that
        }    
        
    }  
    def setIrisCode(geom:Array[Byte], irisCode:String) = {
      val matched = (this.longitude, this.latitude) match {
                        case (Some(lon), Some(lat)) => 
                          GeoManager.intersectsWithPoint(geom, lon, lat)
                        case _ => false
      }
      ParsedAddress(this.id, this.table, this.input, this.number, this.postcode, this.inseeCode, matched match{ case true =>Some(irisCode) case _ => None} ,this.street, this.locality, this.localitySimilarity, this.localityMatchedBy, this.streetSimilarity, this.streetSoftSimilarity, this.longitude , this.latitude)
    }

    def getBestIrisCode(that:ParsedAddress) = {
      if(!that.irisCode.isEmpty) that
      else this
    }
}
