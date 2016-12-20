package org.david_onoro

/**
  * Created by ddor on 20/12/2016.
  */
object Beans {

  case  class OlympicAthletes (
                                name: String,
                                yearFirstMedal:Int,
                                yearLastMedal:Int,
                                goldMedals:Int,
                                silverMedals: Int,
                                bronzeMedals:Int
                              )

  case class OlympicCountries(
                               name: String,
                               yearFirstMedal:Int,
                               yearLastMedal: Int,
                               avgGoldMedals: Int,
                               maxGoldMedals: Int,
                               totGoldMedals: Int,
                               avgSilverMedals: Int,
                               maxSilverMedals: Int,
                               totSilverMedals: Int,
                               avgBronzeMedals: Int,
                               maxBronzeMedals: Int,
                               totBronzeMedals: Int
                             )



  case class OlympicMedalRecords(
                                  name: String,
                                  age: Int,
                                  country: String,
                                  olympicGame: Int,
                                  sport: String,
                                  goldMedals: Int,
                                  silverMedals: Int,
                                  bronzeMedals: Int
                                )
}
