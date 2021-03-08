package ddos_detecter

object Args {

  def nextOption (map : Map[String,String], list: List[String]): Map[String,String] = {
    list match {
      case Nil => map
      case "--srcPath" :: value :: tail => nextOption (map ++ Map ("srcPath" -> value), tail)
      case option::tail => println("unknown option")
      sys.exit(1)
    }

  }
}
