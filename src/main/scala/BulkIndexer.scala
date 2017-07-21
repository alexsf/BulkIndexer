import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import io.circe._, io.circe.parser._
import scalaj.http._
import scala.util.Random

object BulkIndexer {

    def goIndex(payload:String):HttpResponse[String] = {
        val urls = Seq("http://hermione-idx02.hpeswlab.net:9200", "http://hermione-idx03.hpeswlab.net:9200", "http://hermione-idx04.hpeswlab.net:9200")
        val random = new Random
        val url = urls(random.nextInt(urls.length))

        val req:HttpRequest = Http(url + "/_bulk")
        val res = req.postData(payload)
                .header("Content-Type", "application/x-ndjson")
                .header("Charset", "UTF-8")
                .option(HttpOptions.readTimeout(6000000))
                .option(HttpOptions.connTimeout(6000000)).asString

        res
    }

    def main(args:Array[String]):Unit = {

        var argmap = scala.collection.mutable.Map[String,String]()
        var c = 0
        for (arg <- args) {
            if ((c%2) == 0)
                argmap(arg) = args(c+1)
            c = c + 1
        }        

        print("\n\n\n" + argmap.getOrElse("--index-name", "--index-name=?") + "\n")
        
        print(argmap.getOrElse("--index-type",        "--index-type=?")        +     "\n")
        print(argmap.getOrElse("--text-file",         "--text-file=?")         +     "\n")
        print(argmap.getOrElse("--number-of-batches", "--number-of-batches=?") + "\n\n\n")

        val conf = new SparkConf().setAppName("BulkIndexer Application")
        val sc:SparkContext = new SparkContext(conf)
    
        var indexName    = argmap("--index-name")
        var indexType    = argmap("--index-type")
        var textFileName = argmap("--text-file") 
        var nBatches     = argmap("--number-of-batches")

        // load file and partition RDD
        var rdd1 = sc.textFile(textFileName, nBatches.toInt)
       
        // RDD[DOC_ID, DOC_STRING] 
        var rdd2 = rdd1.map(d=>({
            val parseResult = parse(d)
            var cursor = parseResult.right.getOrElse(Json.Null).hcursor
            var id = cursor.downField("ID").as[String].right.get
            id 
        },d))

        // RDD[DOC_ID, HEADER+DOC_STRING]
        var rdd3 = rdd2.map(kv=>{
            val k = kv._1
            (k, s"""{"index": {"_index": "$indexName", "_type": "$indexType", "_id": "$k"}}\n""" + kv._2)
        })

        // break bulk indexing in nBatches
        var rdd4 = rdd3.mapPartitionsWithIndex { (partid, iterator) => List((partid, iterator.toList.map(t=>t._2).mkString("\n"))).iterator }


        // RDD[BATCH_ID, CONCATENATED_DOC_STRINGS]
        var rdd5 = rdd4 //rdd4.reduceByKey(_ + "\n" + _)


        // POST EACH BATCH TO _BULK
        // RDD(200, "ok") or RDD(HTTP_ERROR_CODE, "error")
        rdd5.map(kv=>{
            // post kv._2 to _bulk
            var payload = if (kv._2.endsWith("\n")) kv._2 else kv._2 + "\n"
            var response = goIndex(payload)
            var status = response.code
            (status, if (status >= 200 && status < 300) ((kv._2.split("\n").length) / 2) else response.body)
        }).collect().foreach(println)

        sc.stop()
    }
}
