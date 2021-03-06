
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.rekognition.AmazonRekognition
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder
import com.amazonaws.services.rekognition.model.*
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.fasterxml.jackson.databind.ObjectMapper
import org.joda.time.format.ISODateTimeFormat
import spark.ModelAndView
import spark.Response
import spark.ResponseTransformer
import spark.Spark.get
import spark.template.thymeleaf.ThymeleafTemplateEngine
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


fun mainx() {
    val s3: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withRegion(Regions.EU_CENTRAL_1)
        .build()

    val years = setOf("2020")
    val basePath = Path.of("/Users/m/Dropbox/Camera Uploads")
    val files = Files.list(basePath)
    var uploadFiles = mutableListOf<String>()
    for (file in files) {
        val fname = file.fileName.toString()

        var valid = false
        for (year in years) {
            if (fname.startsWith(year) && (fname.endsWith("jpg") || fname.endsWith("png"))) {
                valid = true
                break
            }
        }
        if (!valid) {
            continue
        }

        uploadFiles.add(fname)
    }
    uploadFiles.sort()
    log("Number of files to upload: ${uploadFiles.size}")

    val pool = Executors.newFixedThreadPool(64)
    for (filename in uploadFiles) {
        pool.submit {
            log("???? $filename")
            val f = File(Paths.get(basePath.toString(), filename).toString())
            if (!f.exists()) {
                log("?????? Should not happen: $f can not be found")
            }
            val result = s3.putObject("com.mlesniak.photos", "photos/$filename", f)
            log("???? $filename")
        }
    }

    pool.shutdown()
    log("... awaiting pool termination")
    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)

    log("Finished")
}

fun log(message: String) {
    val now = System.currentTimeMillis()
    val f = ISODateTimeFormat.ordinalDateTime()
    println("${f.print(now)}\t$message")
}

// TODO(mlesniak) new table with all label in the futures?
fun identify() {
    val s3: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withRegion(Regions.EU_CENTRAL_1)
        .build()


    val bucket = "com.mlesniak.photos"
    val prefix = "photos/"

    var objects = mutableListOf<S3ObjectSummary>()

    var token: String = ""
    while (true) {
        var req = ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(prefix)
        if (token != "") {
            req = req.withContinuationToken(token)
        }

        val result: ListObjectsV2Result = s3.listObjectsV2(req)
        objects.addAll(result.objectSummaries)
        if (!result.isTruncated) {
            break
        }
        token = result.nextContinuationToken
    }


    val latch = CountDownLatch(objects.size)
    val pool = Executors.newFixedThreadPool(4)

    val ddb: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

    val om = ObjectMapper()
    val ai = AtomicInteger(0)
    for (os in objects) {
        pool.submit {
            val a = ai.incrementAndGet()
            println("* ${os.key} -> $a")
            var rs: List<Label>? = null
            var count = 1
            while (rs == null) {
                try {
                    rs = categorize(bucket, os.key)
                } catch (e: InvalidImageFormatException) {
                    log("?????? Illegal image format for ${os.key}, aborting")
                    rs = listOf()
                    break
                } catch (e: AmazonRekognitionException) {
                    log("???? Waiting for retry ($count)")
                    Thread.sleep(5000 + (Math.random() * 1000).toLong())
                    count++
                }
                println("* ${os.key} -> DONE")
                if (rs == null) {

                } else {
                    if (count > 1) {
                        log("???? Image processed, awesome!")
                    }
                }
            }

            val s = om.writeValueAsString(rs)
            for (label in rs!!) {
                val m = mapOf(
                    "label" to AttributeValue(label.name),
                    "filename" to AttributeValue(os.key),
                    "confidence" to AttributeValue().withN(label.confidence.toString()),
                    "json" to AttributeValue(s)
                )
                ddb.putItem("photos", m)
            }

            latch.countDown()
        }
    }

    // val now = System.currentTimeMillis()
    // val scanRequest = ScanRequest()
    //     .withTableName("labels")
    //     .withExpressionAttributeNames(mapOf("#v" to "name"))
    //     .withExpressionAttributeValues(mapOf(":k" to AttributeValue("Text")))
    //     .withFilterExpression("#v = :k")
    // //     .withSelect()
    // //     .withIndexName("Text")
    // //     // .withExpressionAttributeValues(mapOf(":tag" to AttributeValue("Nature")))
    // //     // .withFilterExpression("name = :tag")
    // val res = ddb.scan(scanRequest)
    // val dur = System.currentTimeMillis() - now
    // println(dur)
    // for (map in res.items) {
    //     println("$map")
    // }

    // val req = QueryRequest()
    //     .withTableName("labels")
    //     .withKeyConditionExpression("namex = :n")
    //     .withExpressionAttributeValues(mapOf(":n" to AttributeValue("Nature")))
    //     // .withExpressionAttributeNames()
    // for (map in res.items) {
    //     println("$map")
    // }
    // val res = ddb.getItem("labels", mapOf("name" to AttributeValue("Text"), "filename" to AttributeValue("test/2021-01-01 15.56.07.jpg")))
    // println("${res.item}")

    println("Waiting...")
    pool.shutdown()
    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)
    println("done")
}

fun categorize(bucket: String, name: String): List<Label>? {
    val rekognitionClient: AmazonRekognition = AmazonRekognitionClientBuilder.defaultClient()
    val request: DetectLabelsRequest = DetectLabelsRequest()
        .withImage(Image().withS3Object(S3Object().withName(name).withBucket(bucket)))
        .withMaxLabels(10).withMinConfidence(75f)
    val result: DetectLabelsResult = rekognitionClient.detectLabels(request)
    return result.labels
    // println("* $name")
    // for (label in labels) {
    //     println("  Label: ${label.name} / Confidence: ${label.confidence}")
    // }

}

fun render(model: Map<String?, Any?>?, templatePath: String?): String? {
    return ThymeleafTemplateEngine().render(ModelAndView(model, templatePath))
}

fun main() {
    val ddb: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

    // staticFiles.location("/public")

    get("/") { req, res ->
        var model = mutableMapOf<String?, Any?>()
        model.put("date", Date())
        model.put("labels", (labels() as Map<String, Integer>).keys)

        val slabels = req.queryParamOrDefault("q", "")
        if (slabels != "") {
            var all = getImageURLs(slabels, ddb)
            model.put("urls", all)
        }

        render(model, "index");
    }

// declare this in a util-class
//     public static String render(Map<String, Object> model, String templatePath) {
//         return new VelocityTemplateEngine().render(new ModelAndView(model, templatePath));
//     }

    get("/labels", { req, res ->
        getLabels(res)
        // Files.readString(Path.of("labels.json"))

        // var labels = mutableMapOf<String, Int>()
        // var last: MutableMap<String, AttributeValue>? = null
        //
        // var global = 0
        // while (true) {
        //     var sr = ScanRequest()
        //         .withTableName("photos")
        //         .withAttributesToGet("label")
        //         .withLimit(Int.MAX_VALUE)
        //     if (last != null) {
        //         sr = sr.withExclusiveStartKey(last)
        //     }
        //
        //     val res = ddb.scan(sr)
        //     println("LAST = ${last}")
        //     res.items.map { item ->
        //         val av = item["label"]!!
        //         val count = labels.getOrDefault(av.s, 0)
        //         labels.put(av.s, count+1)
        //         global++
        //     }
        //
        //     if (res.lastEvaluatedKey != null) {
        //         last = res.lastEvaluatedKey
        //     } else {
        //         break
        //     }
        // }
        // println(global)
        //
        // // for once, then load it instead of query
        // val ls = ObjectMapper().writeValueAsString(labels)
        // Files.writeString(Path.of("labels.json"), ls)
        //
        // res.type("application/json")
        // labels
    }, JsonTransformer())

    get("/labels/query", { req, res ->
        val slabels = req.queryParamOrDefault("q", "")
        var all = getImageURLs(slabels, ddb)

        res.type("application/json")
        all
    }, JsonTransformer())
}

private fun getImageURLs(slabels: String, ddb: AmazonDynamoDB): MutableSet<String> {
    val labels = slabels.split(",")

    var all = mutableSetOf<String>()

    for (queryLabel in labels) {
        var filenames = mutableSetOf<String>()
        var last: MutableMap<String, AttributeValue>? = null
        while (true) {
            val cond = Condition().withAttributeValueList(AttributeValue(queryLabel))
                .withComparisonOperator(ComparisonOperator.EQ)
            var sr = QueryRequest()
                .withTableName("photos")
                .withKeyConditions(mapOf("label" to cond))

            if (last != null) {
                sr = sr.withExclusiveStartKey(last)
            }

            val res = ddb.query(sr)
            res.items.map { item ->
                val av = item["filename"]!!
                val s = av.s.replace(" ", "+")
                val ff = "https://s3.eu-central-1.amazonaws.com/com.mlesniak.photos/${s}"
                filenames.add(ff)
            }
            if (res.lastEvaluatedKey != null) {
                last = res.lastEvaluatedKey
            } else {
                break
            }
        }

        all =
            if (all.isEmpty()) {
                filenames
            } else {
                all.intersect(filenames).toMutableSet()
            }
    }
    return all
}

private fun getLabels(res: Response): Object? {
    res.type("application/json")
    return labels()
}

private fun labels(): Object? {
    val om = ObjectMapper()
    return om.readValue<Object>(File("labels.json"), Object::class.java)
}

class JsonTransformer : ResponseTransformer {
    private val om = ObjectMapper()
    override fun render(model: Any): String {
        return om.writeValueAsString(model)
    }
}