
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.rekognition.AmazonRekognition
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder
import com.amazonaws.services.rekognition.model.*
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.fasterxml.jackson.databind.ObjectMapper
import org.joda.time.format.ISODateTimeFormat
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


fun main() {
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
            log("🌍 $filename")
            val f = File(Paths.get(basePath.toString(), filename).toString())
            if (!f.exists()) {
                log("⚡️ Should not happen: $f can not be found")
            }
            val result = s3.putObject("com.mlesniak.photos", "photos/$filename", f)
            log("🥳 $filename")
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

fun mainx() {
    val s3: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withRegion(Regions.EU_CENTRAL_1)
        .build()


    val bucket = "com.mlesniak.photos"
    val prefix = "test/"
    val result: ListObjectsV2Result = s3.listObjectsV2(bucket, prefix)
    val objects = result.objectSummaries

    val latch = CountDownLatch(objects.size)
    val pool = Executors.newFixedThreadPool(20)

    val ddb: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

    val om = ObjectMapper()
    val ai = AtomicInteger(0)
    for (os in objects) {
        pool.submit {
            val a = ai.incrementAndGet()
            println("* ${os.key} ->$a")
            val rs = categorize(bucket, os.key)
            // val s = om.writeValueAsString(rs)
            // s3.putObject(bucket, "json/${os.key}.json", s)

            for (label in rs) {
                val m = mapOf(
                    "name" to AttributeValue(label.name),
                    "filename" to AttributeValue(os.key)
                )
                ddb.putItem("labels", m)
            }

            latch.countDown()
        }
    }

    val now = System.currentTimeMillis()
    val scanRequest = ScanRequest()
        .withTableName("labels")
        .withExpressionAttributeNames(mapOf("#v" to "name"))
        .withExpressionAttributeValues(mapOf(":k" to AttributeValue("Text")))
        .withFilterExpression("#v = :k")
    //     .withSelect()
    //     .withIndexName("Text")
    //     // .withExpressionAttributeValues(mapOf(":tag" to AttributeValue("Nature")))
    //     // .withFilterExpression("name = :tag")
    val res = ddb.scan(scanRequest)
    val dur = System.currentTimeMillis() - now
    println(dur)
    for (map in res.items) {
        println("$map")
    }

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

fun categorize(bucket: String, name: String): List<Label> {
    val rekognitionClient: AmazonRekognition = AmazonRekognitionClientBuilder.defaultClient()
    val request: DetectLabelsRequest = DetectLabelsRequest()
        .withImage(Image().withS3Object(S3Object().withName(name).withBucket(bucket)))
        .withMaxLabels(10).withMinConfidence(75f)
    try {
        val result: DetectLabelsResult = rekognitionClient.detectLabels(request)
        return result.labels
        // println("* $name")
        // for (label in labels) {
        //     println("  Label: ${label.name} / Confidence: ${label.confidence}")
        // }
    } catch (e: AmazonRekognitionException) {
        e.printStackTrace()
    }

    return listOf()
}