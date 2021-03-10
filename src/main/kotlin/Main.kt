
import com.amazonaws.regions.Regions
import com.amazonaws.services.rekognition.AmazonRekognition
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder
import com.amazonaws.services.rekognition.model.*
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


fun main() {
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

    val m = ConcurrentHashMap<String, List<Label>>()

    val ai = AtomicInteger(0)
    for (os in objects) {
        pool.submit {
            val a = ai.incrementAndGet()
            println("* ${os.key} ->$a")
            m.put(os.key, categorize(bucket, os.key))
            latch.countDown()
        }
    }

    println("Waiting...")
    pool.shutdown()
    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)
    println("done")

    println("size=${m.size}")
    val om = ObjectMapper()
    for ((k, o) in m) {
        val json = om.writeValueAsString(o)
        println("$k -> $json")
        s3.putObject(bucket, "test.json/$k.json", json)
    }
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