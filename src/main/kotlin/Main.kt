
import com.amazonaws.regions.Regions
import com.amazonaws.services.rekognition.AmazonRekognition
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder
import com.amazonaws.services.rekognition.model.*
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Result


fun main() {
    val s3: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withRegion(Regions.EU_CENTRAL_1)
        .build()

    // val buckets: List<Bucket> = s3.listBuckets()
    // println("Your Amazon S3 buckets are:")
    // for (b in buckets) {
    //     println("* ${b.name}")
    // }

    val bucket = "com.mlesniak.photos"
    val prefix = "test/"
    val result: ListObjectsV2Result = s3.listObjectsV2(bucket, prefix)
    val objects = result.objectSummaries
    for (os in objects) {
        println("* " + os.key)
        categorize(bucket, os.key)
    }
}

fun categorize(bucket: String, name: String) {

    val rekognitionClient: AmazonRekognition = AmazonRekognitionClientBuilder.defaultClient()
    val request: DetectLabelsRequest = DetectLabelsRequest()
        .withImage(Image().withS3Object(S3Object().withName(name).withBucket(bucket)))
        .withMaxLabels(10).withMinConfidence(75f)
    try {
        val result: DetectLabelsResult = rekognitionClient.detectLabels(request)
        val labels: List<Label> = result.getLabels()
        println("Detected labels for $name\n")
        for (label in labels) {
            System.out.println("Label: " + label.getName())
            System.out.println(
                """
                    Confidence: ${label.getConfidence().toString().toString()}
                    
                    """.trimIndent()
            )
            // val instances: List<Instance> = label.getInstances()
            // System.out.println("Instances of " + label.getName())
            // if (instances.isEmpty()) {
            //     println("  " + "None")
            // } else {
            //     for (instance in instances) {
            //         System.out.println("  Confidence: " + instance.getConfidence().toString())
            //         System.out.println("  Bounding box: " + instance.getBoundingBox().toString())
            //     }
            // }
            // System.out.println("Parent labels for " + label.getName().toString() + ":")
            // val parents: List<Parent> = label.getParents()
            // if (parents.isEmpty()) {
            //     println("  None")
            // } else {
            //     for (parent in parents) {
            //         System.out.println("  " + parent.getName())
            //     }
            // }
            println("--------------------")
        }
    } catch (e: AmazonRekognitionException) {
        e.printStackTrace()
    }
}