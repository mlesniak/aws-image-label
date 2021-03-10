import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.Bucket
import com.amazonaws.services.s3.model.ListObjectsV2Result


fun main() {
    val s3: AmazonS3 = AmazonS3ClientBuilder
        .standard()
        .withRegion(Regions.EU_CENTRAL_1)
        .build()

    val buckets: List<Bucket> = s3.listBuckets()
    println("Your Amazon S3 buckets are:")
    for (b in buckets) {
        println("* ${b.name}")
    }

    val result: ListObjectsV2Result = s3.listObjectsV2("com.mlesniak.photos", "test/")
    val objects = result.objectSummaries
    for (os in objects) {
        println("* " + os.key)
    }
}
