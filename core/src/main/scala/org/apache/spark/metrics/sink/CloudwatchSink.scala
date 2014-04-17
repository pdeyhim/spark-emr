package org.apache.spark.metrics.sink


import java.util.Properties
import com.codahale.metrics.MetricRegistry
import com.plausiblelabs.metrics.reporting.CloudWatchReporter
import com.amazonaws.auth.BasicAWSCredentials
import java.util.concurrent.TimeUnit.SECONDS
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.AmazonClientException
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import org.apache.spark.Logging
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import org.apache.spark.SecurityManager


private[spark] class CloudwatchSink(val property: Properties, val registry: MetricRegistry,securityMgr: SecurityManager) extends Sink with Logging {

	println("started")
    val reporter = new CloudWatchReporter.Builder(registry,"spark",new AmazonCloudWatchClient(getCredentials())).withEC2InstanceIdDimension().build()
    val reporteras = new CloudWatchReporter.Builder(registry,"spark",new AmazonCloudWatchClient(getCredentials())).withInstanceIdDimension("spark-cluster").build()

  override def start() {
    reporter.start(60, SECONDS)
    reporteras.start(60, SECONDS)
  }

  override def stop() {
    reporter.stop()
    reporteras.stop()
  }

   private def getCredentials():AWSCredentialsProvider = {

                  var credentialsProvider:AWSCredentialsProvider=null
          try {
                    credentialsProvider = new InstanceProfileCredentialsProvider()
                    // Verify we can fetch credentials from the provider
                    credentialsProvider.getCredentials()
                    logInfo("Obtained credentials from the IMDS.")
                } catch  {
                  case e:AmazonClientException => {
                     credentialsProvider = new ClasspathPropertiesFileCredentialsProvider()
                    // Verify we can fetch credentials from the provider
                    credentialsProvider.getCredentials()
                    logInfo("Obtained credentials from the properties file.")
                  }
                }
               credentialsProvider
     }


}
