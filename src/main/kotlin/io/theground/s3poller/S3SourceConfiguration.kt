package io.theground.s3poller

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration
import org.springframework.cloud.aws.core.env.ResourceIdResolver
import org.springframework.cloud.aws.core.region.RegionProvider
import org.springframework.cloud.stream.app.file.FileConsumerProperties
import org.springframework.cloud.stream.app.file.FileUtils
import org.springframework.cloud.stream.app.s3.AmazonS3Configuration
import org.springframework.cloud.stream.app.s3.source.AmazonS3SourceProperties
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizer
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizingMessageSource
import org.springframework.integration.aws.support.S3SessionFactory
import org.springframework.integration.aws.support.filters.S3RegexPatternFileListFilter
import org.springframework.integration.aws.support.filters.S3SimplePatternFileListFilter
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.util.StringUtils


@Configuration
@EnableConfigurationProperties(AmazonS3SourceProperties::class, FileConsumerProperties::class,
        TriggerPropertiesMaxMessagesDefaultUnlimited::class)
@Import(TriggerConfiguration::class)
@EnableAutoConfiguration(exclude = [ContextStackAutoConfiguration::class])
class S3SourceConfiguration {

    @Autowired
    private val s3SourceProperties: AmazonS3SourceProperties? = null

    @Value("\${cloud.aws.credentials.access-key}")
    private val accessKey: String? = null

    @Value("\${cloud.aws.credentials.secret-key}")
    private val secretKey: String? = null

    @Value("\${cloud.aws.region.static}")
    private val region: String? = null

    @Bean
    fun basicAWSCredentials(): BasicAWSCredentials {
        return BasicAWSCredentials(accessKey, secretKey)
    }

    @Bean
    fun amazonS3(awsCredentials: AWSCredentials ): AmazonS3 {
        val amazonS3Client = AmazonS3Client(awsCredentials)
        amazonS3Client.setRegion(Region.getRegion(Regions.fromName(region)))
        return amazonS3Client
    }

    @Bean
    fun s3InboundFileSynchronizer(amazonS3: AmazonS3,
                                  resourceIdResolver: ResourceIdResolver): S3InboundFileSynchronizer {
        val s3SessionFactory = S3SessionFactory(amazonS3, resourceIdResolver)
        val synchronizer = S3InboundFileSynchronizer(s3SessionFactory)
        synchronizer.setDeleteRemoteFiles(this.s3SourceProperties!!.isDeleteRemoteFiles)
        synchronizer.setPreserveTimestamp(this.s3SourceProperties.isPreserveTimestamp)
        val remoteDir = this.s3SourceProperties.remoteDir
        synchronizer.setRemoteDirectory(remoteDir)
        synchronizer.setRemoteFileSeparator(this.s3SourceProperties.remoteFileSeparator)
        synchronizer.setTemporaryFileSuffix(this.s3SourceProperties.tmpFileSuffix)

        if (StringUtils.hasText(this.s3SourceProperties.filenamePattern)) {
            synchronizer.setFilter(S3SimplePatternFileListFilter(this.s3SourceProperties.filenamePattern))
        } else if (this.s3SourceProperties.filenameRegex != null) {
            synchronizer.setFilter(S3RegexPatternFileListFilter(this.s3SourceProperties.filenameRegex))
        }

        return synchronizer
    }

    @Bean
    fun s3InboundFlow(fileConsumerProperties: FileConsumerProperties,
                      s3InboundFileSynchronizer: S3InboundFileSynchronizer): IntegrationFlow {
        val s3MessageSource = S3InboundFileSynchronizingMessageSource(s3InboundFileSynchronizer)
        s3MessageSource.setLocalDirectory(this.s3SourceProperties!!.localDir)
        s3MessageSource.setAutoCreateLocalDirectory(this.s3SourceProperties.isAutoCreateLocalDir)

        return FileUtils.enhanceFlowForReadingMode(IntegrationFlows.from(s3MessageSource), fileConsumerProperties)
                .channel(Source.OUTPUT)
                .get()
    }

}