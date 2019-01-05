package io.theground.s3poller

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class S3PollerApplication

fun main(args: Array<String>) {
	runApplication<S3PollerApplication>(*args)
}

