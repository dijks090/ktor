package io.ktor.client.features

import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.content.*

fun HttpClient.defaultTransformers() {
    requestPipeline.intercept(HttpRequestPipeline.Render) { body ->
        when (body) {
            is ByteArray -> proceedWith(ByteArrayContent(body))
        }
    }
}
