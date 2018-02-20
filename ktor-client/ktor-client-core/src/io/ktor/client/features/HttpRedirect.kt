package io.ktor.client.features

import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.util.*

/**
 * [HttpClient] feature that handles http redirect
 */
class HttpRedirect(
        val maxJumps: Int
) {

    class Config {
        var maxJumps: Int = 20
    }

    companion object Feature : HttpClientFeature<Config, HttpRedirect> {
        override val key: AttributeKey<HttpRedirect> = AttributeKey("HttpRedirect")

        override suspend fun prepare(block: Config.() -> Unit): HttpRedirect =
                HttpRedirect(Config().apply(block).maxJumps)

        override fun install(feature: HttpRedirect, scope: HttpClient) {
            scope.sendPipeline.intercept(HttpSendChain.Validation) { execute, requestData ->
                var request = requestData
                repeat(feature.maxJumps) {
                    val call = execute(request)
                    if (!call.response.status.isRedirect()) return@intercept call
                    val location = call.response.headers[HttpHeaders.Location]
                            ?: throw RedirectException(request, "Redirect location is missing")

                    request = HttpRequestBuilder().apply {
                        takeFrom(requestData)
                        url.takeFrom(location)
                    }.build()
                }

                throw RedirectException(request, "Redirect limit ${feature.maxJumps} exceeded")
            }
        }
    }
}

private fun HttpStatusCode.isRedirect(): Boolean = when (value) {
    HttpStatusCode.MovedPermanently.value,
    HttpStatusCode.Found.value,
    HttpStatusCode.TemporaryRedirect.value,
    HttpStatusCode.PermanentRedirect.value -> true
    else -> false
}

class RedirectException(val request: HttpRequestData, cause: String) : IllegalStateException(cause)
