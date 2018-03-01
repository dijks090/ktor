package io.ktor.client.features.cookies

import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.features.*
import io.ktor.client.features.cookies.HttpCookies.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.util.*

/**
 * [HttpClient] feature that handles sent `Cookie`, and received `Set-Cookie` headers,
 * using a specific [storage] for storing and retrieving cookies.
 *
 * You can configure the [Config.storage] and to provide [Config.default] blocks to set
 * cookies when installing.
 */
class HttpCookies(private val storage: CookiesStorage) {

    suspend fun get(host: String): Map<String, Cookie>? = storage.get(host)

    suspend fun get(host: String, name: String): Cookie? = storage.get(host, name)

    suspend fun forEach(host: String, block: (Cookie) -> Unit) = storage.forEach(host, block)

    class Config {
        private val defaults = mutableListOf<suspend CookiesStorage.() -> Unit>()

        /**
         * [CookiesStorage] that will be used at this feature.
         * By default it just uses an initially empty in-memory [AcceptAllCookiesStorage].
         */
        var storage: CookiesStorage = AcceptAllCookiesStorage()

        /**
         * Registers a [block] that will be called when the configuration is complete the specified [storage].
         * The [block] can potentially add new cookies by calling [CookiesStorage.addCookie].
         */
        fun default(block: suspend CookiesStorage.() -> Unit) {
            defaults.add(block)
        }

        suspend fun build(): HttpCookies {
            defaults.forEach {
                it.invoke(storage)
            }

            return HttpCookies(storage)
        }
    }

    companion object Feature : HttpClientFeature<Config, HttpCookies> {
        override suspend fun prepare(block: Config.() -> Unit): HttpCookies = Config().apply(block).build()

        override val key: AttributeKey<HttpCookies> = AttributeKey("HttpCookies")

        override fun install(feature: HttpCookies, scope: HttpClient) {
            scope.sendPipeline.intercept(HttpSendChain.State) { execute, request ->
                val host = request.url.host.toLowerCase()

                val requestWithCookies = feature.get(host)?.let { cookies ->
                    HttpRequestBuilder().apply {
                        takeFrom(request)

                        headers {
                            cookies.forEach { (_, cookie) -> append(HttpHeaders.Cookie, renderSetCookieHeader(cookie)) }
                        }
                    }.build()
                } ?: request

                val call = execute(requestWithCookies)
                call.response.setCookie().forEach { feature.storage.addCookie(host, it) }

                return@intercept call
            }

        }
    }
}

/**
 * Gets all the cookies for the specified [host] for this [HttpClient].
 */
suspend fun HttpClient.cookies(host: String): Map<String, Cookie> = feature(HttpCookies)?.get(host) ?: mapOf()
