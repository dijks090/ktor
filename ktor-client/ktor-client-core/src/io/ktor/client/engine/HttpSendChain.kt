package io.ktor.client.engine

import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.pipeline.*

class HttpSendChain(
        block: suspend (HttpRequestData) -> HttpClientCall
) : ChainPipeline<HttpRequestData, HttpClientCall>(Validation, State, block = block) {

    companion object Phases {
        val State = PipelinePhase("State")

        val Validation = PipelinePhase("Validation")
    }
}
