package io.ktor.tests.utils

import io.ktor.pipeline.*
import kotlinx.coroutines.experimental.*
import kotlin.test.*

class ChainPipelineTest {
    private val rounds = 10

    private val interceptor: suspend (suspend (Request) -> Response, Request) -> Response =
            block@{ execute: suspend (Request) -> Response, request: Request ->
                request.counter++
                val response = execute(request)
                response.counter++

                return@block response
            }

    @Test
    fun singlePhaseTest() = runBlocking {
        val firstPhase = PipelinePhase("1")
        val chain = ChainPipeline(firstPhase) { (id, counter): Request -> Response(id, counter, 0) }

        repeat(rounds) { round ->
            chain.intercept(firstPhase, interceptor)
            val request = Request(round, 0)
            val response = chain.execute(request)

            assertEquals(Request(round, round + 1), request)
            assertEquals(Response(round, round + 1, round + 1), response)
        }
    }

    @Test
    fun multiPhaseTest() = runBlocking {
        val phases = (0..10).map { PipelinePhase(it.toString()) }.toTypedArray()
        val chain = ChainPipeline(*phases) { (id, counter): Request -> Response(id, counter, 0) }

        repeat(rounds) { round ->
            repeat(phases.size) { phase ->
                chain.intercept(phases[phase], interceptor)
                val id = round * phases.size + phase
                val request = Request(id, 0)
                val response = chain.execute(request)

                assertEquals(Request(id, id + 1), request)
                assertEquals(Response(id, id + 1, id + 1), response)
            }
        }
    }

    @Test
    fun multiPhaseOrderTest() = runBlocking {
        val phases = (0..10).map { PipelinePhase(it.toString()) }.toTypedArray()
        val chain = ChainPipeline(*phases) { (id, counter): Request -> Response(id, counter, 0) }

        repeat(rounds) { round ->
            repeat(phases.size) { phase ->
                val round = round
                val phase = phase
                chain.intercept(phases[phase]) { execute: suspend (Request) -> Response, request: Request ->
                    val request = request

                    val interceptorIndex = phase * rounds + (rounds - round - 1)
                    assertEquals(interceptorIndex, request.counter, "round: $round, phase: $phase" )
                    request.counter++
                    val response = execute(request)
                    assertEquals(phases.size * rounds - interceptorIndex - 1, response.counter, "round: $round, phase: $phase")
                    response.counter++
                    response
                }
            }
        }

        chain.execute(Request(0, 0))
        Unit
    }
}

private data class Request(val id: Int, var counter: Int)

private data class Response(val id: Int, var requestCounter: Int, var counter: Int)

