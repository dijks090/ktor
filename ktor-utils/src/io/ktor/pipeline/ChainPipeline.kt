package io.ktor.pipeline

/**
 * Represents an execution pipeline for asynchronous extensible computations.
 * Unlike [Pipeline], it allows you to control the call of the next interceptor
 */
open class ChainPipeline<In, Out>(vararg phases: PipelinePhase, block: suspend (In) -> Out) {
    private val chains: List<ChainPhase<In, Out>>

    init {
        check(phases.isNotEmpty()) { "Execute chain should't be empty" }

        val state = mutableListOf(ChainPhase(phases.last(), block))
        for (id in phases.lastIndex - 1 downTo 0) {
            val last = state.last()
            state.add(ChainPhase(phases[id]) { last.execute(it) })
        }

        chains = state.asReversed()
    }

    /**
     * Adds [block] to the [phase] of this pipeline
     */
    fun intercept(phase: PipelinePhase, block: suspend (suspend (In) -> Out, In) -> Out) {
        val chain = chains.find { it.phase == phase }
                ?: throw InvalidPhaseException("Phase $phase was not registered for this ExecuteChain")

        chain.intercept(block)
    }

    /**
     * Executes this pipeline and with the given [input]
     */
    suspend fun execute(input: In): Out = chains.first().execute(input)
}

internal class ChainPhase<In, Out>(val phase: PipelinePhase, base: suspend (In) -> Out) {
    private var chain: suspend (In) -> Out = base

    fun intercept(block: suspend (suspend (In) -> Out, In) -> Out): Unit {
        val parent = chain
        chain = { input -> block(parent, input) }
    }

    suspend fun execute(input: In): Out = chain(input)
}
