package io.ktor.server.netty.http1

import io.ktor.http.*
import io.ktor.pipeline.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.netty.buffer.*
import io.netty.channel.*
import io.netty.handler.codec.http.*
import io.netty.util.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import kotlinx.coroutines.experimental.io.*

private val ApplicationCallKey = AttributeKey.newInstance<SendChannel<NettyApplicationCall>>("ktor.ApplicationCall")!!

fun ChannelHandlerContext.sendCall(call: NettyApplicationCall) {
    channel().attr(ApplicationCallKey).get().offer(call)
}

fun ChannelHandlerContext.startLoop(enginePipeline: EnginePipeline) {
    channel().attr(ApplicationCallKey).set(dummyNettyLoop(this, enginePipeline))
}

fun ChannelHandlerContext.stopLoop() {
    channel().attr(ApplicationCallKey).get().close()
}

fun dummyNettyLoop(context: ChannelHandlerContext, enginePipeline: EnginePipeline): SendChannel<NettyApplicationCall> {
    val callExecutor = actor<NettyApplicationCall>(Unconfined, start = CoroutineStart.UNDISPATCHED, capacity = 0) {
        consumeEach {
            enginePipeline.execute(it)
        }
    }

    return actor(context = context.executor().asCoroutineDispatcher(),
            start = CoroutineStart.UNDISPATCHED,
            capacity = kotlinx.coroutines.experimental.channels.Channel.UNLIMITED) {
        try {
            while (true) {
                val polled = poll()
                if (polled == null) {
                    context.read()
                    context.flush()
                }

                val call = polled ?: receiveOrNull() ?: break
                callExecutor.send(call)
                processCall1(call)
            }
        } finally {
            callExecutor.close()
        }
    }
}

private fun processCall0(call: NettyApplicationCall): ChannelHandlerContext {
    val context = call.context

    try {
        call.response.status(HttpStatusCode.OK)
        call.response.headers.append("Content-Length", HelloWorldSize, false)
        call.response.headers.append("Content-Type", "text/plain", false)
        call.response.sendResponse(false, ByteReadChannel(HelloWorldBytes))

        context.write(call.response.responseMessage.getCompleted())
        context.write(DefaultLastHttpContent(Unpooled.wrappedBuffer(HelloWorldBytes)))
    } finally {
        call.responseWriteJob.cancel()
    }

    return context
}

private suspend fun processCall1(call: NettyApplicationCall) {
    val context = call.context

    try {
        val message = call.response.responseMessage.await()

        context.write(message)

        if (message !is FullHttpResponse) {
            call.processResponseBody(message, context)
        }
    } finally {
        call.responseWriteJob.cancel()
    }
}

private suspend fun NettyApplicationCall.processResponseBody(message: Any, context: ChannelHandlerContext) {
    val alloc = context.alloc()!!
    val channel = response.responseChannel

    val knownSize = (message as? HttpResponse)?.headers()?.getInt("Content-Length", -1) ?: -1

    when {
        knownSize == 0 -> context.write(DefaultLastHttpContent.EMPTY_LAST_CONTENT)
        knownSize in 1..8192 -> {
            val buffer = alloc.buffer(knownSize)!!
            channel.readFully(buffer.nioBuffer(buffer.writerIndex(), buffer.writableBytes()))
            buffer.writerIndex(buffer.writerIndex() + knownSize)

            context.write(DefaultLastHttpContent(buffer))
        }
        knownSize > 0 -> {
            var remaining = knownSize

            while (remaining > 0) {
                val buffer = alloc.buffer(8192)!!
                val rc = channel.readAvailable(buffer.nioBuffer(buffer.writerIndex(), buffer.writableBytes()))
                if (rc == -1) throw IllegalStateException("Unexpected EOF")
                buffer.writerIndex(buffer.writerIndex() + rc)
                remaining -= rc

                context.write(if (remaining > 0) DefaultHttpContent(buffer) else DefaultLastHttpContent(buffer))
            }
        }
    }
}

private val HelloWorldBytes = "Hello, World!".toByteArray()
private val HelloWorldSize = HelloWorldBytes.size.toString()
