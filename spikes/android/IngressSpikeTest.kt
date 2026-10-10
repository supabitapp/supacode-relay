package expo.modules.supacoderelaytunnel.core

import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.ByteString.Companion.toByteString
import org.json.JSONObject
import org.junit.Test

class IngressSpikeTest {
  @Test
  fun measureCiphertextHandoffWhileActorIsStalled() {
    val address = JSONObject(File("fixtures/vectors.json").readText()).getString("address")
    for (bounded in listOf(false, true)) {
      System.setProperty("spike.bounded", bounded.toString())
      NativeTunnel.spikeMessages.clear()
      val opened = CountDownLatch(1)
      var sender: WebSocket? = null
      MockWebServer().use { server ->
        server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
          override fun onOpen(socket: WebSocket, response: Response) {
            sender = socket
            opened.countDown()
          }
        }))
        NativeTunnel(server.url("/").toString().replaceFirst("http", "ws"), address).use { tunnel ->
          assertTrue(opened.await(10, TimeUnit.SECONDS))
          val field = NativeTunnel::class.java.getDeclaredField("actor").apply { isAccessible = true }
          val actor = field.get(tunnel) as ScheduledThreadPoolExecutor
          val paused = CountDownLatch(1)
          val release = CountDownLatch(1)
          actor.execute { paused.countDown(); release.await(20, TimeUnit.SECONDS) }
          assertTrue(paused.await(10, TimeUnit.SECONDS))
          try {
            val payload = ByteArray(65560) { (it % 251).toByte() }.toByteString()
            val target = if (bounded) 8 else 128
            repeat(target) {
              assertTrue(sender!!.send(payload))
              assertTrue(NativeTunnel.spikeMessages.poll(10, TimeUnit.SECONDS) == payload.size)
            }
            val queued = actor.queue.size
            val bytesField=NativeTunnel::class.java.getDeclaredField("spikeQueuedBytes").apply { isAccessible=true }
            val queuedBytes=(bytesField.get(tunnel) as AtomicInteger).get()
            println("INGRESS_SPIKE bounded=$bounded received=$target queued_tasks=$queued payload_bytes=${if (bounded) queuedBytes else 128 * payload.size}")
            if (bounded) { assertTrue(queued <= 12);assertTrue(queuedBytes <= 524288) } else assertTrue(queued >= 127)
          } finally { release.countDown() }
          actor.submit {}.get(10,TimeUnit.SECONDS)
          if (bounded) {
            val count=NativeTunnel::class.java.getDeclaredField("spikeQueuedBytes").apply { isAccessible=true }
            assertTrue((count.get(tunnel) as AtomicInteger).get()==0)
            println("INGRESS_SPIKE retired_session_budget_bytes=0")
          }
        }
      }
    }
    System.clearProperty("spike.bounded")
  }
}
