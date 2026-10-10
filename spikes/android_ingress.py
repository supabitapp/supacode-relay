import pathlib
import shutil
import sys

module = pathlib.Path(sys.argv[1]).resolve()
out = pathlib.Path(sys.argv[2]).resolve()
fixtures = pathlib.Path(__file__).resolve().parent / 'android'
shutil.copytree(module / 'android' / 'jvm', out / 'android' / 'jvm', dirs_exist_ok=True,
                ignore=shutil.ignore_patterns('build', '.gradle'))
core = pathlib.Path('android/src/main/java/expo/modules/supacoderelaytunnel/core')
shutil.copytree(module / core, out / core, dirs_exist_ok=True)
p = out / core / 'NativeTunnel.kt'
source = p.read_text()
source = source.replace('const val BACKGROUND_GRACE_MILLIS = 15_000L',
    'const val BACKGROUND_GRACE_MILLIS = 15_000L\n    val spikeMessages = LinkedBlockingQueue<Int>()')
source = source.replace('private val actor = Executors.newSingleThreadScheduledExecutor {',
    'private val actor = java.util.concurrent.ScheduledThreadPoolExecutor(1) {')
source = source.replace('private val io = Executors.newCachedThreadPool {',
    'private val spikeQueuedBytes = java.util.concurrent.atomic.AtomicInteger()\n  private val io = Executors.newCachedThreadPool {')
needle = '''        override fun onMessage(socket: WebSocket, bytes: ByteString) {
          post {'''
replacement = '''        override fun onMessage(socket: WebSocket, bytes: ByteString) {
          spikeMessages.add(bytes.size)
          val bounded = java.lang.Boolean.getBoolean("spike.bounded")
          if (bounded && (bytes.size > 65560 || spikeQueuedBytes.addAndGet(bytes.size) > 524288)) {
            if (bytes.size <= 65560) spikeQueuedBytes.addAndGet(-bytes.size)
            socket.cancel()
            post { fail(future, IllegalStateException("Relay ingress overflow")) }
            return
          }
          post {'''
assert needle in source
source = source.replace(needle, replacement)
needle = '(mux ?: error("Binary handshake")).receive(bytes.toByteArray())'
assert needle in source
source = source.replace(needle, '''try { (mux ?: error("Binary handshake")).receive(bytes.toByteArray()) }
              finally { if (bounded) spikeQueuedBytes.addAndGet(-bytes.size) }''')
p.write_text(source)
tests = out / 'android/jvm/src/test/kotlin/expo/modules/supacoderelaytunnel/core'
shutil.copy(fixtures / 'IngressSpikeTest.kt', tests / 'IngressSpikeTest.kt')
print(out / 'android/jvm')
