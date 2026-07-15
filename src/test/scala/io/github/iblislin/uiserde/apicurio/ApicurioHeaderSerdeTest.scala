package io.github.iblislin.uiserde.apicurio

import com.sun.net.httpserver.HttpServer
import io.kafbat.ui.serde.api.PropertyResolver
import io.kafbat.ui.serde.api.RecordHeader
import io.kafbat.ui.serde.api.RecordHeaders
import io.kafbat.ui.serde.api.Serde
import munit.FunSuite
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/** Hermetic end-to-end test: no network, no external services. A JDK built-in
  * [[com.sun.net.httpserver.HttpServer]] stands in for the Apicurio registry,
  * and real Avro bytes are encoded in-test — this exercises the actual
  * deserialize path (header auto-detection, endpoint routing, schema fetch +
  * decode), not mocks.
  */
class ApicurioHeaderSerdeTest extends FunSuite:

  private val schemaJson =
    """{"type":"record","name":"HeaderTest","namespace":"iot262","fields":[
      |{"name":"id","type":"long"},
      |{"name":"note","type":["null","string"],"default":null}
      |]}""".stripMargin

  private val avroSchema = Schema.Parser().parse(schemaJson)

  private def longBytes(v: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(v).array()

  private def encodeRecord(id: Long, note: String): Array[Byte] =
    val record = GenericData.Record(avroSchema)
    record.put("id", id)
    record.put("note", note)
    val out     = ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer  = GenericDatumWriter[GenericRecord](avroSchema)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray

  /** Test double for the SPI's record header carrier. */
  private final case class TestHeader(key: String, value: Array[Byte]) extends RecordHeader

  private final case class TestHeaders(headers: Seq[RecordHeader]) extends RecordHeaders:
    override def iterator(): java.util.Iterator[RecordHeader] = headers.asJava.iterator()

  /** Test double for the SPI's serde-properties config source: a plain map,
    * `Optional.empty()` for anything not present (so plugin defaults apply).
    */
  private final case class MapPropertyResolver(props: Map[String, String]) extends PropertyResolver:
    override def getProperty[T](name: String, clazz: Class[T]): Optional[T] =
      props.get(name).map(_.asInstanceOf[T]).fold(Optional.empty[T]())(Optional.of)
    override def getListProperty[T](name: String, clazz: Class[T]): Optional[java.util.List[T]] =
      Optional.empty()
    override def getMapProperty[K, V](
        name: String,
        kClazz: Class[K],
        vClazz: Class[V]
    ): Optional[java.util.Map[K, V]] = Optional.empty()

  /** A fake Apicurio registry serving the same `.avsc` at both
    * `/ids/contentIds/{id}` and `/ids/globalIds/{id}`, with independent hit
    * counters so tests can assert which endpoint kind was actually routed to.
    */
  private final case class FakeRegistry(server: HttpServer, port: Int, contentHits: AtomicInteger, globalHits: AtomicInteger):
    def registryUrl: String = s"http://127.0.0.1:$port"
    def stop(): Unit        = server.stop(0)

  private def startFakeRegistry(): FakeRegistry =
    val server      = HttpServer.create(InetSocketAddress(0), 0)
    val contentHits = AtomicInteger(0)
    val globalHits  = AtomicInteger(0)
    val body        = schemaJson.getBytes(StandardCharsets.UTF_8)

    def serveSchema(hits: AtomicInteger): com.sun.net.httpserver.HttpHandler =
      exchange =>
        hits.incrementAndGet()
        exchange.sendResponseHeaders(200, body.length.toLong)
        val os = exchange.getResponseBody
        try os.write(body)
        finally os.close()

    server.createContext("/ids/contentIds/", serveSchema(contentHits))
    server.createContext("/ids/globalIds/", serveSchema(globalHits))
    server.setExecutor(null)
    server.start()
    FakeRegistry(server, server.getAddress.getPort, contentHits, globalHits)

  private def newSerde(registryUrl: String): ApicurioHeaderSerde =
    val serde = ApicurioHeaderSerde()
    serde.configure(
      MapPropertyResolver(Map("registryUrl" -> registryUrl)),
      MapPropertyResolver(Map.empty),
      MapPropertyResolver(Map.empty)
    )
    serde

  test("contentId header (the Apicurio default) decodes and hits /ids/contentIds") {
    val registry = startFakeRegistry()
    try
      val serde         = newSerde(registry.registryUrl)
      val deserializer  = serde.deserializer("test-topic", Serde.Target.VALUE)
      val payload       = encodeRecord(42L, "hello")
      val headers       = TestHeaders(Seq(TestHeader("apicurio.value.contentId", longBytes(7L))))
      val result        = deserializer.deserialize(headers, payload)

      assert(result.getResult.contains("\"id\""), s"expected decoded field 'id' in ${result.getResult}")
      assert(result.getResult.contains("42"), s"expected decoded value 42 in ${result.getResult}")
      assertEquals(result.getAdditionalProperties.get("contentId"), "7")
      assertEquals(result.getAdditionalProperties.get("schema"), "iot262.HeaderTest")
      assertEquals(registry.contentHits.get(), 1)
      assertEquals(registry.globalHits.get(), 0)
    finally registry.stop()
  }

  test("globalId header (use.id=globalId) decodes and hits /ids/globalIds") {
    val registry = startFakeRegistry()
    try
      val serde        = newSerde(registry.registryUrl)
      val deserializer = serde.deserializer("test-topic", Serde.Target.VALUE)
      val payload      = encodeRecord(99L, "world")
      val headers      = TestHeaders(Seq(TestHeader("apicurio.value.globalId", longBytes(13L))))
      val result       = deserializer.deserialize(headers, payload)

      assert(result.getResult.contains("99"), s"expected decoded value 99 in ${result.getResult}")
      assertEquals(result.getAdditionalProperties.get("globalId"), "13")
      assertEquals(registry.globalHits.get(), 1)
      assertEquals(registry.contentHits.get(), 0)
    finally registry.stop()
  }

  test("globalId takes precedence when both headers are present") {
    val registry = startFakeRegistry()
    try
      val serde        = newSerde(registry.registryUrl)
      val deserializer = serde.deserializer("test-topic", Serde.Target.VALUE)
      val payload      = encodeRecord(1L, "both")
      val headers = TestHeaders(
        Seq(
          TestHeader("apicurio.value.contentId", longBytes(5L)),
          TestHeader("apicurio.value.globalId", longBytes(6L))
        )
      )
      val result = deserializer.deserialize(headers, payload)

      assertEquals(result.getAdditionalProperties.get("globalId"), "6")
      assertEquals(registry.globalHits.get(), 1)
      assertEquals(registry.contentHits.get(), 0)
    finally registry.stop()
  }

  test("KEY target uses the key header names") {
    val registry = startFakeRegistry()
    try
      val serde        = newSerde(registry.registryUrl)
      val deserializer = serde.deserializer("test-topic", Serde.Target.KEY)
      val payload      = encodeRecord(2L, "key")
      val headers      = TestHeaders(Seq(TestHeader("apicurio.key.contentId", longBytes(9L))))
      val result       = deserializer.deserialize(headers, payload)

      assertEquals(result.getAdditionalProperties.get("contentId"), "9")
      assertEquals(registry.contentHits.get(), 1)
    finally registry.stop()
  }

  test("neither header present throws, naming both expected header names") {
    // No registry needed — should fail before any HTTP call is attempted.
    val serde        = newSerde("http://127.0.0.1:1")
    val deserializer = serde.deserializer("test-topic", Serde.Target.VALUE)
    val headers      = TestHeaders(Seq.empty)

    val ex = intercept[IllegalStateException] {
      deserializer.deserialize(headers, Array.emptyByteArray)
    }
    assert(ex.getMessage.contains("apicurio.value.globalId"), ex.getMessage)
    assert(ex.getMessage.contains("apicurio.value.contentId"), ex.getMessage)
  }

  test("8-byte big-endian long round-trips (Apicurio header encoding)") {
    val v     = 123456789012345L
    val bytes = longBytes(v)
    assertEquals(bytes.length, 8)
    assertEquals(ByteBuffer.wrap(bytes).getLong, v)
  }
