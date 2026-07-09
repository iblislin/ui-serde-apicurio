package io.github.iblislin.uiserde.apicurio

import io.kafbat.ui.serde.api.DeserializeResult
import io.kafbat.ui.serde.api.PropertyResolver
import io.kafbat.ui.serde.api.RecordHeaders
import io.kafbat.ui.serde.api.SchemaDescription
import io.kafbat.ui.serde.api.Serde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/** kafka-ui (kafbat) serde that decodes Avro whose Apicurio schema id is carried
  * in the Kafka record HEADER (Apicurio `HeaderIdHandler`), not in the payload
  * magic-byte prefix.
  *
  * The built-in kafka-ui SchemaRegistry serde only understands the Confluent
  * payload envelope `[0x00][id 4B][avro]`. This serde instead reads
  * `apicurio.value.globalId` / `apicurio.key.globalId` from the record headers,
  * fetches the writer schema from the Apicurio registry, and Avro-decodes the
  * payload. Requires kafka-ui >= v1.5.0 (record headers are passed to the
  * deserializer — kafbat/kafka-ui#1768).
  *
  * No JSON library is used: Avro parses the `.avsc` and renders the decoded
  * record to JSON itself. `serde-api` is `provided` (supplied by kafka-ui).
  *
  * Config (per kafka-ui serde properties):
  *   kafka.clusters.N.serde.M.properties.registryUrl: http://host/apis/registry/v3
  */
class ApicurioHeaderSerde extends Serde:
  import ApicurioHeaderSerde.readGlobalId

  private var registryUrl: String = uninitialized
  private val http                = HttpClient.newHttpClient()
  // globalId -> parsed writer schema (schemas are immutable per id)
  private val schemaCache = ConcurrentHashMap[java.lang.Long, Schema]()

  override def configure(
      serdeProperties: PropertyResolver,
      kafkaClusterProperties: PropertyResolver,
      globalProperties: PropertyResolver
  ): Unit =
    registryUrl = serdeProperties
      .getProperty("registryUrl", classOf[String])
      .orElseThrow(() =>
        IllegalArgumentException(
          "ui-serde-apicurio: required property 'registryUrl' is missing " +
            "(e.g. http://10.18.28.95/apis/registry/v3)"
        )
      )
      .stripSuffix("/")

  override def getDescription(): Optional[String] =
    Optional.of("Apicurio header-mode Avro deserializer (schema id read from Kafka record headers).")

  override def getSchema(topic: String, target: Serde.Target): Optional[SchemaDescription] =
    // Schema is per-message (resolved from the header id at decode time), so we
    // expose no topic-level schema here.
    Optional.empty()

  override def canSerialize(topic: String, target: Serde.Target): Boolean = false

  override def serializer(topic: String, target: Serde.Target): Serde.Serializer =
    throw UnsupportedOperationException(
      "ui-serde-apicurio is deserialize-only (message viewing) for now"
    )

  override def canDeserialize(topic: String, target: Serde.Target): Boolean = true

  override def deserializer(topic: String, target: Serde.Target): Serde.Deserializer =
    val headerName = target match
      case Serde.Target.KEY   => "apicurio.key.globalId"
      case Serde.Target.VALUE => "apicurio.value.globalId"

    new Serde.Deserializer:
      override def deserialize(headers: RecordHeaders, data: Array[Byte]): DeserializeResult =
        val globalId = readGlobalId(headers, headerName).getOrElse {
          throw IllegalStateException(
            s"no '$headerName' header on this record — is the producer using " +
              "Apicurio header mode (apicurio.registry.headers.enabled=true)?"
          )
        }
        val schema  = schemaCache.computeIfAbsent(globalId, id => fetchSchema(id))
        val reader  = GenericDatumReader[GenericRecord](schema)
        val decoder = DecoderFactory.get().binaryDecoder(data, null)
        val record  = reader.read(null.asInstanceOf[GenericRecord], decoder)
        DeserializeResult(
          record.toString, // Avro's own JSON rendering
          DeserializeResult.Type.JSON,
          java.util.Map.of("globalId", globalId.toString, "schema", schema.getFullName)
        )

  private def fetchSchema(globalId: java.lang.Long): Schema =
    val req = HttpRequest
      .newBuilder(URI.create(s"$registryUrl/ids/globalIds/$globalId"))
      .GET()
      .build()
    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
    if resp.statusCode() != 200 then
      throw IllegalStateException(
        s"Apicurio registry GET /ids/globalIds/$globalId returned HTTP ${resp.statusCode()}"
      )
    Schema.Parser().parse(resp.body())

  override def close(): Unit = ()

object ApicurioHeaderSerde:

  /** Apicurio `HeaderIdHandler` writes the globalId as an 8-byte big-endian long
    * into the record header value. Returns the last header matching `name`.
    */
  private def readGlobalId(headers: RecordHeaders, name: String): Option[Long] =
    var found: Option[Long] = None
    val it                  = headers.iterator()
    while it.hasNext do
      val h = it.next()
      if h.key() == name then
        val v = h.value()
        if v != null && v.length == java.lang.Long.BYTES then
          found = Some(ByteBuffer.wrap(v).getLong)
    found
