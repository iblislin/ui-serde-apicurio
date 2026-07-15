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
  * in the Kafka record HEADER (Apicurio's `DefaultHeadersHandler`, i.e. Apicurio
  * "header mode"), not in the payload magic-byte prefix.
  *
  * The built-in kafka-ui SchemaRegistry serde only understands the Confluent
  * payload envelope `[0x00][id 4B][avro]`. This serde instead reads the schema
  * id from the record headers, fetches the writer schema from the Apicurio
  * registry, and Avro-decodes the payload. Requires kafka-ui >= v1.5.0 (record
  * headers are passed to the deserializer — kafbat/kafka-ui#1768).
  *
  * Apicurio's `DefaultHeadersHandler.writeHeaders` writes EXACTLY ONE of two
  * headers per target, depending on the producer's `apicurio.registry.use-id`:
  *   - default (`contentId`, `USE_ID_DEFAULT`): `apicurio.{key,value}.contentId`
  *   - `use.id=globalId`:                       `apicurio.{key,value}.globalId`
  * Both header values are an 8-byte big-endian long (`ByteBuffer.putLong`).
  * contentId and globalId are INDEPENDENT id spaces, resolved via different
  * registry endpoints (`/ids/contentIds/{id}` vs `/ids/globalIds/{id}`), so this
  * serde auto-detects which header is present per record and routes accordingly.
  *
  * No JSON library is used: Avro parses the `.avsc` and renders the decoded
  * record to JSON itself. `serde-api` is `provided` (supplied by kafka-ui).
  *
  * Config (per kafka-ui serde properties):
  *   kafka.clusters.N.serde.M.properties.registryUrl: http://host/apis/registry/v3
  *   kafka.clusters.N.serde.M.properties.globalIdHeaderName.key: apicurio.key.globalId (optional override)
  *   kafka.clusters.N.serde.M.properties.globalIdHeaderName.value: apicurio.value.globalId (optional override)
  *   kafka.clusters.N.serde.M.properties.contentIdHeaderName.key: apicurio.key.contentId (optional override)
  *   kafka.clusters.N.serde.M.properties.contentIdHeaderName.value: apicurio.value.contentId (optional override)
  */
class ApicurioHeaderSerde extends Serde:
  import ApicurioHeaderSerde.IdKind
  import ApicurioHeaderSerde.findHeaderId
  import ApicurioHeaderSerde.resolveUrl

  private var registryUrl: String = null
  private val http                = HttpClient.newHttpClient()
  // resolve URL -> parsed writer schema (schemas are immutable per URL; the URL
  // already disambiguates the contentId/globalId id spaces).
  private val schemaCache = ConcurrentHashMap[String, Schema]()

  private var globalIdHeaderName: Map[Serde.Target, String]  = Map.empty
  private var contentIdHeaderName: Map[Serde.Target, String] = Map.empty

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

    def strProp(name: String, default: String): String =
      serdeProperties.getProperty(name, classOf[String]).orElse(default)

    globalIdHeaderName = Map(
      Serde.Target.KEY   -> strProp("globalIdHeaderName.key", "apicurio.key.globalId"),
      Serde.Target.VALUE -> strProp("globalIdHeaderName.value", "apicurio.value.globalId")
    )
    contentIdHeaderName = Map(
      Serde.Target.KEY   -> strProp("contentIdHeaderName.key", "apicurio.key.contentId"),
      Serde.Target.VALUE -> strProp("contentIdHeaderName.value", "apicurio.value.contentId")
    )

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
    val globalHeaderName  = globalIdHeaderName(target)
    val contentHeaderName = contentIdHeaderName(target)

    new Serde.Deserializer:
      override def deserialize(headers: RecordHeaders, data: Array[Byte]): DeserializeResult =
        // Apicurio's DefaultHeadersHandler writes exactly one of the two
        // headers depending on the producer's `use.id` setting. globalId is
        // checked first (matches Apicurio's own header-mode deserializer
        // precedence), falling back to contentId (the USE_ID_DEFAULT case).
        val (kind, id) = findHeaderId(headers, globalHeaderName)
          .map((IdKind.GlobalId, _))
          .orElse(findHeaderId(headers, contentHeaderName).map((IdKind.ContentId, _)))
          .getOrElse {
            throw IllegalStateException(
              s"neither '$globalHeaderName' nor '$contentHeaderName' header found on this " +
                "record — is the producer using Apicurio header mode " +
                "(apicurio.registry.headers.enabled=true)?"
            )
          }

        val url    = resolveUrl(registryUrl, kind, id)
        val schema = schemaCache.computeIfAbsent(url, u => fetchSchema(u))
        val reader  = GenericDatumReader[GenericRecord](schema)
        val decoder = DecoderFactory.get().binaryDecoder(data, null)
        val record  = reader.read(null.asInstanceOf[GenericRecord], decoder)
        DeserializeResult(
          record.toString, // Avro's own JSON rendering
          DeserializeResult.Type.JSON,
          java.util.Map.of(kind.propertyName, id.toString, "schema", schema.getFullName)
        )

  private def fetchSchema(schemaUrl: String): Schema =
    val req = HttpRequest
      .newBuilder(URI.create(schemaUrl))
      .GET()
      .build()
    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
    if resp.statusCode() != 200 then
      throw IllegalStateException(
        s"Apicurio registry GET $schemaUrl returned HTTP ${resp.statusCode()}"
      )
    Schema.Parser().parse(resp.body())

  override def close(): Unit = ()

object ApicurioHeaderSerde:

  /** The two independent id spaces Apicurio header mode may use, and how each
    * maps to a registry resolve endpoint / result metadata key.
    */
  enum IdKind(val propertyName: String, val urlSegment: String):
    case GlobalId  extends IdKind("globalId", "globalIds")
    case ContentId extends IdKind("contentId", "contentIds")

  private def resolveUrl(registryUrl: String, kind: IdKind, id: Long): String =
    s"$registryUrl/ids/${kind.urlSegment}/$id"

  /** Apicurio's `DefaultHeadersHandler` writes the id as an 8-byte big-endian
    * long into the record header value. Returns the last header matching
    * `name`, if any.
    */
  private def findHeaderId(headers: RecordHeaders, name: String): Option[Long] =
    var found: Option[Long] = None
    val it                  = headers.iterator()
    while it.hasNext do
      val h = it.next()
      if h.key() == name then
        val v = h.value()
        if v != null && v.length == java.lang.Long.BYTES then
          found = Some(ByteBuffer.wrap(v).getLong)
    found
