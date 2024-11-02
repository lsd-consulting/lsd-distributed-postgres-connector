package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.connector.repository.InterceptedDocumentRepository
import lsd.logging.log
import org.postgresql.util.PSQLException
import javax.sql.DataSource


private const val QUERY_BY_TRACE_IDS_LIMIT_100 =
    "select * from intercepted_interactions o where o.trace_id = ANY (?) limit 100"
private const val INSERT_QUERY =
    "insert into intercepted_interactions (trace_id, body, request_headers, response_headers, service_name, target, path, http_status, http_method, interaction_type, profile, elapsed_time, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

private const val DEFAULT_CONNECTION_TIMEOUT_MILLIS = 500L
private const val DRIVER_CLASS_NAME = "org.postgresql.Driver"

class InterceptedDocumentPostgresRepository : InterceptedDocumentRepository {
    private var active: Boolean = true
    private val dataSource: DataSource?
    private val objectMapper: ObjectMapper
    private val traceIdMaxLength: Int
    private val bodyMaxLength: Int
    private val requestHeadersMaxLength: Int
    private val responseHeadersMaxLength: Int
    private val serviceNameMaxLength: Int
    private val targetMaxLength: Int
    private val pathMaxLength: Int
    private val httpStatusMaxLength: Int
    private val httpMethodMaxLength: Int
    private val profileMaxLength: Int

    constructor(
        dataSource: DataSource,
        objectMapper: ObjectMapper,
        traceIdMaxLength: Int,
        bodyMaxLength: Int,
        requestHeadersMaxLength: Int,
        responseHeadersMaxLength: Int,
        serviceNameMaxLength: Int,
        targetMaxLength: Int,
        pathMaxLength: Int,
        httpStatusMaxLength: Int,
        httpMethodMaxLength: Int,
        profileMaxLength: Int,
    ) {
        this.dataSource = dataSource
        this.objectMapper = objectMapper
        this.traceIdMaxLength = traceIdMaxLength
        this.bodyMaxLength = bodyMaxLength
        this.requestHeadersMaxLength = requestHeadersMaxLength
        this.responseHeadersMaxLength = responseHeadersMaxLength
        this.serviceNameMaxLength = serviceNameMaxLength
        this.targetMaxLength = targetMaxLength
        this.pathMaxLength = pathMaxLength
        this.httpStatusMaxLength = httpStatusMaxLength
        this.httpMethodMaxLength = httpMethodMaxLength
        this.profileMaxLength = profileMaxLength
    }

    constructor(
        dbConnectionString: String,
        objectMapper: ObjectMapper,
        failOnConnectionError: Boolean = false,
        connectionTimeout: Long = DEFAULT_CONNECTION_TIMEOUT_MILLIS,
        traceIdMaxLength: Int,
        bodyMaxLength: Int,
        requestHeadersMaxLength: Int,
        responseHeadersMaxLength: Int,
        serviceNameMaxLength: Int,
        targetMaxLength: Int,
        pathMaxLength: Int,
        httpStatusMaxLength: Int,
        httpMethodMaxLength: Int,
        profileMaxLength: Int,
    ) {
        val config = HikariConfig()
        config.initializationFailTimeout = connectionTimeout
        config.jdbcUrl = dbConnectionString
        config.driverClassName = DRIVER_CLASS_NAME
        this.dataSource = createDataSource(config, failOnConnectionError)
        this.objectMapper = objectMapper
        this.traceIdMaxLength = traceIdMaxLength
        this.bodyMaxLength = bodyMaxLength
        this.requestHeadersMaxLength = requestHeadersMaxLength
        this.responseHeadersMaxLength = responseHeadersMaxLength
        this.serviceNameMaxLength = serviceNameMaxLength
        this.targetMaxLength = targetMaxLength
        this.pathMaxLength = pathMaxLength
        this.httpStatusMaxLength = httpStatusMaxLength
        this.httpMethodMaxLength = httpMethodMaxLength
        this.profileMaxLength = profileMaxLength
    }

    private fun createDataSource(config: HikariConfig, failOnConnectionError: Boolean): DataSource? = try {
        HikariDataSource(config)
    } catch (e: HikariPool.PoolInitializationException) {
        if (failOnConnectionError) {
            throw e
        }
        active = false
        null
    }

    override fun save(interceptedInteraction: InterceptedInteraction) {
        if (isActive()) {
            try {
                dataSource!!.connection.use { con ->
                    con.prepareStatement(INSERT_QUERY).use { pst ->
                        pst.setString(1, interceptedInteraction.traceId.trimToSize(traceIdMaxLength))
                        pst.setString(2, interceptedInteraction.body?.trimToSize(bodyMaxLength))
                        pst.setString(3, objectMapper.writeValueAsString(interceptedInteraction.requestHeaders).trimToSize(requestHeadersMaxLength))
                        pst.setString(4, objectMapper.writeValueAsString(interceptedInteraction.responseHeaders).trimToSize(responseHeadersMaxLength))
                        pst.setString(5, interceptedInteraction.serviceName.trimToSize(serviceNameMaxLength))
                        pst.setString(6, interceptedInteraction.target.trimToSize(targetMaxLength))
                        pst.setString(7, interceptedInteraction.path.trimToSize(pathMaxLength))
                        pst.setString(8, interceptedInteraction.httpStatus?.trimToSize(httpStatusMaxLength))
                        pst.setString(9, interceptedInteraction.httpMethod?.trimToSize(httpMethodMaxLength))
                        pst.setString(10, interceptedInteraction.interactionType.name)
                        pst.setString(11, interceptedInteraction.profile?.trimToSize(profileMaxLength))
                        pst.setLong(12, interceptedInteraction.elapsedTime)
                        pst.setObject(13, interceptedInteraction.createdAt.toOffsetDateTime())
                        pst.executeUpdate()
                    }
                }
            } catch (e: PSQLException) {
                log().error(
                    "Skipping persisting the interceptedInteraction due to exception - interceptedInteraction:{}, message:{}, stackTrace:{}",
                    interceptedInteraction,
                    e.message,
                    e.stackTrace
                )
            }
        }
    }

    override fun findByTraceIds(vararg traceId: String): List<InterceptedInteraction> {
        if (isActive()) {
            val startTime = System.currentTimeMillis()
            val interceptedInteractions: MutableList<InterceptedInteraction> = mutableListOf()
            try {
                dataSource!!.connection.use { con ->
                    val prepareStatement = con.prepareStatement(QUERY_BY_TRACE_IDS_LIMIT_100)
                    prepareStatement.setArray(1, con.createArrayOf("text", traceId))
                    prepareStatement.use { pst ->
                        pst.executeQuery().use { rs ->
                            while (rs.next()) {
                                val interceptedInteraction = rs.toInterceptedInteraction(objectMapper)
                                interceptedInteractions.add(interceptedInteraction)
                            }
                        }
                    }
                }
            } catch (e: PSQLException) {
                log().error("Failed to retrieve interceptedInteractions - message:${e.message}", e.stackTrace)
            }
            log().trace("findByTraceIds took {} ms", System.currentTimeMillis() - startTime)
            interceptedInteractions.sortBy { it.createdAt }
            return interceptedInteractions
        }
        return listOf()
    }

    override fun isActive() = active.also {
        if (!it) log().warn("The LSD Postgres repository is disabled!")
    }
}
