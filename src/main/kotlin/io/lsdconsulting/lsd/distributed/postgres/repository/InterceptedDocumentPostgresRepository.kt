package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.connector.repository.InterceptedDocumentRepository
import io.lsdconsulting.lsd.distributed.postgres.config.log
import javax.sql.DataSource


private const val QUERY_BY_TRACE_IDS_SORTED_BY_CREATED_AT =
    "select * from lsd.intercepted_interactions o where o.trace_id = ANY (?) order by o.created_at"
private const val INSERT_QUERY =
    "insert into lsd.intercepted_interactions (trace_id, body, request_headers, response_headers, service_name, target, path, http_status, http_method, interaction_type, profile, elapsed_time, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

class InterceptedDocumentPostgresRepository : InterceptedDocumentRepository {
    private var active: Boolean = true
    private var dataSource: DataSource?
    private lateinit var config: HikariConfig
    private var objectMapper: ObjectMapper
    private val failOnConnectionError: Boolean

    constructor(
        dataSource: DataSource,
        objectMapper: ObjectMapper
    ) {
        this.dataSource = dataSource
        this.objectMapper = objectMapper
        this.failOnConnectionError = false
    }

    constructor(dbConnectionString: String, objectMapper: ObjectMapper, failOnConnectionError: Boolean = false) {
        config = HikariConfig()
        config.jdbcUrl = dbConnectionString
        config.driverClassName = "org.postgresql.Driver"
        this.dataSource = null
        this.objectMapper = objectMapper
        this.failOnConnectionError = failOnConnectionError

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
        if (dataSource == null) {
            this.dataSource = createDataSource(config, failOnConnectionError)
        }
        if (isActive()) {
            dataSource!!.connection.use { con ->
                con.prepareStatement(INSERT_QUERY).use { pst ->
                    pst.setString(1, interceptedInteraction.traceId)
                    pst.setString(2, interceptedInteraction.body)
                    pst.setString(3, objectMapper.writeValueAsString(interceptedInteraction.requestHeaders))
                    pst.setString(4, objectMapper.writeValueAsString(interceptedInteraction.responseHeaders))
                    pst.setString(5, interceptedInteraction.serviceName)
                    pst.setString(6, interceptedInteraction.target)
                    pst.setString(7, interceptedInteraction.path)
                    pst.setString(8, interceptedInteraction.httpStatus)
                    pst.setString(9, interceptedInteraction.httpMethod)
                    pst.setString(10, interceptedInteraction.interactionType.name)
                    pst.setString(11, interceptedInteraction.profile)
                    pst.setLong(12, interceptedInteraction.elapsedTime)
                    pst.setObject(13, interceptedInteraction.createdAt.toOffsetDateTime())
                    pst.executeUpdate()
                }
            }
        }
    }

    override fun findByTraceIds(vararg traceId: String): List<InterceptedInteraction> {
        if (dataSource == null) {
            this.dataSource = createDataSource(config, failOnConnectionError)
        }
        if (isActive()) {
            val startTime = System.currentTimeMillis()
            val interceptedInteractions: MutableList<InterceptedInteraction> = mutableListOf()
            dataSource!!.connection.use { con ->
                val prepareStatement = con.prepareStatement(QUERY_BY_TRACE_IDS_SORTED_BY_CREATED_AT)
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
            log().trace("findByTraceIds took {} ms", System.currentTimeMillis() - startTime)
            return interceptedInteractions
        }
        return listOf()
    }

    override fun isActive() = active.also {
        if (!it) log().warn("The LSD Postgres repository is disabled!")
    }
}
