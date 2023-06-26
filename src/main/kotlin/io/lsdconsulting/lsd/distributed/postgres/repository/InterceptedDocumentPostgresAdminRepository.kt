package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedFlow
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.connector.repository.InterceptedDocumentAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.config.log
import java.sql.ResultSet
import java.time.ZoneId
import java.time.ZonedDateTime
import javax.sql.DataSource

private const val QUERY_BY_TRACE_IDS =
    "select * from lsd.intercepted_interactions o where o.trace_id = ANY (?)"
private const val QUERY_FOR_RECENT_UNIQUE_TRACE_IDS =
    "select m.trace_id from (select trace_id, max(created_at) c from lsd.intercepted_interactions group by trace_id order by c desc limit (?)) m limit (?)"

class InterceptedDocumentPostgresAdminRepository : InterceptedDocumentAdminRepository {
    private var active: Boolean = true
    private var dataSource: DataSource?
    private var objectMapper: ObjectMapper

    constructor(
        dataSource: DataSource,
        objectMapper: ObjectMapper
    ) {
        this.dataSource = dataSource
        this.objectMapper = objectMapper
    }

    constructor(dbConnectionString: String, objectMapper: ObjectMapper, failOnConnectionError: Boolean = false) {
        val config = HikariConfig()
        config.jdbcUrl = dbConnectionString
        config.driverClassName = "org.postgresql.Driver"
        this.dataSource = createDataSource(config, failOnConnectionError)
        this.objectMapper = objectMapper
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

    private val typeReference = object : TypeReference<Map<String, Collection<String>>>() {}

    override fun findRecentFlows(resultSizeLimit: Int): List<InterceptedFlow> {
        val distinctTraceIds = findRecentTraceIds(resultSizeLimit)
        val interactionsGroupedByTraceId = findByTraceIdsUnsorted(distinctTraceIds).groupBy { it.traceId }

        return distinctTraceIds
            .map {
                interactionsGroupedByTraceId[it]!! // This is necessary to ensure the order set in distinctTraceIds
            }.map {
                InterceptedFlow(
                    initialInteraction = it.minBy { x -> x.createdAt },
                    finalInteraction = it.maxBy { x -> x.createdAt },
                    totalCapturedInteractions = it.size
                )
            }
    }

    private fun findRecentTraceIds(resultSizeLimit: Int): MutableList<String> {
        val traceIds: MutableList<String> = mutableListOf()
        dataSource!!.connection.use { con ->
            val prepareStatement = con.prepareStatement(QUERY_FOR_RECENT_UNIQUE_TRACE_IDS)
            prepareStatement.setInt(1, resultSizeLimit * 1000)
            prepareStatement.setInt(2, resultSizeLimit)
            prepareStatement.use { pst ->
                pst.executeQuery().use { rs ->
                    while (rs.next()) {
                        traceIds.add(rs.getString("trace_id"))
                    }
                }
            }
        }
        return traceIds
    }

    private fun findByTraceIdsUnsorted(traceId: List<String>): List<InterceptedInteraction> {
            val startTime = System.currentTimeMillis()
            val interceptedInteractions: MutableList<InterceptedInteraction> = mutableListOf()
            dataSource!!.connection.use { con ->
                val prepareStatement = con.prepareStatement(QUERY_BY_TRACE_IDS)
                prepareStatement.setArray(1, con.createArrayOf("text", traceId.toTypedArray()))
                prepareStatement.use { pst ->
                    pst.executeQuery().use { rs ->
                        while (rs.next()) {
                            val interceptedInteraction = mapResult(rs)
                            interceptedInteractions.add(interceptedInteraction)
                        }
                    }
                }
            }
            log().trace("findByTraceIds took {} ms", System.currentTimeMillis() - startTime)
            return interceptedInteractions
    }

    private fun mapResult(rs: ResultSet): InterceptedInteraction = InterceptedInteraction(
        traceId = rs.getString("trace_id"),
        body = rs.getString("body"),
        requestHeaders = objectMapper.readValue(rs.getString("request_headers"), typeReference),
        responseHeaders = objectMapper.readValue(rs.getString("response_headers"), typeReference),
        serviceName = rs.getString("service_name"),
        target = rs.getString("target"),
        path = rs.getString("path"),
        httpStatus = rs.getString("http_status"),
        httpMethod = rs.getString("http_method"),
        interactionType = InteractionType.valueOf(rs.getString("interaction_type")),
        profile = rs.getString("profile"),
        elapsedTime = rs.getLong("elapsed_time"),
        createdAt = ZonedDateTime.parse(rs.getString("created_at").replace(" ", "T"))
            .withZoneSameInstant(ZoneId.of("UTC")),
    )
}