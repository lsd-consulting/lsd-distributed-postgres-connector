package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedFlow
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.connector.repository.InterceptedDocumentAdminRepository
import lsd.logging.log
import javax.sql.DataSource

private const val QUERY_BY_TRACE_IDS =
    "select * from intercepted_interactions o where o.trace_id = ANY (?)"
private const val QUERY_FOR_RECENT_UNIQUE_TRACE_IDS =
    "select m.trace_id from (select trace_id, max(created_at) c from intercepted_interactions group by trace_id order by c desc limit (?)) m limit (?)"

private const val DEFAULT_CONNECTION_TIMEOUT_MILLIS = 1500L

class InterceptedDocumentPostgresAdminRepository : InterceptedDocumentAdminRepository {
    private var dataSource: DataSource
    private var objectMapper: ObjectMapper

    constructor(
        dataSource: DataSource,
        objectMapper: ObjectMapper
    ) {
        this.dataSource = dataSource
        this.objectMapper = objectMapper
    }

    constructor(
        dbConnectionString: String,
        objectMapper: ObjectMapper,
        connectionTimeout: Long = DEFAULT_CONNECTION_TIMEOUT_MILLIS
    ) {
        val config = HikariConfig()
        config.initializationFailTimeout = connectionTimeout
        config.jdbcUrl = dbConnectionString
        config.driverClassName = "org.postgresql.Driver"
        this.dataSource = HikariDataSource(config)
        this.objectMapper = objectMapper
    }

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
        dataSource.connection.use { con ->
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
        dataSource.connection.use { con ->
            val prepareStatement = con.prepareStatement(QUERY_BY_TRACE_IDS)
            prepareStatement.setArray(1, con.createArrayOf("text", traceId.toTypedArray()))
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
}
