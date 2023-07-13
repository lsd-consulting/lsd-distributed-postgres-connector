package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedFlow
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.connector.repository.InterceptedDocumentAdminRepository
import io.lsdconsulting.lsd.distributed.postgres.config.log
import javax.sql.DataSource

private const val QUERY_BY_TRACE_IDS =
    "select * from lsd.intercepted_interactions o where o.trace_id = ANY (?)"
private const val QUERY_FOR_RECENT_UNIQUE_TRACE_IDS =
    "select m.trace_id from (select trace_id, max(created_at) c from lsd.intercepted_interactions group by trace_id order by c desc limit (?)) m limit (?)"

class InterceptedDocumentPostgresAdminRepository : InterceptedDocumentAdminRepository {
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

    override fun findRecentFlows(resultSizeLimit: Int): List<InterceptedFlow> {
        if (dataSource == null) {
            this.dataSource = createDataSource(config, failOnConnectionError)
        }
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