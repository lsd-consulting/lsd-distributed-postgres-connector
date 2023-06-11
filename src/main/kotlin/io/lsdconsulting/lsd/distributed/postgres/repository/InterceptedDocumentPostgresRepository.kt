package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.lsdconsulting.lsd.distributed.access.model.InteractionType
import io.lsdconsulting.lsd.distributed.access.model.InterceptedInteraction
import io.lsdconsulting.lsd.distributed.access.repository.InterceptedDocumentRepository
import io.lsdconsulting.lsd.distributed.postgres.config.log
import java.sql.ResultSet
import java.time.ZoneId
import java.time.ZonedDateTime
import javax.sql.DataSource


private const val QUERY_BY_TRACE_IDS_SORTED_BY_CREATED_AT = "select * from lsd.intercepted_interactions o where o.trace_id = ANY (?) order by o.created_at"
private const val QUERY_BY_TRACE_IDS = "select * from lsd.intercepted_interactions o where o.trace_id = ANY (?)"
private const val INSERT_QUERY = "insert into lsd.intercepted_interactions (trace_id, body, request_headers, response_headers, service_name, target, path, http_status, http_method, interaction_type, profile, elapsed_time, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

class InterceptedDocumentPostgresRepository(
    private val dataSource: DataSource,
    private val objectMapper: ObjectMapper,
    failOnConnectionError: Boolean = false
) : InterceptedDocumentRepository {

    private var active: Boolean = false

    init {
        if (failOnConnectionError) {
            dataSource.connection.use { con ->
                val prepareStatement = con.prepareStatement("SELECT 1")
                prepareStatement.use { pst ->
                    pst.executeQuery().use { rs ->
                        if (rs.next()) {
                            active = true
                        }
                    }
                }
            }
        } else active = true
    }

    override fun save(interceptedInteraction: InterceptedInteraction) {
        if (isActive()) {
            dataSource.connection.use { con ->
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

    private val typeReference = object : TypeReference<Map<String, Collection<String>>>() {}

    override fun findByTraceIds(vararg traceId: String): List<InterceptedInteraction> {
        if (isActive()) {
            val startTime = System.currentTimeMillis()
            val interceptedInteractions: MutableList<InterceptedInteraction> = mutableListOf()
            dataSource.connection.use { con ->
                val prepareStatement = con.prepareStatement(QUERY_BY_TRACE_IDS_SORTED_BY_CREATED_AT)
                prepareStatement.setArray(1, con.createArrayOf("text", traceId))
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
        return listOf()
    }

    fun findByTraceIdsUnsorted(vararg traceId: String): List<InterceptedInteraction> {
        if (isActive()) {
            val startTime = System.currentTimeMillis()
            val interceptedInteractions: MutableList<InterceptedInteraction> = mutableListOf()
            dataSource.connection.use { con ->
                val prepareStatement = con.prepareStatement(QUERY_BY_TRACE_IDS)
                prepareStatement.setArray(1, con.createArrayOf("text", traceId))
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
        return listOf()
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

    override fun isActive() = active.also {
        log().warn("The LSD Postgres repository is disabled!")
    }
}
