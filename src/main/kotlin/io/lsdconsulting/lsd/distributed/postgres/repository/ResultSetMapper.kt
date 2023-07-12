package io.lsdconsulting.lsd.distributed.postgres.repository

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.lsdconsulting.lsd.distributed.connector.model.InteractionType
import io.lsdconsulting.lsd.distributed.connector.model.InterceptedInteraction
import java.sql.ResultSet
import java.time.ZoneId
import java.time.ZonedDateTime

private val typeReference = object : TypeReference<Map<String, Collection<String>>>() {}

fun ResultSet.toInterceptedInteraction(objectMapper: ObjectMapper) = InterceptedInteraction(
    traceId = this.getString("trace_id"),
    body = this.getString("body"),
    requestHeaders = objectMapper.readValue(this.getString("request_headers"), typeReference),
    responseHeaders = objectMapper.readValue(this.getString("response_headers"), typeReference),
    serviceName = this.getString("service_name"),
    target = this.getString("target"),
    path = this.getString("path"),
    httpStatus = this.getString("http_status"),
    httpMethod = this.getString("http_method"),
    interactionType = InteractionType.valueOf(this.getString("interaction_type")),
    profile = this.getString("profile"),
    elapsedTime = this.getLong("elapsed_time"),
    createdAt = ZonedDateTime.parse(this.getString("created_at").replace(" ", "T"))
        .withZoneSameInstant(ZoneId.of("UTC")),
)
