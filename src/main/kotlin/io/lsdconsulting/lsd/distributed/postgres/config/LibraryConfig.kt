package io.lsdconsulting.lsd.distributed.postgres.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.lsdconsulting.lsd.distributed.postgres.repository.InterceptedDocumentPostgresRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

@Configuration
@ConditionalOnProperty(name = ["lsd.dist.connectionString"])
open class LibraryConfig {
    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('postgresql://')}")
    open fun interceptedDocumentRepository(
        dataSource: DataSource, objectMapper: ObjectMapper,
        @Value("\${lsd.dist.db.failOnConnectionError:#{true}}") failOnConnectionError: Boolean,
    ) = InterceptedDocumentPostgresRepository(dataSource, objectMapper, failOnConnectionError)

    @Bean
    @ConditionalOnExpression("#{'\${lsd.dist.connectionString:}'.startsWith('postgresql://')}")
    @ConditionalOnMissingBean(value = [DataSource::class])
    open fun dataSource(
        @Value("\${lsd.dist.connectionString}") dbConnectionString: String
    ): DataSource {
        val config = HikariConfig()
        config.jdbcUrl = dbConnectionString;
        return HikariDataSource(config)
    }
}
