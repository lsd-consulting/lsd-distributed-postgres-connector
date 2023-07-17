package io.lsdconsulting.lsd.distributed.postgres.repository

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.core.Is.`is`
import org.junit.jupiter.api.Test

class TrimmerShould {

    @Test
    fun `handle empty strings`() {
        assertThat("".trimToSize(0), `is`(""))
    }

    @Test
    fun `handle trim size grater than length on empty string`() {
        assertThat("".trimToSize(5), `is`(""))
    }

    @Test
    fun `handle trim size grater than length on non-empty string`() {
        assertThat("123".trimToSize(5), `is`("123"))
    }

    @Test
    fun `handle trim size smaller than string length`() {
        assertThat("123".trimToSize(2), `is`("12"))
    }
}
