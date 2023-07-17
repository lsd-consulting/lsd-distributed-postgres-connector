package io.lsdconsulting.lsd.distributed.postgres.repository

fun String.trimToSize(size: Int) = this.substring(0, kotlin.math.min(this.length, size))
