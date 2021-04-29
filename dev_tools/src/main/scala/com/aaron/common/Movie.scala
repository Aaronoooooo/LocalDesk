package com.aaron.common
import java.util

case class Movie(
                  id: Long,
                  name: String,
                  doubanScore: Float,
                  actorList: util.List[util.Map[String, Any]]
                )
