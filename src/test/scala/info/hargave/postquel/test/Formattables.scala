package info.hargave.postquel.test

import java.sql.Timestamp
import java.util.{Calendar, TimeZone, UUID}

import org.scalatest.{FunSpec, Matchers}
import info.hargrave.postquel.{TimestampFormattable, UUIDFormattable}
import net.noerd.prequel.SQLFormatter
import org.postgresql.util.PGTimestamp

/**
  * Date: 5/6/17
  * Time: 4:16 PM
  */
class Formattables extends FunSpec with Matchers {

  val formatter = SQLFormatter.DefaultSQLFormatter

  val formats =
    List(
      ("UUIDFormattable should enclose the UUID in quotes",
        UUIDFormattable(new UUID(0, 0)),
        "'00000000-0000-0000-0000-000000000000'"),
      ("TimestampFormattable should format as ISO 8601 Standard (NULL TZ)",
        TimestampFormattable(new Timestamp(0)),
        "'1970-01-01 00:00:00.000000Z'"),
      ("TimestampFormattable should format as ISO 8601 Standard (With TZ)",
        TimestampFormattable(new PGTimestamp(0, Calendar.getInstance(TimeZone.getTimeZone("CST")))),
        "'1970-12-31 18:00:00.000000-0600'")
    )

  describe("Formattables") {
    formats.foreach {
      case (spec, formattable, expected) =>
        it (spec) {
          formattable.escaped(formatter) should equal(expected)
        }
    }
  }
}
