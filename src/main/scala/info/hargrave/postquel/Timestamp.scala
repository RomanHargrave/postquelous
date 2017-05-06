package info.hargrave.postquel

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.{Calendar, GregorianCalendar, SimpleTimeZone, TimeZone}

import net.noerd.prequel._
import org.postgresql.util.PGTimestamp

/**
  * Date: 5/6/17
  * Time: 1:29 PM
  */
class TimestampColumnType(val row: ResultSetRow)
  extends ColumnType[Timestamp] {
  override def nextValueOption: Option[Timestamp] =
    Option(row.nextObject).map(_.asInstanceOf[Timestamp])

  override def columnValueOption(columnName: String): Option[Timestamp] =
    row.columnObject(columnName).map(_.asInstanceOf[Timestamp])
}

object TimestampColumnType
  extends ColumnTypeFactory[Timestamp] {
  def apply(row: ResultSetRow) = new TimestampColumnType(row)
}

object TimestampFormattable {
  private def formatTimestamp(ts: Timestamp): String = {
    val formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSSSSSXX")
    ts match {
      case pgts: PGTimestamp =>
        formatter.setCalendar(pgts.getCalendar)
        formatter.format(pgts)
      case sqlts: Timestamp =>
        // formatter will default to the local TZ if we don't bring our own
        formatter.setCalendar(Calendar.getInstance(TimeZone.getTimeZone("UTC")))
        formatter.format(sqlts)
    }
  }

  def apply(ts: Timestamp): TimestampFormattable = new TimestampFormattable(ts)
}

class TimestampFormattable(override val value: Timestamp)
  extends Formattable {

  override def escaped(formatter: SQLFormatter): String =
    s"'${TimestampFormattable.formatTimestamp(value)}'"

  override def addTo(statement: ReusableStatement): Unit =
    statement.addObject(value)
}

trait TimestampTypeImplicits {
  implicit def ts2fmt(ts: Timestamp): Formattable = new TimestampFormattable(ts)

  implicit def row2tsOption(row: ResultSetRow): Option[Timestamp] =
    TimestampColumnType(row).nextValueOption

  implicit def row2ts(row: ResultSetRow): Timestamp =
    TimestampColumnType(row).nextValue

  implicit def row2instOption(row: ResultSetRow): Option[Instant] =
    TimestampColumnType(row).nextValueOption.map(_.toInstant)

  implicit def row2inst(row: ResultSetRow): Instant =
    TimestampColumnType(row).nextValue.toInstant
}