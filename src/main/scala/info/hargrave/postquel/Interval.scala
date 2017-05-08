package info.hargrave.postquel

import java.time.{Duration, Instant, LocalDateTime, Period}
import java.time.temporal.{ChronoUnit, Temporal, TemporalUnit}
import java.util.Calendar

import net.noerd.prequel._
import org.joda.time.format.PeriodFormat
import org.postgresql.util.PGInterval

/**
  * Why metric time is needed
  */
object IntervalConversions {
  import java.time.temporal.ChronoUnit.{YEARS, MONTHS, DAYS, HOURS, MINUTES, MILLIS}

  // Conversion to/from is done via a similar calendar hack to PGInterval.add()
  def pg2java(i: PGInterval): (Period, Duration) = (
    Period.ZERO
      .plusDays(i.getDays.toLong)
      .plusMonths(i.getMonths.toLong)
      .plusYears(i.getYears.toLong),
    Duration.ZERO
      .plus((i.getSeconds * 1000).toLong,  MILLIS)
      .plus(i.getMinutes.toLong,  MINUTES)
      .plus(i.getHours.toLong,    HOURS)
  )


  def java2pg(t: (Period, Duration)): PGInterval = t match { case (p, d) =>
    val hours = ChronoUnit.HOURS.between(Instant.EPOCH, Instant.EPOCH.plus(d))
    val minutes = ChronoUnit.MINUTES.between(Instant.EPOCH, Instant.EPOCH.plus(d.minus(hours, HOURS)))
    val secondsFrac = ChronoUnit.MILLIS.between(Instant.EPOCH, Instant.EPOCH.plus(d.minus(hours, HOURS).minus(minutes, MINUTES))).toDouble / 1000D
    new PGInterval(
      p.getYears,
      p.getMonths,
      p.getDays,
      hours.toInt,
      minutes.toInt,
      secondsFrac
    )
  }
}

class IntervalColumnType(val row: ResultSetRow)
  extends ColumnType[(Period, Duration)] {
  override def nextValueOption: Option[(Period, Duration)] =
    row.nextObject
      .map(_.asInstanceOf[PGInterval])
      .map(IntervalConversions.pg2java)

  override def columnValueOption(columnName: String): Option[(Period, Duration)] =
    row.columnObject(columnName)
      .map(_.asInstanceOf[PGInterval])
      .map(IntervalConversions.pg2java)
}

object IntervalColumnType
  extends ColumnTypeFactory[(Period, Duration)] {
  override def apply(row: ResultSetRow): ColumnType[(Period, Duration)] = new IntervalColumnType(row)
}

class IntervalFormattable(override val value: (Period, Duration))
  extends Formattable {
  private lazy val pGInterval =
    Option(value).map(IntervalConversions.java2pg)

  override def escaped(formatter: SQLFormatter): String =
    pGInterval
      .map({x => s"'${x.getValue}'"})
      .getOrElse("NULL")

  override def addTo(statement: ReusableStatement): Unit =
    statement.addObject(pGInterval.orNull)
}

object IntervalFormattable {
  def apply(t: (Period, Duration)): IntervalFormattable = new IntervalFormattable(t)
  def apply(d: Duration): IntervalFormattable = IntervalFormattable((Period.ZERO, d))
  def apply(p: Period): IntervalFormattable = IntervalFormattable((p, Duration.ZERO))
}

trait IntervalTypeImplicits {
  implicit def duration2fmt(d: Duration): Formattable = IntervalFormattable(d)
  implicit def period2fmt(p: Period): Formattable = IntervalFormattable(p)
  implicit def intervalpair2fmt(t: (Period, Duration)): Formattable = IntervalFormattable(t)

  implicit def row2durationOpt(row: ResultSetRow): Option[Duration] = IntervalColumnType(row).nextValueOption.map(_._2)
  implicit def row2duration(row: ResultSetRow): Duration = IntervalColumnType(row).nextValue._2

  implicit def row2periodOpt(row: ResultSetRow): Option[Period] = IntervalColumnType(row).nextValueOption.map(_._1)
  implicit def row2period(row: ResultSetRow): Period = IntervalColumnType(row).nextValue._1

  implicit def row2intervalpairOpt(row: ResultSetRow): Option[(Period, Duration)] = IntervalColumnType(row).nextValueOption
  implicit def row2intervalpair(row: ResultSetRow): (Period, Duration) = IntervalColumnType(row).nextValue
}
