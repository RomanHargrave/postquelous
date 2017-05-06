package info.hargrave.postquel

import java.util.UUID

import net.noerd.prequel._

/**
  * UUID Column Type Extractor
  */
class UUIDColumnType(val row: ResultSetRow)
   extends ColumnType[UUID]
{
   override def nextValueOption: Option[ UUID ] =
      Option(row.nextObject).map(_.asInstanceOf[UUID])

   override def columnValueOption(columnName: String): Option[ UUID ] =
      row.columnObject(columnName).map(_.asInstanceOf[UUID])
}

object UUIDColumnType
   extends ColumnTypeFactory[UUID]
{
   def apply(row: ResultSetRow) = new UUIDColumnType(row)
}

class UUIDFormattable(override val value: UUID)
   extends Formattable
{
   override def escaped(formatter: SQLFormatter): String =
      s"${formatter.toSQLString(value.toString)}::uuid"

   override def addTo(stmt: ReusableStatement): Unit =
      stmt.addObject(value)
}

trait UUIDTypeImplicits
{
   implicit def uuid2fmt(uuid: UUID): Formattable = new UUIDFormattable(uuid)
   implicit def row2uuid(row: ResultSetRow): UUID = UUIDColumnType(row).nextValue
}