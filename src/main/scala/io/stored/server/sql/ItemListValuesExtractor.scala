package io.stored.server.sql


import net.sf.jsqlparser.statement.select.SubSelect
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.StringValue
import collection.mutable.ListBuffer
import net.sf.jsqlparser.expression.operators.relational.{ItemsListVisitor, ExpressionList}
import io.stored.server.common.ProjectionField


class ItemListValuesExtractor extends ItemsListVisitor
{
  private val columnValues = new ListBuffer[BigInt]

  def getValues = columnValues.toList

  def visit(p1: SubSelect) {}

  def visit(p1: ExpressionList)
  {
    import collection.JavaConversions._
    p1.getExpressions.foreach{x =>
      val o = x.asInstanceOf[AnyRef]
      if (o.isInstanceOf[LongValue]) {
        columnValues.append(ProjectionField.md5Hash(o.asInstanceOf[LongValue].getValue))
      } else if (o.isInstanceOf[StringValue]) {
        columnValues.append(ProjectionField.md5Hash(o.asInstanceOf[StringValue].getValue))
      }
    }
  }
}
