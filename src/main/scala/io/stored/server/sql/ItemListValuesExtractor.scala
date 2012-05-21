package io.stored.server.sql


import net.sf.jsqlparser.statement.select.SubSelect
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.StringValue
import collection.mutable.ListBuffer
import net.sf.jsqlparser.expression.operators.relational.{ItemsListVisitor, ExpressionList}


class ItemListValuesExtractor extends ItemsListVisitor
{
  private val columnValues = new ListBuffer[AnyRef]

  def getValues = columnValues.toList

  def visit(p1: SubSelect)
  {}

  def visit(p1: ExpressionList)
  {
    import collection.JavaConversions._
    p1.getExpressions.foreach{x =>
      val o = x.asInstanceOf[AnyRef]
      if (o.isInstanceOf[LongValue]) {
        columnValues.append(o.asInstanceOf[LongValue].getValue.asInstanceOf[AnyRef])
      } else if (o.isInstanceOf[StringValue]) {
        columnValues.append(o.asInstanceOf[StringValue].getValue.asInstanceOf[AnyRef])
      }
    }
  }
}
