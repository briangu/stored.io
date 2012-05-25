package io.stored.server.sql


import net.sf.jsqlparser.expression._
import operators.arithmetic._
import operators.conditional._
import operators.relational._
import java.util.Arrays
import collection.mutable.{HashMap, ListBuffer}
import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.schema.{Table, Column}


class SqlRequestProcessor extends SelectVisitor with ExpressionVisitor
{
  var projectionName: String = null
  var selectItems: List[String] = null
  var whereItems: HashMap[String, List[BigInt]] = new HashMap[String, List[BigInt]]

  def visit(p1: NullValue)
  {}

  def visit(p1: Function)
  {}

  def visit(p1: InverseExpression)
  {}

  def visit(p1: JdbcParameter)
  {}

  def visit(p1: DoubleValue)
  {
    println(p1.toString)
  }

  def visit(p1: LongValue)
  {}

  def visit(p1: DateValue)
  {}

  def visit(p1: TimeValue)
  {}

  def visit(p1: TimestampValue)
  {}

  def visit(p1: Parenthesis)
  {}

  def visit(p1: StringValue)
  {
    println(p1.toString)
  }

  def visit(p1: Addition)
  {}

  def visit(p1: Division)
  {}

  def visit(p1: Multiplication)
  {}

  def visit(p1: Subtraction)
  {}

  def visit(p1: AndExpression)
  {
    p1.getLeftExpression.accept(this)
    p1.getRightExpression.accept(this)
  }

  def visit(p1: OrExpression)
  {}

  def visit(p1: Between)
  {}

  def visit(p1: EqualsTo)
  {
    println(p1.toString)

    val colExtractor = new ColumnNameExtractor
    p1.getLeftExpression.accept(colExtractor)

    if (colExtractor.columnName != null) {
      val valueExtractor = new ExpressionValueExtractor
      p1.getRightExpression.accept(valueExtractor)
      if (valueExtractor.expressionValue != null) {
        whereItems.put(colExtractor.columnName, List(valueExtractor.expressionValue))
      }
    }
  }

  def visit(p1: GreaterThan)
  {}

  def visit(p1: GreaterThanEquals)
  {}

  def visit(p1: InExpression)
  {
    println(p1.toString)

    val colExtractor = new ColumnNameExtractor
    p1.getLeftExpression.accept(colExtractor)

    if (colExtractor.columnName != null) {
      val valuesExtractor = new ItemListValuesExtractor
      p1.getItemsList.accept(valuesExtractor)

      val values = (valuesExtractor.getValues)
      if (values.size > 0) whereItems.put(colExtractor.columnName, values)
    }
  }

  def visit(p1: IsNullExpression)
  {}

  def visit(p1: LikeExpression)
  {}

  def visit(p1: MinorThan)
  {}

  def visit(p1: MinorThanEquals)
  {}

  def visit(p1: NotEqualsTo)
  {}

  def visit(p1: Column)
  {
    println(p1.toString)
  }

  def visit(p1: SubSelect)
  {}

  def visit(p1: CaseExpression)
  {}

  def visit(p1: WhenClause)
  {}

  def visit(p1: ExistsExpression)
  {}

  def visit(p1: AllComparisonExpression)
  {}

  def visit(p1: AnyComparisonExpression)
  {}

  def visit(p1: Concat)
  {}

  def visit(p1: Matches)
  {
    println(p1.toString)
  }

  def visit(p1: BitwiseAnd)
  {}

  def visit(p1: BitwiseOr)
  {}

  def visit(p1: BitwiseXor)
  {}

  def visit(plainSelect: PlainSelect)
  {
    val originalSelectItems = new ListBuffer[String]

    plainSelect.getFromItem.accept(new FromItemVisitor {
      def visit(p1: Table)
      {
        projectionName = p1.getName
        p1.setName("DATA_INDEX")
      }

      def visit(p1: SubSelect)
      {}

      def visit(p1: SubJoin)
      {}
    })

    val sqlSelectItems = plainSelect.getSelectItems
    if (!(sqlSelectItems.size() == 1 && sqlSelectItems.get(0).equals("*")))
    {
      for (i <- 0 until sqlSelectItems.size())
      {
        val selectItem = sqlSelectItems.get(i).toString

        // TODO: there has to be a better way with jsqlparser!
        if (!selectItem.toUpperCase.startsWith("COUNT")) {
          originalSelectItems.append(selectItem)
        }
      }
      plainSelect.setSelectItems(Arrays.asList("*"))
    }

    selectItems = originalSelectItems.toList

    if (plainSelect.getWhere != null) plainSelect.getWhere.accept(this)
  }

  def visit(p1: Union)
  {}
}
