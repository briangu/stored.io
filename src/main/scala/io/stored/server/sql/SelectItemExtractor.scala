package io.stored.server.sql

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.statement.select._
import operators.arithmetic._
import operators.conditional.{AndExpression, OrExpression}
import operators.relational._
import collection.mutable.ListBuffer
import net.sf.jsqlparser.schema.{Table, Column}


class SelectItemExtractor extends SelectItemVisitor with ExpressionVisitor {

  val selectItems = new ListBuffer[String]

  private final val emptyTable = new Table

  def visit(allColumns: AllColumns) {}

  def visit(allTableColumns: AllTableColumns) {}

  def visit(selectExpressionItem: SelectExpressionItem) {
    val expression = selectExpressionItem.getExpression
    if (expression.isInstanceOf[Function]) {
      println(expression)
    } else {
      expression.accept(this)
    }
  }

  def visit(nullValue: NullValue) {}

  def visit(function: Function) {}

  def visit(inverseExpression: InverseExpression) {}

  def visit(jdbcParameter: JdbcParameter) {}

  def visit(doubleValue: DoubleValue) {}

  def visit(longValue: LongValue) {}

  def visit(dateValue: DateValue) {}

  def visit(timeValue: TimeValue) {}

  def visit(timestampValue: TimestampValue) {}

  def visit(parenthesis: Parenthesis) {}

  def visit(stringValue: StringValue) {}

  def visit(addition: Addition) {}

  def visit(division: Division) {}

  def visit(multiplication: Multiplication) {}

  def visit(subtraction: Subtraction) {}

  def visit(andExpression: AndExpression) {}

  def visit(orExpression: OrExpression) {}

  def visit(between: Between) {}

  def visit(equalsTo: EqualsTo) {}

  def visit(greaterThan: GreaterThan) {}

  def visit(greaterThanEquals: GreaterThanEquals) {}

  def visit(inExpression: InExpression) {}

  def visit(isNullExpression: IsNullExpression) {}

  def visit(likeExpression: LikeExpression) {}

  def visit(minorThan: MinorThan) {}

  def visit(minorThanEquals: MinorThanEquals) {}

  def visit(notEqualsTo: NotEqualsTo) {}

  // Extract select-clause columns for JSON data extraction.
  // Note, we don't upCase them since the JSON fields are not upcased
  def visit(tableColumn: Column) {
    val colName = tableColumn.getColumnName
    selectItems.append(colName)
    tableColumn.setTable(emptyTable)
    tableColumn.setColumnName(colName)
  }

  def visit(subSelect: SubSelect) {}

  def visit(caseExpression: CaseExpression) {}

  def visit(whenClause: WhenClause) {}

  def visit(existsExpression: ExistsExpression) {}

  def visit(allComparisonExpression: AllComparisonExpression) {}

  def visit(anyComparisonExpression: AnyComparisonExpression) {}

  def visit(concat: Concat) {}

  def visit(matches: Matches) {}

  def visit(bitwiseAnd: BitwiseAnd) {}

  def visit(bitwiseOr: BitwiseOr) {}

  def visit(bitwiseXor: BitwiseXor) {}
}
