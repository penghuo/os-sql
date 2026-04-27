// Generated from /home/penghuo/oss/trino/core/trino-grammar/src/main/antlr4/io/trino/grammar/jsonpath/JsonPath.g4 by ANTLR 4.13.1
package io.trino.grammar.jsonpath;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link JsonPathParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface JsonPathVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#path}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPath(JsonPathParser.PathContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#pathMode}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPathMode(JsonPathParser.PathModeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code expressionDefault}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressionDefault(JsonPathParser.ExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code signedUnary}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSignedUnary(JsonPathParser.SignedUnaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code binary}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinary(JsonPathParser.BinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code descendantMemberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescendantMemberAccessor(JsonPathParser.DescendantMemberAccessorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleMethod(JsonPathParser.DoubleMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code datetimeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatetimeMethod(JsonPathParser.DatetimeMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code absMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAbsMethod(JsonPathParser.AbsMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code memberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMemberAccessor(JsonPathParser.MemberAccessorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arrayAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArrayAccessor(JsonPathParser.ArrayAccessorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code filter}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFilter(JsonPathParser.FilterContext ctx);
	/**
	 * Visit a parse tree produced by the {@code wildcardArrayAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcardArrayAccessor(JsonPathParser.WildcardArrayAccessorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ceilingMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCeilingMethod(JsonPathParser.CeilingMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code floorMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloorMethod(JsonPathParser.FloorMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code typeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeMethod(JsonPathParser.TypeMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code accessorExpressionDefault}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAccessorExpressionDefault(JsonPathParser.AccessorExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code wildcardMemberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcardMemberAccessor(JsonPathParser.WildcardMemberAccessorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sizeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSizeMethod(JsonPathParser.SizeMethodContext ctx);
	/**
	 * Visit a parse tree produced by the {@code keyValueMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeyValueMethod(JsonPathParser.KeyValueMethodContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(JsonPathParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#subscript}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscript(JsonPathParser.SubscriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code literalPrimary}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteralPrimary(JsonPathParser.LiteralPrimaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code variablePrimary}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariablePrimary(JsonPathParser.VariablePrimaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedPath}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedPath(JsonPathParser.ParenthesizedPathContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(JsonPathParser.LiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(JsonPathParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(JsonPathParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(JsonPathParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#stringLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(JsonPathParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#nullLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullLiteral(JsonPathParser.NullLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#booleanLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(JsonPathParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code contextVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitContextVariable(JsonPathParser.ContextVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code namedVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedVariable(JsonPathParser.NamedVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lastIndexVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLastIndexVariable(JsonPathParser.LastIndexVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code predicateCurrentItemVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicateCurrentItemVariable(JsonPathParser.PredicateCurrentItemVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code negationPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNegationPredicate(JsonPathParser.NegationPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code predicateDefault}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicateDefault(JsonPathParser.PredicateDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code disjunctionPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDisjunctionPredicate(JsonPathParser.DisjunctionPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code conjunctionPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConjunctionPredicate(JsonPathParser.ConjunctionPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code predicatePrimaryDefault}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicatePrimaryDefault(JsonPathParser.PredicatePrimaryDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparisonPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonPredicate(JsonPathParser.ComparisonPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code likeRegexPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLikeRegexPredicate(JsonPathParser.LikeRegexPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code startsWithPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStartsWithPredicate(JsonPathParser.StartsWithPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code isUnknownPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsUnknownPredicate(JsonPathParser.IsUnknownPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code existsPredicate}
	 * labeled alternative in {@link JsonPathParser#delimitedPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExistsPredicate(JsonPathParser.ExistsPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedPredicate}
	 * labeled alternative in {@link JsonPathParser#delimitedPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedPredicate(JsonPathParser.ParenthesizedPredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(JsonPathParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link JsonPathParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(JsonPathParser.NonReservedContext ctx);
}