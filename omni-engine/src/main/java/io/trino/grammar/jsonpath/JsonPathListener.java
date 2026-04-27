// Generated from /home/penghuo/oss/trino/core/trino-grammar/src/main/antlr4/io/trino/grammar/jsonpath/JsonPath.g4 by ANTLR 4.13.1
package io.trino.grammar.jsonpath;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link JsonPathParser}.
 */
public interface JsonPathListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#path}.
	 * @param ctx the parse tree
	 */
	void enterPath(JsonPathParser.PathContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#path}.
	 * @param ctx the parse tree
	 */
	void exitPath(JsonPathParser.PathContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#pathMode}.
	 * @param ctx the parse tree
	 */
	void enterPathMode(JsonPathParser.PathModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#pathMode}.
	 * @param ctx the parse tree
	 */
	void exitPathMode(JsonPathParser.PathModeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code expressionDefault}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	void enterExpressionDefault(JsonPathParser.ExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code expressionDefault}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	void exitExpressionDefault(JsonPathParser.ExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code signedUnary}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	void enterSignedUnary(JsonPathParser.SignedUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code signedUnary}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	void exitSignedUnary(JsonPathParser.SignedUnaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code binary}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	void enterBinary(JsonPathParser.BinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code binary}
	 * labeled alternative in {@link JsonPathParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	void exitBinary(JsonPathParser.BinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code descendantMemberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterDescendantMemberAccessor(JsonPathParser.DescendantMemberAccessorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code descendantMemberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitDescendantMemberAccessor(JsonPathParser.DescendantMemberAccessorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterDoubleMethod(JsonPathParser.DoubleMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitDoubleMethod(JsonPathParser.DoubleMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code datetimeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterDatetimeMethod(JsonPathParser.DatetimeMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code datetimeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitDatetimeMethod(JsonPathParser.DatetimeMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code absMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterAbsMethod(JsonPathParser.AbsMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code absMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitAbsMethod(JsonPathParser.AbsMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code memberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterMemberAccessor(JsonPathParser.MemberAccessorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code memberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitMemberAccessor(JsonPathParser.MemberAccessorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arrayAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterArrayAccessor(JsonPathParser.ArrayAccessorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arrayAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitArrayAccessor(JsonPathParser.ArrayAccessorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code filter}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterFilter(JsonPathParser.FilterContext ctx);
	/**
	 * Exit a parse tree produced by the {@code filter}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitFilter(JsonPathParser.FilterContext ctx);
	/**
	 * Enter a parse tree produced by the {@code wildcardArrayAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterWildcardArrayAccessor(JsonPathParser.WildcardArrayAccessorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code wildcardArrayAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitWildcardArrayAccessor(JsonPathParser.WildcardArrayAccessorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ceilingMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterCeilingMethod(JsonPathParser.CeilingMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ceilingMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitCeilingMethod(JsonPathParser.CeilingMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code floorMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterFloorMethod(JsonPathParser.FloorMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code floorMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitFloorMethod(JsonPathParser.FloorMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code typeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterTypeMethod(JsonPathParser.TypeMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code typeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitTypeMethod(JsonPathParser.TypeMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code accessorExpressionDefault}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterAccessorExpressionDefault(JsonPathParser.AccessorExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code accessorExpressionDefault}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitAccessorExpressionDefault(JsonPathParser.AccessorExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code wildcardMemberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterWildcardMemberAccessor(JsonPathParser.WildcardMemberAccessorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code wildcardMemberAccessor}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitWildcardMemberAccessor(JsonPathParser.WildcardMemberAccessorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sizeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterSizeMethod(JsonPathParser.SizeMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sizeMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitSizeMethod(JsonPathParser.SizeMethodContext ctx);
	/**
	 * Enter a parse tree produced by the {@code keyValueMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void enterKeyValueMethod(JsonPathParser.KeyValueMethodContext ctx);
	/**
	 * Exit a parse tree produced by the {@code keyValueMethod}
	 * labeled alternative in {@link JsonPathParser#accessorExpression}.
	 * @param ctx the parse tree
	 */
	void exitKeyValueMethod(JsonPathParser.KeyValueMethodContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(JsonPathParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(JsonPathParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#subscript}.
	 * @param ctx the parse tree
	 */
	void enterSubscript(JsonPathParser.SubscriptContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#subscript}.
	 * @param ctx the parse tree
	 */
	void exitSubscript(JsonPathParser.SubscriptContext ctx);
	/**
	 * Enter a parse tree produced by the {@code literalPrimary}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void enterLiteralPrimary(JsonPathParser.LiteralPrimaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code literalPrimary}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void exitLiteralPrimary(JsonPathParser.LiteralPrimaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code variablePrimary}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void enterVariablePrimary(JsonPathParser.VariablePrimaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code variablePrimary}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void exitVariablePrimary(JsonPathParser.VariablePrimaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedPath}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedPath(JsonPathParser.ParenthesizedPathContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedPath}
	 * labeled alternative in {@link JsonPathParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedPath(JsonPathParser.ParenthesizedPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#literal}.
	 * @param ctx the parse tree
	 */
	void enterLiteral(JsonPathParser.LiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#literal}.
	 * @param ctx the parse tree
	 */
	void exitLiteral(JsonPathParser.LiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(JsonPathParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(JsonPathParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(JsonPathParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(JsonPathParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(JsonPathParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link JsonPathParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(JsonPathParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(JsonPathParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(JsonPathParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#nullLiteral}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(JsonPathParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#nullLiteral}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(JsonPathParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#booleanLiteral}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(JsonPathParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#booleanLiteral}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(JsonPathParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code contextVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterContextVariable(JsonPathParser.ContextVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code contextVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitContextVariable(JsonPathParser.ContextVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code namedVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterNamedVariable(JsonPathParser.NamedVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code namedVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitNamedVariable(JsonPathParser.NamedVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lastIndexVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterLastIndexVariable(JsonPathParser.LastIndexVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lastIndexVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitLastIndexVariable(JsonPathParser.LastIndexVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicateCurrentItemVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterPredicateCurrentItemVariable(JsonPathParser.PredicateCurrentItemVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicateCurrentItemVariable}
	 * labeled alternative in {@link JsonPathParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitPredicateCurrentItemVariable(JsonPathParser.PredicateCurrentItemVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code negationPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterNegationPredicate(JsonPathParser.NegationPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code negationPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitNegationPredicate(JsonPathParser.NegationPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicateDefault}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicateDefault(JsonPathParser.PredicateDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicateDefault}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicateDefault(JsonPathParser.PredicateDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code disjunctionPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterDisjunctionPredicate(JsonPathParser.DisjunctionPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code disjunctionPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitDisjunctionPredicate(JsonPathParser.DisjunctionPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code conjunctionPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterConjunctionPredicate(JsonPathParser.ConjunctionPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code conjunctionPredicate}
	 * labeled alternative in {@link JsonPathParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitConjunctionPredicate(JsonPathParser.ConjunctionPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicatePrimaryDefault}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void enterPredicatePrimaryDefault(JsonPathParser.PredicatePrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicatePrimaryDefault}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void exitPredicatePrimaryDefault(JsonPathParser.PredicatePrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparisonPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void enterComparisonPredicate(JsonPathParser.ComparisonPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparisonPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void exitComparisonPredicate(JsonPathParser.ComparisonPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code likeRegexPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void enterLikeRegexPredicate(JsonPathParser.LikeRegexPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code likeRegexPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void exitLikeRegexPredicate(JsonPathParser.LikeRegexPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code startsWithPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void enterStartsWithPredicate(JsonPathParser.StartsWithPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code startsWithPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void exitStartsWithPredicate(JsonPathParser.StartsWithPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code isUnknownPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void enterIsUnknownPredicate(JsonPathParser.IsUnknownPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code isUnknownPredicate}
	 * labeled alternative in {@link JsonPathParser#predicatePrimary}.
	 * @param ctx the parse tree
	 */
	void exitIsUnknownPredicate(JsonPathParser.IsUnknownPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code existsPredicate}
	 * labeled alternative in {@link JsonPathParser#delimitedPredicate}.
	 * @param ctx the parse tree
	 */
	void enterExistsPredicate(JsonPathParser.ExistsPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code existsPredicate}
	 * labeled alternative in {@link JsonPathParser#delimitedPredicate}.
	 * @param ctx the parse tree
	 */
	void exitExistsPredicate(JsonPathParser.ExistsPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedPredicate}
	 * labeled alternative in {@link JsonPathParser#delimitedPredicate}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedPredicate(JsonPathParser.ParenthesizedPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedPredicate}
	 * labeled alternative in {@link JsonPathParser#delimitedPredicate}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedPredicate(JsonPathParser.ParenthesizedPredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(JsonPathParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(JsonPathParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link JsonPathParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(JsonPathParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link JsonPathParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(JsonPathParser.NonReservedContext ctx);
}