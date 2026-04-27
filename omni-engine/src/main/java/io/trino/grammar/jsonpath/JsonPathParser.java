// Generated from /home/penghuo/oss/trino/core/trino-grammar/src/main/antlr4/io/trino/grammar/jsonpath/JsonPath.g4 by ANTLR 4.13.1
package io.trino.grammar.jsonpath;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class JsonPathParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		ABS=25, CEILING=26, DATETIME=27, DOUBLE=28, EXISTS=29, FALSE=30, FLAG=31, 
		FLOOR=32, IS=33, KEYVALUE=34, LAST=35, LAX=36, LIKE_REGEX=37, MINUS=38, 
		NULL=39, SIZE=40, STARTS=41, STRICT=42, TO=43, TRUE=44, TYPE=45, UNKNOWN=46, 
		WITH=47, DECIMAL_VALUE=48, DOUBLE_VALUE=49, INTEGER_VALUE=50, STRING=51, 
		IDENTIFIER=52, NAMED_VARIABLE=53, WS=54, UNRECOGNIZED=55, DELIMITER=56;
	public static final int
		RULE_path = 0, RULE_pathMode = 1, RULE_pathExpression = 2, RULE_accessorExpression = 3, 
		RULE_identifier = 4, RULE_subscript = 5, RULE_pathPrimary = 6, RULE_literal = 7, 
		RULE_numericLiteral = 8, RULE_stringLiteral = 9, RULE_nullLiteral = 10, 
		RULE_booleanLiteral = 11, RULE_variable = 12, RULE_predicate = 13, RULE_predicatePrimary = 14, 
		RULE_delimitedPredicate = 15, RULE_comparisonOperator = 16, RULE_nonReserved = 17;
	private static String[] makeRuleNames() {
		return new String[] {
			"path", "pathMode", "pathExpression", "accessorExpression", "identifier", 
			"subscript", "pathPrimary", "literal", "numericLiteral", "stringLiteral", 
			"nullLiteral", "booleanLiteral", "variable", "predicate", "predicatePrimary", 
			"delimitedPredicate", "comparisonOperator", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'+'", "'*'", "'/'", "'%'", "'.'", "'..'", "'['", "','", "']'", 
			"'?'", "'('", "')'", "'$'", "'@'", "'!'", "'&&'", "'||'", "'=='", "'<>'", 
			"'!='", "'<'", "'>'", "'<='", "'>='", "'abs'", "'ceiling'", "'datetime'", 
			"'double'", "'exists'", "'false'", "'flag'", "'floor'", "'is'", "'keyvalue'", 
			"'last'", "'lax'", "'like_regex'", "'-'", "'null'", "'size'", "'starts'", 
			"'strict'", "'to'", "'true'", "'type'", "'unknown'", "'with'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, "ABS", "CEILING", "DATETIME", "DOUBLE", "EXISTS", "FALSE", "FLAG", 
			"FLOOR", "IS", "KEYVALUE", "LAST", "LAX", "LIKE_REGEX", "MINUS", "NULL", 
			"SIZE", "STARTS", "STRICT", "TO", "TRUE", "TYPE", "UNKNOWN", "WITH", 
			"DECIMAL_VALUE", "DOUBLE_VALUE", "INTEGER_VALUE", "STRING", "IDENTIFIER", 
			"NAMED_VARIABLE", "WS", "UNRECOGNIZED", "DELIMITER"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "JsonPath.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public JsonPathParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathContext extends ParserRuleContext {
		public PathModeContext pathMode() {
			return getRuleContext(PathModeContext.class,0);
		}
		public PathExpressionContext pathExpression() {
			return getRuleContext(PathExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(JsonPathParser.EOF, 0); }
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_path);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(36);
			pathMode();
			setState(37);
			pathExpression(0);
			setState(38);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathModeContext extends ParserRuleContext {
		public TerminalNode LAX() { return getToken(JsonPathParser.LAX, 0); }
		public TerminalNode STRICT() { return getToken(JsonPathParser.STRICT, 0); }
		public PathModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathMode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterPathMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitPathMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitPathMode(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathModeContext pathMode() throws RecognitionException {
		PathModeContext _localctx = new PathModeContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_pathMode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(40);
			_la = _input.LA(1);
			if ( !(_la==LAX || _la==STRICT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathExpressionContext extends ParserRuleContext {
		public PathExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathExpression; }
	 
		public PathExpressionContext() { }
		public void copyFrom(PathExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionDefaultContext extends PathExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public ExpressionDefaultContext(PathExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SignedUnaryContext extends PathExpressionContext {
		public Token sign;
		public PathExpressionContext pathExpression() {
			return getRuleContext(PathExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(JsonPathParser.MINUS, 0); }
		public SignedUnaryContext(PathExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterSignedUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitSignedUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitSignedUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryContext extends PathExpressionContext {
		public PathExpressionContext left;
		public Token operator;
		public PathExpressionContext right;
		public List<PathExpressionContext> pathExpression() {
			return getRuleContexts(PathExpressionContext.class);
		}
		public PathExpressionContext pathExpression(int i) {
			return getRuleContext(PathExpressionContext.class,i);
		}
		public TerminalNode MINUS() { return getToken(JsonPathParser.MINUS, 0); }
		public BinaryContext(PathExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathExpressionContext pathExpression() throws RecognitionException {
		return pathExpression(0);
	}

	private PathExpressionContext pathExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PathExpressionContext _localctx = new PathExpressionContext(_ctx, _parentState);
		PathExpressionContext _prevctx = _localctx;
		int _startState = 4;
		enterRecursionRule(_localctx, 4, RULE_pathExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(46);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				{
				_localctx = new ExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(43);
				accessorExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new SignedUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(44);
				((SignedUnaryContext)_localctx).sign = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==T__0 || _la==MINUS) ) {
					((SignedUnaryContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(45);
				pathExpression(3);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(56);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(54);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryContext(new PathExpressionContext(_parentctx, _parentState));
						((BinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_pathExpression);
						setState(48);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(49);
						((BinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 28L) != 0)) ) {
							((BinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(50);
						((BinaryContext)_localctx).right = pathExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new BinaryContext(new PathExpressionContext(_parentctx, _parentState));
						((BinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_pathExpression);
						setState(51);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(52);
						((BinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__0 || _la==MINUS) ) {
							((BinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(53);
						((BinaryContext)_localctx).right = pathExpression(2);
						}
						break;
					}
					} 
				}
				setState(58);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AccessorExpressionContext extends ParserRuleContext {
		public AccessorExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_accessorExpression; }
	 
		public AccessorExpressionContext() { }
		public void copyFrom(AccessorExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescendantMemberAccessorContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public DescendantMemberAccessorContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterDescendantMemberAccessor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitDescendantMemberAccessor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitDescendantMemberAccessor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode DOUBLE() { return getToken(JsonPathParser.DOUBLE, 0); }
		public DoubleMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterDoubleMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitDoubleMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitDoubleMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DatetimeMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode DATETIME() { return getToken(JsonPathParser.DATETIME, 0); }
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public DatetimeMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterDatetimeMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitDatetimeMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitDatetimeMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AbsMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode ABS() { return getToken(JsonPathParser.ABS, 0); }
		public AbsMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterAbsMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitAbsMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitAbsMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MemberAccessorContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public MemberAccessorContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterMemberAccessor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitMemberAccessor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitMemberAccessor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayAccessorContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public List<SubscriptContext> subscript() {
			return getRuleContexts(SubscriptContext.class);
		}
		public SubscriptContext subscript(int i) {
			return getRuleContext(SubscriptContext.class,i);
		}
		public ArrayAccessorContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterArrayAccessor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitArrayAccessor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitArrayAccessor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FilterContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public FilterContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterFilter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitFilter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitFilter(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WildcardArrayAccessorContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public WildcardArrayAccessorContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterWildcardArrayAccessor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitWildcardArrayAccessor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitWildcardArrayAccessor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CeilingMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode CEILING() { return getToken(JsonPathParser.CEILING, 0); }
		public CeilingMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterCeilingMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitCeilingMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitCeilingMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FloorMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode FLOOR() { return getToken(JsonPathParser.FLOOR, 0); }
		public FloorMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterFloorMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitFloorMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitFloorMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode TYPE() { return getToken(JsonPathParser.TYPE, 0); }
		public TypeMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterTypeMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitTypeMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitTypeMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AccessorExpressionDefaultContext extends AccessorExpressionContext {
		public PathPrimaryContext pathPrimary() {
			return getRuleContext(PathPrimaryContext.class,0);
		}
		public AccessorExpressionDefaultContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterAccessorExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitAccessorExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitAccessorExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WildcardMemberAccessorContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public WildcardMemberAccessorContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterWildcardMemberAccessor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitWildcardMemberAccessor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitWildcardMemberAccessor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SizeMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode SIZE() { return getToken(JsonPathParser.SIZE, 0); }
		public SizeMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterSizeMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitSizeMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitSizeMethod(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class KeyValueMethodContext extends AccessorExpressionContext {
		public AccessorExpressionContext accessorExpression() {
			return getRuleContext(AccessorExpressionContext.class,0);
		}
		public TerminalNode KEYVALUE() { return getToken(JsonPathParser.KEYVALUE, 0); }
		public KeyValueMethodContext(AccessorExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterKeyValueMethod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitKeyValueMethod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitKeyValueMethod(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AccessorExpressionContext accessorExpression() throws RecognitionException {
		return accessorExpression(0);
	}

	private AccessorExpressionContext accessorExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		AccessorExpressionContext _localctx = new AccessorExpressionContext(_ctx, _parentState);
		AccessorExpressionContext _prevctx = _localctx;
		int _startState = 6;
		enterRecursionRule(_localctx, 6, RULE_accessorExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new AccessorExpressionDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(60);
			pathPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(144);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(142);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
					case 1:
						{
						_localctx = new MemberAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(62);
						if (!(precpred(_ctx, 16))) throw new FailedPredicateException(this, "precpred(_ctx, 16)");
						setState(63);
						match(T__4);
						setState(64);
						identifier();
						}
						break;
					case 2:
						{
						_localctx = new MemberAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(65);
						if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
						setState(66);
						match(T__4);
						setState(67);
						stringLiteral();
						}
						break;
					case 3:
						{
						_localctx = new WildcardMemberAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(68);
						if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
						setState(69);
						match(T__4);
						setState(70);
						match(T__1);
						}
						break;
					case 4:
						{
						_localctx = new DescendantMemberAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(71);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(72);
						match(T__5);
						setState(73);
						identifier();
						}
						break;
					case 5:
						{
						_localctx = new DescendantMemberAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(74);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(75);
						match(T__5);
						setState(76);
						stringLiteral();
						}
						break;
					case 6:
						{
						_localctx = new ArrayAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(77);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(78);
						match(T__6);
						setState(79);
						subscript();
						setState(84);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__7) {
							{
							{
							setState(80);
							match(T__7);
							setState(81);
							subscript();
							}
							}
							setState(86);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(87);
						match(T__8);
						}
						break;
					case 7:
						{
						_localctx = new WildcardArrayAccessorContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(89);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(90);
						match(T__6);
						setState(91);
						match(T__1);
						setState(92);
						match(T__8);
						}
						break;
					case 8:
						{
						_localctx = new FilterContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(93);
						if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
						setState(94);
						match(T__9);
						setState(95);
						match(T__10);
						setState(96);
						predicate(0);
						setState(97);
						match(T__11);
						}
						break;
					case 9:
						{
						_localctx = new TypeMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(99);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(100);
						match(T__4);
						setState(101);
						match(TYPE);
						setState(102);
						match(T__10);
						setState(103);
						match(T__11);
						}
						break;
					case 10:
						{
						_localctx = new SizeMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(104);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(105);
						match(T__4);
						setState(106);
						match(SIZE);
						setState(107);
						match(T__10);
						setState(108);
						match(T__11);
						}
						break;
					case 11:
						{
						_localctx = new DoubleMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(109);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(110);
						match(T__4);
						setState(111);
						match(DOUBLE);
						setState(112);
						match(T__10);
						setState(113);
						match(T__11);
						}
						break;
					case 12:
						{
						_localctx = new CeilingMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(114);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(115);
						match(T__4);
						setState(116);
						match(CEILING);
						setState(117);
						match(T__10);
						setState(118);
						match(T__11);
						}
						break;
					case 13:
						{
						_localctx = new FloorMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(119);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(120);
						match(T__4);
						setState(121);
						match(FLOOR);
						setState(122);
						match(T__10);
						setState(123);
						match(T__11);
						}
						break;
					case 14:
						{
						_localctx = new AbsMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(124);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(125);
						match(T__4);
						setState(126);
						match(ABS);
						setState(127);
						match(T__10);
						setState(128);
						match(T__11);
						}
						break;
					case 15:
						{
						_localctx = new DatetimeMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(129);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(130);
						match(T__4);
						setState(131);
						match(DATETIME);
						setState(132);
						match(T__10);
						setState(134);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==STRING) {
							{
							setState(133);
							stringLiteral();
							}
						}

						setState(136);
						match(T__11);
						}
						break;
					case 16:
						{
						_localctx = new KeyValueMethodContext(new AccessorExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_accessorExpression);
						setState(137);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(138);
						match(T__4);
						setState(139);
						match(KEYVALUE);
						setState(140);
						match(T__10);
						setState(141);
						match(T__11);
						}
						break;
					}
					} 
				}
				setState(146);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(JsonPathParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_identifier);
		try {
			setState(149);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(147);
				match(IDENTIFIER);
				}
				break;
			case ABS:
			case CEILING:
			case DATETIME:
			case DOUBLE:
			case EXISTS:
			case FALSE:
			case FLAG:
			case FLOOR:
			case IS:
			case KEYVALUE:
			case LAST:
			case LAX:
			case LIKE_REGEX:
			case MINUS:
			case NULL:
			case SIZE:
			case STARTS:
			case STRICT:
			case TO:
			case TRUE:
			case TYPE:
			case UNKNOWN:
			case WITH:
				enterOuterAlt(_localctx, 2);
				{
				setState(148);
				nonReserved();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptContext extends ParserRuleContext {
		public PathExpressionContext singleton;
		public PathExpressionContext from;
		public PathExpressionContext to;
		public List<PathExpressionContext> pathExpression() {
			return getRuleContexts(PathExpressionContext.class);
		}
		public PathExpressionContext pathExpression(int i) {
			return getRuleContext(PathExpressionContext.class,i);
		}
		public TerminalNode TO() { return getToken(JsonPathParser.TO, 0); }
		public SubscriptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subscript; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubscriptContext subscript() throws RecognitionException {
		SubscriptContext _localctx = new SubscriptContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_subscript);
		try {
			setState(156);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(151);
				((SubscriptContext)_localctx).singleton = pathExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(152);
				((SubscriptContext)_localctx).from = pathExpression(0);
				setState(153);
				match(TO);
				setState(154);
				((SubscriptContext)_localctx).to = pathExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathPrimaryContext extends ParserRuleContext {
		public PathPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathPrimary; }
	 
		public PathPrimaryContext() { }
		public void copyFrom(PathPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class VariablePrimaryContext extends PathPrimaryContext {
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public VariablePrimaryContext(PathPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterVariablePrimary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitVariablePrimary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitVariablePrimary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LiteralPrimaryContext extends PathPrimaryContext {
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public LiteralPrimaryContext(PathPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterLiteralPrimary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitLiteralPrimary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitLiteralPrimary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedPathContext extends PathPrimaryContext {
		public PathExpressionContext pathExpression() {
			return getRuleContext(PathExpressionContext.class,0);
		}
		public ParenthesizedPathContext(PathPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterParenthesizedPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitParenthesizedPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitParenthesizedPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathPrimaryContext pathPrimary() throws RecognitionException {
		PathPrimaryContext _localctx = new PathPrimaryContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_pathPrimary);
		try {
			setState(164);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FALSE:
			case MINUS:
			case NULL:
			case TRUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case INTEGER_VALUE:
			case STRING:
				_localctx = new LiteralPrimaryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(158);
				literal();
				}
				break;
			case T__12:
			case T__13:
			case LAST:
			case NAMED_VARIABLE:
				_localctx = new VariablePrimaryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(159);
				variable();
				}
				break;
			case T__10:
				_localctx = new ParenthesizedPathContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(160);
				match(T__10);
				setState(161);
				pathExpression(0);
				setState(162);
				match(T__11);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LiteralContext extends ParserRuleContext {
		public NumericLiteralContext numericLiteral() {
			return getRuleContext(NumericLiteralContext.class,0);
		}
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public NullLiteralContext nullLiteral() {
			return getRuleContext(NullLiteralContext.class,0);
		}
		public BooleanLiteralContext booleanLiteral() {
			return getRuleContext(BooleanLiteralContext.class,0);
		}
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_literal);
		try {
			setState(170);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MINUS:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(166);
				numericLiteral();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(167);
				stringLiteral();
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 3);
				{
				setState(168);
				nullLiteral();
				}
				break;
			case FALSE:
			case TRUE:
				enterOuterAlt(_localctx, 4);
				{
				setState(169);
				booleanLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends ParserRuleContext {
		public NumericLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericLiteral; }
	 
		public NumericLiteralContext() { }
		public void copyFrom(NumericLiteralContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumericLiteralContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(JsonPathParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(JsonPathParser.MINUS, 0); }
		public DecimalLiteralContext(NumericLiteralContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumericLiteralContext {
		public TerminalNode DOUBLE_VALUE() { return getToken(JsonPathParser.DOUBLE_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(JsonPathParser.MINUS, 0); }
		public DoubleLiteralContext(NumericLiteralContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumericLiteralContext {
		public TerminalNode INTEGER_VALUE() { return getToken(JsonPathParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(JsonPathParser.MINUS, 0); }
		public IntegerLiteralContext(NumericLiteralContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumericLiteralContext numericLiteral() throws RecognitionException {
		NumericLiteralContext _localctx = new NumericLiteralContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_numericLiteral);
		int _la;
		try {
			setState(184);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(173);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(172);
					match(MINUS);
					}
				}

				setState(175);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(177);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(176);
					match(MINUS);
					}
				}

				setState(179);
				match(DOUBLE_VALUE);
				}
				break;
			case 3:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(181);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(180);
					match(MINUS);
					}
				}

				setState(183);
				match(INTEGER_VALUE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(JsonPathParser.STRING, 0); }
		public StringLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stringLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringLiteralContext stringLiteral() throws RecognitionException {
		StringLiteralContext _localctx = new StringLiteralContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_stringLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(186);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends ParserRuleContext {
		public TerminalNode NULL() { return getToken(JsonPathParser.NULL, 0); }
		public NullLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullLiteralContext nullLiteral() throws RecognitionException {
		NullLiteralContext _localctx = new NullLiteralContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_nullLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(188);
			match(NULL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(JsonPathParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(JsonPathParser.FALSE, 0); }
		public BooleanLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanLiteralContext booleanLiteral() throws RecognitionException {
		BooleanLiteralContext _localctx = new BooleanLiteralContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_booleanLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(190);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VariableContext extends ParserRuleContext {
		public VariableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variable; }
	 
		public VariableContext() { }
		public void copyFrom(VariableContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ContextVariableContext extends VariableContext {
		public ContextVariableContext(VariableContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterContextVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitContextVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitContextVariable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NamedVariableContext extends VariableContext {
		public TerminalNode NAMED_VARIABLE() { return getToken(JsonPathParser.NAMED_VARIABLE, 0); }
		public NamedVariableContext(VariableContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterNamedVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitNamedVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitNamedVariable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LastIndexVariableContext extends VariableContext {
		public TerminalNode LAST() { return getToken(JsonPathParser.LAST, 0); }
		public LastIndexVariableContext(VariableContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterLastIndexVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitLastIndexVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitLastIndexVariable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicateCurrentItemVariableContext extends VariableContext {
		public PredicateCurrentItemVariableContext(VariableContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterPredicateCurrentItemVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitPredicateCurrentItemVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitPredicateCurrentItemVariable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableContext variable() throws RecognitionException {
		VariableContext _localctx = new VariableContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_variable);
		try {
			setState(196);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__12:
				_localctx = new ContextVariableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(192);
				match(T__12);
				}
				break;
			case NAMED_VARIABLE:
				_localctx = new NamedVariableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(193);
				match(NAMED_VARIABLE);
				}
				break;
			case LAST:
				_localctx = new LastIndexVariableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(194);
				match(LAST);
				}
				break;
			case T__13:
				_localctx = new PredicateCurrentItemVariableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(195);
				match(T__13);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateContext extends ParserRuleContext {
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NegationPredicateContext extends PredicateContext {
		public DelimitedPredicateContext delimitedPredicate() {
			return getRuleContext(DelimitedPredicateContext.class,0);
		}
		public NegationPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterNegationPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitNegationPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitNegationPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicateDefaultContext extends PredicateContext {
		public PredicatePrimaryContext predicatePrimary() {
			return getRuleContext(PredicatePrimaryContext.class,0);
		}
		public PredicateDefaultContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterPredicateDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitPredicateDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitPredicateDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DisjunctionPredicateContext extends PredicateContext {
		public PredicateContext left;
		public PredicateContext right;
		public List<PredicateContext> predicate() {
			return getRuleContexts(PredicateContext.class);
		}
		public PredicateContext predicate(int i) {
			return getRuleContext(PredicateContext.class,i);
		}
		public DisjunctionPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterDisjunctionPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitDisjunctionPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitDisjunctionPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConjunctionPredicateContext extends PredicateContext {
		public PredicateContext left;
		public PredicateContext right;
		public List<PredicateContext> predicate() {
			return getRuleContexts(PredicateContext.class);
		}
		public PredicateContext predicate(int i) {
			return getRuleContext(PredicateContext.class,i);
		}
		public ConjunctionPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterConjunctionPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitConjunctionPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitConjunctionPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		return predicate(0);
	}

	private PredicateContext predicate(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PredicateContext _localctx = new PredicateContext(_ctx, _parentState);
		PredicateContext _prevctx = _localctx;
		int _startState = 26;
		enterRecursionRule(_localctx, 26, RULE_predicate, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(202);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case T__10:
			case T__12:
			case T__13:
			case EXISTS:
			case FALSE:
			case LAST:
			case MINUS:
			case NULL:
			case TRUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case INTEGER_VALUE:
			case STRING:
			case NAMED_VARIABLE:
				{
				_localctx = new PredicateDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(199);
				predicatePrimary();
				}
				break;
			case T__14:
				{
				_localctx = new NegationPredicateContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(200);
				match(T__14);
				setState(201);
				delimitedPredicate();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(212);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(210);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
					case 1:
						{
						_localctx = new ConjunctionPredicateContext(new PredicateContext(_parentctx, _parentState));
						((ConjunctionPredicateContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_predicate);
						setState(204);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(205);
						match(T__15);
						setState(206);
						((ConjunctionPredicateContext)_localctx).right = predicate(3);
						}
						break;
					case 2:
						{
						_localctx = new DisjunctionPredicateContext(new PredicateContext(_parentctx, _parentState));
						((DisjunctionPredicateContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_predicate);
						setState(207);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(208);
						match(T__16);
						setState(209);
						((DisjunctionPredicateContext)_localctx).right = predicate(2);
						}
						break;
					}
					} 
				}
				setState(214);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicatePrimaryContext extends ParserRuleContext {
		public PredicatePrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicatePrimary; }
	 
		public PredicatePrimaryContext() { }
		public void copyFrom(PredicatePrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LikeRegexPredicateContext extends PredicatePrimaryContext {
		public PathExpressionContext base;
		public StringLiteralContext pattern;
		public StringLiteralContext flag;
		public TerminalNode LIKE_REGEX() { return getToken(JsonPathParser.LIKE_REGEX, 0); }
		public PathExpressionContext pathExpression() {
			return getRuleContext(PathExpressionContext.class,0);
		}
		public List<StringLiteralContext> stringLiteral() {
			return getRuleContexts(StringLiteralContext.class);
		}
		public StringLiteralContext stringLiteral(int i) {
			return getRuleContext(StringLiteralContext.class,i);
		}
		public TerminalNode FLAG() { return getToken(JsonPathParser.FLAG, 0); }
		public LikeRegexPredicateContext(PredicatePrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterLikeRegexPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitLikeRegexPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitLikeRegexPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonPredicateContext extends PredicatePrimaryContext {
		public PathExpressionContext left;
		public PathExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<PathExpressionContext> pathExpression() {
			return getRuleContexts(PathExpressionContext.class);
		}
		public PathExpressionContext pathExpression(int i) {
			return getRuleContext(PathExpressionContext.class,i);
		}
		public ComparisonPredicateContext(PredicatePrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterComparisonPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitComparisonPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitComparisonPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatePrimaryDefaultContext extends PredicatePrimaryContext {
		public DelimitedPredicateContext delimitedPredicate() {
			return getRuleContext(DelimitedPredicateContext.class,0);
		}
		public PredicatePrimaryDefaultContext(PredicatePrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterPredicatePrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitPredicatePrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitPredicatePrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StartsWithPredicateContext extends PredicatePrimaryContext {
		public PathExpressionContext whole;
		public StringLiteralContext string;
		public TerminalNode STARTS() { return getToken(JsonPathParser.STARTS, 0); }
		public TerminalNode WITH() { return getToken(JsonPathParser.WITH, 0); }
		public PathExpressionContext pathExpression() {
			return getRuleContext(PathExpressionContext.class,0);
		}
		public TerminalNode NAMED_VARIABLE() { return getToken(JsonPathParser.NAMED_VARIABLE, 0); }
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public StartsWithPredicateContext(PredicatePrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterStartsWithPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitStartsWithPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitStartsWithPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IsUnknownPredicateContext extends PredicatePrimaryContext {
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public TerminalNode IS() { return getToken(JsonPathParser.IS, 0); }
		public TerminalNode UNKNOWN() { return getToken(JsonPathParser.UNKNOWN, 0); }
		public IsUnknownPredicateContext(PredicatePrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterIsUnknownPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitIsUnknownPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitIsUnknownPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicatePrimaryContext predicatePrimary() throws RecognitionException {
		PredicatePrimaryContext _localctx = new PredicatePrimaryContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_predicatePrimary);
		try {
			setState(240);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
			case 1:
				_localctx = new PredicatePrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(215);
				delimitedPredicate();
				}
				break;
			case 2:
				_localctx = new ComparisonPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(216);
				((ComparisonPredicateContext)_localctx).left = pathExpression(0);
				setState(217);
				comparisonOperator();
				setState(218);
				((ComparisonPredicateContext)_localctx).right = pathExpression(0);
				}
				break;
			case 3:
				_localctx = new LikeRegexPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(220);
				((LikeRegexPredicateContext)_localctx).base = pathExpression(0);
				setState(221);
				match(LIKE_REGEX);
				setState(222);
				((LikeRegexPredicateContext)_localctx).pattern = stringLiteral();
				setState(225);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
				case 1:
					{
					setState(223);
					match(FLAG);
					setState(224);
					((LikeRegexPredicateContext)_localctx).flag = stringLiteral();
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new StartsWithPredicateContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(227);
				((StartsWithPredicateContext)_localctx).whole = pathExpression(0);
				setState(228);
				match(STARTS);
				setState(229);
				match(WITH);
				setState(232);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
					{
					setState(230);
					((StartsWithPredicateContext)_localctx).string = stringLiteral();
					}
					break;
				case NAMED_VARIABLE:
					{
					setState(231);
					match(NAMED_VARIABLE);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 5:
				_localctx = new IsUnknownPredicateContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(234);
				match(T__10);
				setState(235);
				predicate(0);
				setState(236);
				match(T__11);
				setState(237);
				match(IS);
				setState(238);
				match(UNKNOWN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DelimitedPredicateContext extends ParserRuleContext {
		public DelimitedPredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delimitedPredicate; }
	 
		public DelimitedPredicateContext() { }
		public void copyFrom(DelimitedPredicateContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsPredicateContext extends DelimitedPredicateContext {
		public TerminalNode EXISTS() { return getToken(JsonPathParser.EXISTS, 0); }
		public PathExpressionContext pathExpression() {
			return getRuleContext(PathExpressionContext.class,0);
		}
		public ExistsPredicateContext(DelimitedPredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterExistsPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitExistsPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitExistsPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedPredicateContext extends DelimitedPredicateContext {
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public ParenthesizedPredicateContext(DelimitedPredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterParenthesizedPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitParenthesizedPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitParenthesizedPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DelimitedPredicateContext delimitedPredicate() throws RecognitionException {
		DelimitedPredicateContext _localctx = new DelimitedPredicateContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_delimitedPredicate);
		try {
			setState(251);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EXISTS:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(242);
				match(EXISTS);
				setState(243);
				match(T__10);
				setState(244);
				pathExpression(0);
				setState(245);
				match(T__11);
				}
				break;
			case T__10:
				_localctx = new ParenthesizedPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(247);
				match(T__10);
				setState(248);
				predicate(0);
				setState(249);
				match(T__11);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(253);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 33292288L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ABS() { return getToken(JsonPathParser.ABS, 0); }
		public TerminalNode CEILING() { return getToken(JsonPathParser.CEILING, 0); }
		public TerminalNode DATETIME() { return getToken(JsonPathParser.DATETIME, 0); }
		public TerminalNode DOUBLE() { return getToken(JsonPathParser.DOUBLE, 0); }
		public TerminalNode EXISTS() { return getToken(JsonPathParser.EXISTS, 0); }
		public TerminalNode FALSE() { return getToken(JsonPathParser.FALSE, 0); }
		public TerminalNode FLAG() { return getToken(JsonPathParser.FLAG, 0); }
		public TerminalNode FLOOR() { return getToken(JsonPathParser.FLOOR, 0); }
		public TerminalNode IS() { return getToken(JsonPathParser.IS, 0); }
		public TerminalNode KEYVALUE() { return getToken(JsonPathParser.KEYVALUE, 0); }
		public TerminalNode LAST() { return getToken(JsonPathParser.LAST, 0); }
		public TerminalNode LAX() { return getToken(JsonPathParser.LAX, 0); }
		public TerminalNode LIKE_REGEX() { return getToken(JsonPathParser.LIKE_REGEX, 0); }
		public TerminalNode MINUS() { return getToken(JsonPathParser.MINUS, 0); }
		public TerminalNode NULL() { return getToken(JsonPathParser.NULL, 0); }
		public TerminalNode SIZE() { return getToken(JsonPathParser.SIZE, 0); }
		public TerminalNode STARTS() { return getToken(JsonPathParser.STARTS, 0); }
		public TerminalNode STRICT() { return getToken(JsonPathParser.STRICT, 0); }
		public TerminalNode TO() { return getToken(JsonPathParser.TO, 0); }
		public TerminalNode TRUE() { return getToken(JsonPathParser.TRUE, 0); }
		public TerminalNode TYPE() { return getToken(JsonPathParser.TYPE, 0); }
		public TerminalNode UNKNOWN() { return getToken(JsonPathParser.UNKNOWN, 0); }
		public TerminalNode WITH() { return getToken(JsonPathParser.WITH, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof JsonPathListener ) ((JsonPathListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JsonPathVisitor ) return ((JsonPathVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(255);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 281474943156224L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 2:
			return pathExpression_sempred((PathExpressionContext)_localctx, predIndex);
		case 3:
			return accessorExpression_sempred((AccessorExpressionContext)_localctx, predIndex);
		case 13:
			return predicate_sempred((PredicateContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean pathExpression_sempred(PathExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean accessorExpression_sempred(AccessorExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 16);
		case 3:
			return precpred(_ctx, 15);
		case 4:
			return precpred(_ctx, 14);
		case 5:
			return precpred(_ctx, 13);
		case 6:
			return precpred(_ctx, 12);
		case 7:
			return precpred(_ctx, 11);
		case 8:
			return precpred(_ctx, 10);
		case 9:
			return precpred(_ctx, 9);
		case 10:
			return precpred(_ctx, 8);
		case 11:
			return precpred(_ctx, 7);
		case 12:
			return precpred(_ctx, 6);
		case 13:
			return precpred(_ctx, 5);
		case 14:
			return precpred(_ctx, 4);
		case 15:
			return precpred(_ctx, 3);
		case 16:
			return precpred(_ctx, 2);
		case 17:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean predicate_sempred(PredicateContext _localctx, int predIndex) {
		switch (predIndex) {
		case 18:
			return precpred(_ctx, 2);
		case 19:
			return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u00018\u0102\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0001\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0003\u0002/\b\u0002\u0001\u0002\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0005\u00027\b\u0002"+
		"\n\u0002\f\u0002:\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0005\u0003S\b\u0003\n\u0003\f\u0003V\t\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u0087\b\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0005\u0003\u008f\b\u0003\n\u0003\f\u0003\u0092\t\u0003\u0001\u0004\u0001"+
		"\u0004\u0003\u0004\u0096\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u009d\b\u0005\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u00a5\b\u0006\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u00ab\b\u0007\u0001"+
		"\b\u0003\b\u00ae\b\b\u0001\b\u0001\b\u0003\b\u00b2\b\b\u0001\b\u0001\b"+
		"\u0003\b\u00b6\b\b\u0001\b\u0003\b\u00b9\b\b\u0001\t\u0001\t\u0001\n\u0001"+
		"\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0003\f\u00c5"+
		"\b\f\u0001\r\u0001\r\u0001\r\u0001\r\u0003\r\u00cb\b\r\u0001\r\u0001\r"+
		"\u0001\r\u0001\r\u0001\r\u0001\r\u0005\r\u00d3\b\r\n\r\f\r\u00d6\t\r\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u00e2\b\u000e\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u00e9"+
		"\b\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0001"+
		"\u000e\u0003\u000e\u00f1\b\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0003"+
		"\u000f\u00fc\b\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001"+
		"\u0011\u0000\u0003\u0004\u0006\u001a\u0012\u0000\u0002\u0004\u0006\b\n"+
		"\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"\u0000\u0006"+
		"\u0002\u0000$$**\u0002\u0000\u0001\u0001&&\u0001\u0000\u0002\u0004\u0002"+
		"\u0000\u001e\u001e,,\u0001\u0000\u0012\u0018\u0001\u0000\u0019/\u011d"+
		"\u0000$\u0001\u0000\u0000\u0000\u0002(\u0001\u0000\u0000\u0000\u0004."+
		"\u0001\u0000\u0000\u0000\u0006;\u0001\u0000\u0000\u0000\b\u0095\u0001"+
		"\u0000\u0000\u0000\n\u009c\u0001\u0000\u0000\u0000\f\u00a4\u0001\u0000"+
		"\u0000\u0000\u000e\u00aa\u0001\u0000\u0000\u0000\u0010\u00b8\u0001\u0000"+
		"\u0000\u0000\u0012\u00ba\u0001\u0000\u0000\u0000\u0014\u00bc\u0001\u0000"+
		"\u0000\u0000\u0016\u00be\u0001\u0000\u0000\u0000\u0018\u00c4\u0001\u0000"+
		"\u0000\u0000\u001a\u00ca\u0001\u0000\u0000\u0000\u001c\u00f0\u0001\u0000"+
		"\u0000\u0000\u001e\u00fb\u0001\u0000\u0000\u0000 \u00fd\u0001\u0000\u0000"+
		"\u0000\"\u00ff\u0001\u0000\u0000\u0000$%\u0003\u0002\u0001\u0000%&\u0003"+
		"\u0004\u0002\u0000&\'\u0005\u0000\u0000\u0001\'\u0001\u0001\u0000\u0000"+
		"\u0000()\u0007\u0000\u0000\u0000)\u0003\u0001\u0000\u0000\u0000*+\u0006"+
		"\u0002\uffff\uffff\u0000+/\u0003\u0006\u0003\u0000,-\u0007\u0001\u0000"+
		"\u0000-/\u0003\u0004\u0002\u0003.*\u0001\u0000\u0000\u0000.,\u0001\u0000"+
		"\u0000\u0000/8\u0001\u0000\u0000\u000001\n\u0002\u0000\u000012\u0007\u0002"+
		"\u0000\u000027\u0003\u0004\u0002\u000334\n\u0001\u0000\u000045\u0007\u0001"+
		"\u0000\u000057\u0003\u0004\u0002\u000260\u0001\u0000\u0000\u000063\u0001"+
		"\u0000\u0000\u00007:\u0001\u0000\u0000\u000086\u0001\u0000\u0000\u0000"+
		"89\u0001\u0000\u0000\u00009\u0005\u0001\u0000\u0000\u0000:8\u0001\u0000"+
		"\u0000\u0000;<\u0006\u0003\uffff\uffff\u0000<=\u0003\f\u0006\u0000=\u0090"+
		"\u0001\u0000\u0000\u0000>?\n\u0010\u0000\u0000?@\u0005\u0005\u0000\u0000"+
		"@\u008f\u0003\b\u0004\u0000AB\n\u000f\u0000\u0000BC\u0005\u0005\u0000"+
		"\u0000C\u008f\u0003\u0012\t\u0000DE\n\u000e\u0000\u0000EF\u0005\u0005"+
		"\u0000\u0000F\u008f\u0005\u0002\u0000\u0000GH\n\r\u0000\u0000HI\u0005"+
		"\u0006\u0000\u0000I\u008f\u0003\b\u0004\u0000JK\n\f\u0000\u0000KL\u0005"+
		"\u0006\u0000\u0000L\u008f\u0003\u0012\t\u0000MN\n\u000b\u0000\u0000NO"+
		"\u0005\u0007\u0000\u0000OT\u0003\n\u0005\u0000PQ\u0005\b\u0000\u0000Q"+
		"S\u0003\n\u0005\u0000RP\u0001\u0000\u0000\u0000SV\u0001\u0000\u0000\u0000"+
		"TR\u0001\u0000\u0000\u0000TU\u0001\u0000\u0000\u0000UW\u0001\u0000\u0000"+
		"\u0000VT\u0001\u0000\u0000\u0000WX\u0005\t\u0000\u0000X\u008f\u0001\u0000"+
		"\u0000\u0000YZ\n\n\u0000\u0000Z[\u0005\u0007\u0000\u0000[\\\u0005\u0002"+
		"\u0000\u0000\\\u008f\u0005\t\u0000\u0000]^\n\t\u0000\u0000^_\u0005\n\u0000"+
		"\u0000_`\u0005\u000b\u0000\u0000`a\u0003\u001a\r\u0000ab\u0005\f\u0000"+
		"\u0000b\u008f\u0001\u0000\u0000\u0000cd\n\b\u0000\u0000de\u0005\u0005"+
		"\u0000\u0000ef\u0005-\u0000\u0000fg\u0005\u000b\u0000\u0000g\u008f\u0005"+
		"\f\u0000\u0000hi\n\u0007\u0000\u0000ij\u0005\u0005\u0000\u0000jk\u0005"+
		"(\u0000\u0000kl\u0005\u000b\u0000\u0000l\u008f\u0005\f\u0000\u0000mn\n"+
		"\u0006\u0000\u0000no\u0005\u0005\u0000\u0000op\u0005\u001c\u0000\u0000"+
		"pq\u0005\u000b\u0000\u0000q\u008f\u0005\f\u0000\u0000rs\n\u0005\u0000"+
		"\u0000st\u0005\u0005\u0000\u0000tu\u0005\u001a\u0000\u0000uv\u0005\u000b"+
		"\u0000\u0000v\u008f\u0005\f\u0000\u0000wx\n\u0004\u0000\u0000xy\u0005"+
		"\u0005\u0000\u0000yz\u0005 \u0000\u0000z{\u0005\u000b\u0000\u0000{\u008f"+
		"\u0005\f\u0000\u0000|}\n\u0003\u0000\u0000}~\u0005\u0005\u0000\u0000~"+
		"\u007f\u0005\u0019\u0000\u0000\u007f\u0080\u0005\u000b\u0000\u0000\u0080"+
		"\u008f\u0005\f\u0000\u0000\u0081\u0082\n\u0002\u0000\u0000\u0082\u0083"+
		"\u0005\u0005\u0000\u0000\u0083\u0084\u0005\u001b\u0000\u0000\u0084\u0086"+
		"\u0005\u000b\u0000\u0000\u0085\u0087\u0003\u0012\t\u0000\u0086\u0085\u0001"+
		"\u0000\u0000\u0000\u0086\u0087\u0001\u0000\u0000\u0000\u0087\u0088\u0001"+
		"\u0000\u0000\u0000\u0088\u008f\u0005\f\u0000\u0000\u0089\u008a\n\u0001"+
		"\u0000\u0000\u008a\u008b\u0005\u0005\u0000\u0000\u008b\u008c\u0005\"\u0000"+
		"\u0000\u008c\u008d\u0005\u000b\u0000\u0000\u008d\u008f\u0005\f\u0000\u0000"+
		"\u008e>\u0001\u0000\u0000\u0000\u008eA\u0001\u0000\u0000\u0000\u008eD"+
		"\u0001\u0000\u0000\u0000\u008eG\u0001\u0000\u0000\u0000\u008eJ\u0001\u0000"+
		"\u0000\u0000\u008eM\u0001\u0000\u0000\u0000\u008eY\u0001\u0000\u0000\u0000"+
		"\u008e]\u0001\u0000\u0000\u0000\u008ec\u0001\u0000\u0000\u0000\u008eh"+
		"\u0001\u0000\u0000\u0000\u008em\u0001\u0000\u0000\u0000\u008er\u0001\u0000"+
		"\u0000\u0000\u008ew\u0001\u0000\u0000\u0000\u008e|\u0001\u0000\u0000\u0000"+
		"\u008e\u0081\u0001\u0000\u0000\u0000\u008e\u0089\u0001\u0000\u0000\u0000"+
		"\u008f\u0092\u0001\u0000\u0000\u0000\u0090\u008e\u0001\u0000\u0000\u0000"+
		"\u0090\u0091\u0001\u0000\u0000\u0000\u0091\u0007\u0001\u0000\u0000\u0000"+
		"\u0092\u0090\u0001\u0000\u0000\u0000\u0093\u0096\u00054\u0000\u0000\u0094"+
		"\u0096\u0003\"\u0011\u0000\u0095\u0093\u0001\u0000\u0000\u0000\u0095\u0094"+
		"\u0001\u0000\u0000\u0000\u0096\t\u0001\u0000\u0000\u0000\u0097\u009d\u0003"+
		"\u0004\u0002\u0000\u0098\u0099\u0003\u0004\u0002\u0000\u0099\u009a\u0005"+
		"+\u0000\u0000\u009a\u009b\u0003\u0004\u0002\u0000\u009b\u009d\u0001\u0000"+
		"\u0000\u0000\u009c\u0097\u0001\u0000\u0000\u0000\u009c\u0098\u0001\u0000"+
		"\u0000\u0000\u009d\u000b\u0001\u0000\u0000\u0000\u009e\u00a5\u0003\u000e"+
		"\u0007\u0000\u009f\u00a5\u0003\u0018\f\u0000\u00a0\u00a1\u0005\u000b\u0000"+
		"\u0000\u00a1\u00a2\u0003\u0004\u0002\u0000\u00a2\u00a3\u0005\f\u0000\u0000"+
		"\u00a3\u00a5\u0001\u0000\u0000\u0000\u00a4\u009e\u0001\u0000\u0000\u0000"+
		"\u00a4\u009f\u0001\u0000\u0000\u0000\u00a4\u00a0\u0001\u0000\u0000\u0000"+
		"\u00a5\r\u0001\u0000\u0000\u0000\u00a6\u00ab\u0003\u0010\b\u0000\u00a7"+
		"\u00ab\u0003\u0012\t\u0000\u00a8\u00ab\u0003\u0014\n\u0000\u00a9\u00ab"+
		"\u0003\u0016\u000b\u0000\u00aa\u00a6\u0001\u0000\u0000\u0000\u00aa\u00a7"+
		"\u0001\u0000\u0000\u0000\u00aa\u00a8\u0001\u0000\u0000\u0000\u00aa\u00a9"+
		"\u0001\u0000\u0000\u0000\u00ab\u000f\u0001\u0000\u0000\u0000\u00ac\u00ae"+
		"\u0005&\u0000\u0000\u00ad\u00ac\u0001\u0000\u0000\u0000\u00ad\u00ae\u0001"+
		"\u0000\u0000\u0000\u00ae\u00af\u0001\u0000\u0000\u0000\u00af\u00b9\u0005"+
		"0\u0000\u0000\u00b0\u00b2\u0005&\u0000\u0000\u00b1\u00b0\u0001\u0000\u0000"+
		"\u0000\u00b1\u00b2\u0001\u0000\u0000\u0000\u00b2\u00b3\u0001\u0000\u0000"+
		"\u0000\u00b3\u00b9\u00051\u0000\u0000\u00b4\u00b6\u0005&\u0000\u0000\u00b5"+
		"\u00b4\u0001\u0000\u0000\u0000\u00b5\u00b6\u0001\u0000\u0000\u0000\u00b6"+
		"\u00b7\u0001\u0000\u0000\u0000\u00b7\u00b9\u00052\u0000\u0000\u00b8\u00ad"+
		"\u0001\u0000\u0000\u0000\u00b8\u00b1\u0001\u0000\u0000\u0000\u00b8\u00b5"+
		"\u0001\u0000\u0000\u0000\u00b9\u0011\u0001\u0000\u0000\u0000\u00ba\u00bb"+
		"\u00053\u0000\u0000\u00bb\u0013\u0001\u0000\u0000\u0000\u00bc\u00bd\u0005"+
		"\'\u0000\u0000\u00bd\u0015\u0001\u0000\u0000\u0000\u00be\u00bf\u0007\u0003"+
		"\u0000\u0000\u00bf\u0017\u0001\u0000\u0000\u0000\u00c0\u00c5\u0005\r\u0000"+
		"\u0000\u00c1\u00c5\u00055\u0000\u0000\u00c2\u00c5\u0005#\u0000\u0000\u00c3"+
		"\u00c5\u0005\u000e\u0000\u0000\u00c4\u00c0\u0001\u0000\u0000\u0000\u00c4"+
		"\u00c1\u0001\u0000\u0000\u0000\u00c4\u00c2\u0001\u0000\u0000\u0000\u00c4"+
		"\u00c3\u0001\u0000\u0000\u0000\u00c5\u0019\u0001\u0000\u0000\u0000\u00c6"+
		"\u00c7\u0006\r\uffff\uffff\u0000\u00c7\u00cb\u0003\u001c\u000e\u0000\u00c8"+
		"\u00c9\u0005\u000f\u0000\u0000\u00c9\u00cb\u0003\u001e\u000f\u0000\u00ca"+
		"\u00c6\u0001\u0000\u0000\u0000\u00ca\u00c8\u0001\u0000\u0000\u0000\u00cb"+
		"\u00d4\u0001\u0000\u0000\u0000\u00cc\u00cd\n\u0002\u0000\u0000\u00cd\u00ce"+
		"\u0005\u0010\u0000\u0000\u00ce\u00d3\u0003\u001a\r\u0003\u00cf\u00d0\n"+
		"\u0001\u0000\u0000\u00d0\u00d1\u0005\u0011\u0000\u0000\u00d1\u00d3\u0003"+
		"\u001a\r\u0002\u00d2\u00cc\u0001\u0000\u0000\u0000\u00d2\u00cf\u0001\u0000"+
		"\u0000\u0000\u00d3\u00d6\u0001\u0000\u0000\u0000\u00d4\u00d2\u0001\u0000"+
		"\u0000\u0000\u00d4\u00d5\u0001\u0000\u0000\u0000\u00d5\u001b\u0001\u0000"+
		"\u0000\u0000\u00d6\u00d4\u0001\u0000\u0000\u0000\u00d7\u00f1\u0003\u001e"+
		"\u000f\u0000\u00d8\u00d9\u0003\u0004\u0002\u0000\u00d9\u00da\u0003 \u0010"+
		"\u0000\u00da\u00db\u0003\u0004\u0002\u0000\u00db\u00f1\u0001\u0000\u0000"+
		"\u0000\u00dc\u00dd\u0003\u0004\u0002\u0000\u00dd\u00de\u0005%\u0000\u0000"+
		"\u00de\u00e1\u0003\u0012\t\u0000\u00df\u00e0\u0005\u001f\u0000\u0000\u00e0"+
		"\u00e2\u0003\u0012\t\u0000\u00e1\u00df\u0001\u0000\u0000\u0000\u00e1\u00e2"+
		"\u0001\u0000\u0000\u0000\u00e2\u00f1\u0001\u0000\u0000\u0000\u00e3\u00e4"+
		"\u0003\u0004\u0002\u0000\u00e4\u00e5\u0005)\u0000\u0000\u00e5\u00e8\u0005"+
		"/\u0000\u0000\u00e6\u00e9\u0003\u0012\t\u0000\u00e7\u00e9\u00055\u0000"+
		"\u0000\u00e8\u00e6\u0001\u0000\u0000\u0000\u00e8\u00e7\u0001\u0000\u0000"+
		"\u0000\u00e9\u00f1\u0001\u0000\u0000\u0000\u00ea\u00eb\u0005\u000b\u0000"+
		"\u0000\u00eb\u00ec\u0003\u001a\r\u0000\u00ec\u00ed\u0005\f\u0000\u0000"+
		"\u00ed\u00ee\u0005!\u0000\u0000\u00ee\u00ef\u0005.\u0000\u0000\u00ef\u00f1"+
		"\u0001\u0000\u0000\u0000\u00f0\u00d7\u0001\u0000\u0000\u0000\u00f0\u00d8"+
		"\u0001\u0000\u0000\u0000\u00f0\u00dc\u0001\u0000\u0000\u0000\u00f0\u00e3"+
		"\u0001\u0000\u0000\u0000\u00f0\u00ea\u0001\u0000\u0000\u0000\u00f1\u001d"+
		"\u0001\u0000\u0000\u0000\u00f2\u00f3\u0005\u001d\u0000\u0000\u00f3\u00f4"+
		"\u0005\u000b\u0000\u0000\u00f4\u00f5\u0003\u0004\u0002\u0000\u00f5\u00f6"+
		"\u0005\f\u0000\u0000\u00f6\u00fc\u0001\u0000\u0000\u0000\u00f7\u00f8\u0005"+
		"\u000b\u0000\u0000\u00f8\u00f9\u0003\u001a\r\u0000\u00f9\u00fa\u0005\f"+
		"\u0000\u0000\u00fa\u00fc\u0001\u0000\u0000\u0000\u00fb\u00f2\u0001\u0000"+
		"\u0000\u0000\u00fb\u00f7\u0001\u0000\u0000\u0000\u00fc\u001f\u0001\u0000"+
		"\u0000\u0000\u00fd\u00fe\u0007\u0004\u0000\u0000\u00fe!\u0001\u0000\u0000"+
		"\u0000\u00ff\u0100\u0007\u0005\u0000\u0000\u0100#\u0001\u0000\u0000\u0000"+
		"\u0017.68T\u0086\u008e\u0090\u0095\u009c\u00a4\u00aa\u00ad\u00b1\u00b5"+
		"\u00b8\u00c4\u00ca\u00d2\u00d4\u00e1\u00e8\u00f0\u00fb";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}