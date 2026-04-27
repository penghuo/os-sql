// Generated from /home/penghuo/oss/trino/core/trino-grammar/src/main/antlr4/io/trino/grammar/type/TypeCalculation.g4 by ANTLR 4.13.1
package io.trino.grammar.type;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class TypeCalculationLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, PLUS=4, MINUS=5, ASTERISK=6, SLASH=7, NULL=8, 
		MIN=9, MAX=10, IDENTIFIER=11, INTEGER_VALUE=12, WS=13;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "PLUS", "MINUS", "ASTERISK", "SLASH", "NULL", 
			"MIN", "MAX", "IDENTIFIER", "INTEGER_VALUE", "DIGIT", "LETTER", "WS"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "','", "')'", "'+'", "'-'", "'*'", "'/'", "'NULL'", "'MIN'", 
			"'MAX'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, "PLUS", "MINUS", "ASTERISK", "SLASH", "NULL", 
			"MIN", "MAX", "IDENTIFIER", "INTEGER_VALUE", "WS"
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


	public TypeCalculationLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "TypeCalculation.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\u0004\u0000\rV\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
		"\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
		"\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
		"\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
		"\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0001"+
		"\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001"+
		"\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001"+
		"\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001\t\u0001\t"+
		"\u0001\n\u0001\n\u0003\n=\b\n\u0001\n\u0001\n\u0001\n\u0005\nB\b\n\n\n"+
		"\f\nE\t\n\u0001\u000b\u0004\u000bH\b\u000b\u000b\u000b\f\u000bI\u0001"+
		"\f\u0001\f\u0001\r\u0001\r\u0001\u000e\u0004\u000eQ\b\u000e\u000b\u000e"+
		"\f\u000eR\u0001\u000e\u0001\u000e\u0000\u0000\u000f\u0001\u0001\u0003"+
		"\u0002\u0005\u0003\u0007\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011"+
		"\t\u0013\n\u0015\u000b\u0017\f\u0019\u0000\u001b\u0000\u001d\r\u0001\u0000"+
		"\t\u0002\u0000NNnn\u0002\u0000UUuu\u0002\u0000LLll\u0002\u0000MMmm\u0002"+
		"\u0000IIii\u0002\u0000AAaa\u0002\u0000XXxx\u0002\u0000AZaz\u0003\u0000"+
		"\t\n\r\r  Y\u0000\u0001\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000"+
		"\u0000\u0000\u0000\u0005\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000"+
		"\u0000\u0000\u0000\t\u0001\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000"+
		"\u0000\u0000\r\u0001\u0000\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000"+
		"\u0000\u0011\u0001\u0000\u0000\u0000\u0000\u0013\u0001\u0000\u0000\u0000"+
		"\u0000\u0015\u0001\u0000\u0000\u0000\u0000\u0017\u0001\u0000\u0000\u0000"+
		"\u0000\u001d\u0001\u0000\u0000\u0000\u0001\u001f\u0001\u0000\u0000\u0000"+
		"\u0003!\u0001\u0000\u0000\u0000\u0005#\u0001\u0000\u0000\u0000\u0007%"+
		"\u0001\u0000\u0000\u0000\t\'\u0001\u0000\u0000\u0000\u000b)\u0001\u0000"+
		"\u0000\u0000\r+\u0001\u0000\u0000\u0000\u000f-\u0001\u0000\u0000\u0000"+
		"\u00112\u0001\u0000\u0000\u0000\u00136\u0001\u0000\u0000\u0000\u0015<"+
		"\u0001\u0000\u0000\u0000\u0017G\u0001\u0000\u0000\u0000\u0019K\u0001\u0000"+
		"\u0000\u0000\u001bM\u0001\u0000\u0000\u0000\u001dP\u0001\u0000\u0000\u0000"+
		"\u001f \u0005(\u0000\u0000 \u0002\u0001\u0000\u0000\u0000!\"\u0005,\u0000"+
		"\u0000\"\u0004\u0001\u0000\u0000\u0000#$\u0005)\u0000\u0000$\u0006\u0001"+
		"\u0000\u0000\u0000%&\u0005+\u0000\u0000&\b\u0001\u0000\u0000\u0000\'("+
		"\u0005-\u0000\u0000(\n\u0001\u0000\u0000\u0000)*\u0005*\u0000\u0000*\f"+
		"\u0001\u0000\u0000\u0000+,\u0005/\u0000\u0000,\u000e\u0001\u0000\u0000"+
		"\u0000-.\u0007\u0000\u0000\u0000./\u0007\u0001\u0000\u0000/0\u0007\u0002"+
		"\u0000\u000001\u0007\u0002\u0000\u00001\u0010\u0001\u0000\u0000\u0000"+
		"23\u0007\u0003\u0000\u000034\u0007\u0004\u0000\u000045\u0007\u0000\u0000"+
		"\u00005\u0012\u0001\u0000\u0000\u000067\u0007\u0003\u0000\u000078\u0007"+
		"\u0005\u0000\u000089\u0007\u0006\u0000\u00009\u0014\u0001\u0000\u0000"+
		"\u0000:=\u0003\u001b\r\u0000;=\u0005_\u0000\u0000<:\u0001\u0000\u0000"+
		"\u0000<;\u0001\u0000\u0000\u0000=C\u0001\u0000\u0000\u0000>B\u0003\u001b"+
		"\r\u0000?B\u0003\u0019\f\u0000@B\u0005_\u0000\u0000A>\u0001\u0000\u0000"+
		"\u0000A?\u0001\u0000\u0000\u0000A@\u0001\u0000\u0000\u0000BE\u0001\u0000"+
		"\u0000\u0000CA\u0001\u0000\u0000\u0000CD\u0001\u0000\u0000\u0000D\u0016"+
		"\u0001\u0000\u0000\u0000EC\u0001\u0000\u0000\u0000FH\u0003\u0019\f\u0000"+
		"GF\u0001\u0000\u0000\u0000HI\u0001\u0000\u0000\u0000IG\u0001\u0000\u0000"+
		"\u0000IJ\u0001\u0000\u0000\u0000J\u0018\u0001\u0000\u0000\u0000KL\u0002"+
		"09\u0000L\u001a\u0001\u0000\u0000\u0000MN\u0007\u0007\u0000\u0000N\u001c"+
		"\u0001\u0000\u0000\u0000OQ\u0007\b\u0000\u0000PO\u0001\u0000\u0000\u0000"+
		"QR\u0001\u0000\u0000\u0000RP\u0001\u0000\u0000\u0000RS\u0001\u0000\u0000"+
		"\u0000ST\u0001\u0000\u0000\u0000TU\u0006\u000e\u0000\u0000U\u001e\u0001"+
		"\u0000\u0000\u0000\u0006\u0000<ACIR\u0001\u0000\u0001\u0000";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}