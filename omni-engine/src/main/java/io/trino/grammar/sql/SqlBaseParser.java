// Generated from /home/penghuo/oss/trino/core/trino-grammar/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4 by ANTLR 4.13.1
package io.trino.grammar.sql;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		ABSENT=18, ADD=19, ADMIN=20, AFTER=21, ALL=22, ALTER=23, ANALYZE=24, AND=25, 
		ANY=26, ARRAY=27, AS=28, ASC=29, AT=30, AUTHORIZATION=31, BEGIN=32, BERNOULLI=33, 
		BETWEEN=34, BOTH=35, BY=36, CALL=37, CALLED=38, CASCADE=39, CASE=40, CAST=41, 
		CATALOG=42, CATALOGS=43, COLUMN=44, COLUMNS=45, COMMENT=46, COMMIT=47, 
		COMMITTED=48, CONDITIONAL=49, CONSTRAINT=50, COUNT=51, COPARTITION=52, 
		CREATE=53, CROSS=54, CUBE=55, CURRENT=56, CURRENT_CATALOG=57, CURRENT_DATE=58, 
		CURRENT_PATH=59, CURRENT_ROLE=60, CURRENT_SCHEMA=61, CURRENT_TIME=62, 
		CURRENT_TIMESTAMP=63, CURRENT_USER=64, DATA=65, DATE=66, DAY=67, DEALLOCATE=68, 
		DECLARE=69, DEFAULT=70, DEFINE=71, DEFINER=72, DELETE=73, DENY=74, DESC=75, 
		DESCRIBE=76, DESCRIPTOR=77, DETERMINISTIC=78, DISTINCT=79, DISTRIBUTED=80, 
		DO=81, DOUBLE=82, DROP=83, ELSE=84, EMPTY=85, ELSEIF=86, ENCODING=87, 
		END=88, ERROR=89, ESCAPE=90, EXCEPT=91, EXCLUDING=92, EXECUTE=93, EXISTS=94, 
		EXPLAIN=95, EXTRACT=96, FALSE=97, FETCH=98, FILTER=99, FINAL=100, FIRST=101, 
		FOLLOWING=102, FOR=103, FORMAT=104, FROM=105, FULL=106, FUNCTION=107, 
		FUNCTIONS=108, GRACE=109, GRANT=110, GRANTED=111, GRANTS=112, GRAPHVIZ=113, 
		GROUP=114, GROUPING=115, GROUPS=116, HAVING=117, HOUR=118, IF=119, IGNORE=120, 
		IMMEDIATE=121, IN=122, INCLUDING=123, INITIAL=124, INNER=125, INPUT=126, 
		INSERT=127, INTERSECT=128, INTERVAL=129, INTO=130, INVOKER=131, IO=132, 
		IS=133, ISOLATION=134, ITERATE=135, JOIN=136, JSON=137, JSON_ARRAY=138, 
		JSON_EXISTS=139, JSON_OBJECT=140, JSON_QUERY=141, JSON_TABLE=142, JSON_VALUE=143, 
		KEEP=144, KEY=145, KEYS=146, LANGUAGE=147, LAST=148, LATERAL=149, LEADING=150, 
		LEAVE=151, LEFT=152, LEVEL=153, LIKE=154, LIMIT=155, LISTAGG=156, LOCAL=157, 
		LOCALTIME=158, LOCALTIMESTAMP=159, LOGICAL=160, LOOP=161, MAP=162, MATCH=163, 
		MATCHED=164, MATCHES=165, MATCH_RECOGNIZE=166, MATERIALIZED=167, MEASURES=168, 
		MERGE=169, MINUTE=170, MONTH=171, NATURAL=172, NESTED=173, NEXT=174, NFC=175, 
		NFD=176, NFKC=177, NFKD=178, NO=179, NONE=180, NORMALIZE=181, NOT=182, 
		NULL=183, NULLIF=184, NULLS=185, OBJECT=186, OF=187, OFFSET=188, OMIT=189, 
		ON=190, ONE=191, ONLY=192, OPTION=193, OR=194, ORDER=195, ORDINALITY=196, 
		OUTER=197, OUTPUT=198, OVER=199, OVERFLOW=200, PARTITION=201, PARTITIONS=202, 
		PASSING=203, PAST=204, PATH=205, PATTERN=206, PER=207, PERIOD=208, PERMUTE=209, 
		PLAN=210, POSITION=211, PRECEDING=212, PRECISION=213, PREPARE=214, PRIVILEGES=215, 
		PROPERTIES=216, PRUNE=217, QUOTES=218, RANGE=219, READ=220, RECURSIVE=221, 
		REFRESH=222, RENAME=223, REPEAT=224, REPEATABLE=225, REPLACE=226, RESET=227, 
		RESPECT=228, RESTRICT=229, RETURN=230, RETURNING=231, RETURNS=232, REVOKE=233, 
		RIGHT=234, ROLE=235, ROLES=236, ROLLBACK=237, ROLLUP=238, ROW=239, ROWS=240, 
		RUNNING=241, SCALAR=242, SCHEMA=243, SCHEMAS=244, SECOND=245, SECURITY=246, 
		SEEK=247, SELECT=248, SERIALIZABLE=249, SESSION=250, SET=251, SETS=252, 
		SHOW=253, SOME=254, START=255, STATS=256, SUBSET=257, SUBSTRING=258, SYSTEM=259, 
		TABLE=260, TABLES=261, TABLESAMPLE=262, TEXT=263, TEXT_STRING=264, THEN=265, 
		TIES=266, TIME=267, TIMESTAMP=268, TO=269, TRAILING=270, TRANSACTION=271, 
		TRIM=272, TRUE=273, TRUNCATE=274, TRY_CAST=275, TYPE=276, UESCAPE=277, 
		UNBOUNDED=278, UNCOMMITTED=279, UNCONDITIONAL=280, UNION=281, UNIQUE=282, 
		UNKNOWN=283, UNMATCHED=284, UNNEST=285, UNTIL=286, UPDATE=287, USE=288, 
		USER=289, USING=290, UTF16=291, UTF32=292, UTF8=293, VALIDATE=294, VALUE=295, 
		VALUES=296, VERBOSE=297, VERSION=298, VIEW=299, WHEN=300, WHERE=301, WHILE=302, 
		WINDOW=303, WITH=304, WITHIN=305, WITHOUT=306, WORK=307, WRAPPER=308, 
		WRITE=309, YEAR=310, ZONE=311, EQ=312, NEQ=313, LT=314, LTE=315, GT=316, 
		GTE=317, PLUS=318, MINUS=319, ASTERISK=320, SLASH=321, PERCENT=322, CONCAT=323, 
		QUESTION_MARK=324, SEMICOLON=325, STRING=326, UNICODE_STRING=327, BINARY_LITERAL=328, 
		INTEGER_VALUE=329, DECIMAL_VALUE=330, DOUBLE_VALUE=331, IDENTIFIER=332, 
		DIGIT_IDENTIFIER=333, QUOTED_IDENTIFIER=334, BACKQUOTED_IDENTIFIER=335, 
		SIMPLE_COMMENT=336, BRACKETED_COMMENT=337, WS=338, UNRECOGNIZED=339, DELIMITER=340;
	public static final int
		RULE_singleStatement = 0, RULE_standaloneExpression = 1, RULE_standalonePathSpecification = 2, 
		RULE_standaloneType = 3, RULE_standaloneRowPattern = 4, RULE_standaloneFunctionSpecification = 5, 
		RULE_statement = 6, RULE_rootQuery = 7, RULE_withFunction = 8, RULE_query = 9, 
		RULE_with = 10, RULE_tableElement = 11, RULE_columnDefinition = 12, RULE_likeClause = 13, 
		RULE_properties = 14, RULE_propertyAssignments = 15, RULE_property = 16, 
		RULE_propertyValue = 17, RULE_queryNoWith = 18, RULE_limitRowCount = 19, 
		RULE_rowCount = 20, RULE_queryTerm = 21, RULE_queryPrimary = 22, RULE_sortItem = 23, 
		RULE_querySpecification = 24, RULE_groupBy = 25, RULE_groupingElement = 26, 
		RULE_groupingSet = 27, RULE_windowDefinition = 28, RULE_windowSpecification = 29, 
		RULE_namedQuery = 30, RULE_setQuantifier = 31, RULE_selectItem = 32, RULE_relation = 33, 
		RULE_joinType = 34, RULE_joinCriteria = 35, RULE_sampledRelation = 36, 
		RULE_sampleType = 37, RULE_trimsSpecification = 38, RULE_listAggOverflowBehavior = 39, 
		RULE_listaggCountIndication = 40, RULE_patternRecognition = 41, RULE_measureDefinition = 42, 
		RULE_rowsPerMatch = 43, RULE_emptyMatchHandling = 44, RULE_skipTo = 45, 
		RULE_subsetDefinition = 46, RULE_variableDefinition = 47, RULE_aliasedRelation = 48, 
		RULE_columnAliases = 49, RULE_relationPrimary = 50, RULE_jsonTableColumn = 51, 
		RULE_jsonTableSpecificPlan = 52, RULE_jsonTablePathName = 53, RULE_planPrimary = 54, 
		RULE_jsonTableDefaultPlan = 55, RULE_tableFunctionCall = 56, RULE_tableFunctionArgument = 57, 
		RULE_tableArgument = 58, RULE_tableArgumentRelation = 59, RULE_descriptorArgument = 60, 
		RULE_descriptorField = 61, RULE_copartitionTables = 62, RULE_expression = 63, 
		RULE_booleanExpression = 64, RULE_predicate = 65, RULE_valueExpression = 66, 
		RULE_primaryExpression = 67, RULE_jsonPathInvocation = 68, RULE_jsonValueExpression = 69, 
		RULE_jsonRepresentation = 70, RULE_jsonArgument = 71, RULE_jsonExistsErrorBehavior = 72, 
		RULE_jsonValueBehavior = 73, RULE_jsonQueryWrapperBehavior = 74, RULE_jsonQueryBehavior = 75, 
		RULE_jsonObjectMember = 76, RULE_processingMode = 77, RULE_nullTreatment = 78, 
		RULE_string = 79, RULE_timeZoneSpecifier = 80, RULE_comparisonOperator = 81, 
		RULE_comparisonQuantifier = 82, RULE_booleanValue = 83, RULE_interval = 84, 
		RULE_intervalField = 85, RULE_normalForm = 86, RULE_type = 87, RULE_rowField = 88, 
		RULE_typeParameter = 89, RULE_whenClause = 90, RULE_filter = 91, RULE_mergeCase = 92, 
		RULE_over = 93, RULE_windowFrame = 94, RULE_frameExtent = 95, RULE_frameBound = 96, 
		RULE_rowPattern = 97, RULE_patternPrimary = 98, RULE_patternQuantifier = 99, 
		RULE_updateAssignment = 100, RULE_explainOption = 101, RULE_transactionMode = 102, 
		RULE_levelOfIsolation = 103, RULE_callArgument = 104, RULE_pathElement = 105, 
		RULE_pathSpecification = 106, RULE_functionSpecification = 107, RULE_functionDeclaration = 108, 
		RULE_parameterDeclaration = 109, RULE_returnsClause = 110, RULE_routineCharacteristic = 111, 
		RULE_controlStatement = 112, RULE_caseStatementWhenClause = 113, RULE_elseIfClause = 114, 
		RULE_elseClause = 115, RULE_variableDeclaration = 116, RULE_sqlStatementList = 117, 
		RULE_privilege = 118, RULE_entityKind = 119, RULE_grantObject = 120, RULE_qualifiedName = 121, 
		RULE_queryPeriod = 122, RULE_rangeType = 123, RULE_grantor = 124, RULE_principal = 125, 
		RULE_roles = 126, RULE_privilegeOrRole = 127, RULE_identifier = 128, RULE_number = 129, 
		RULE_authorizationUser = 130, RULE_nonReserved = 131;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "standaloneExpression", "standalonePathSpecification", 
			"standaloneType", "standaloneRowPattern", "standaloneFunctionSpecification", 
			"statement", "rootQuery", "withFunction", "query", "with", "tableElement", 
			"columnDefinition", "likeClause", "properties", "propertyAssignments", 
			"property", "propertyValue", "queryNoWith", "limitRowCount", "rowCount", 
			"queryTerm", "queryPrimary", "sortItem", "querySpecification", "groupBy", 
			"groupingElement", "groupingSet", "windowDefinition", "windowSpecification", 
			"namedQuery", "setQuantifier", "selectItem", "relation", "joinType", 
			"joinCriteria", "sampledRelation", "sampleType", "trimsSpecification", 
			"listAggOverflowBehavior", "listaggCountIndication", "patternRecognition", 
			"measureDefinition", "rowsPerMatch", "emptyMatchHandling", "skipTo", 
			"subsetDefinition", "variableDefinition", "aliasedRelation", "columnAliases", 
			"relationPrimary", "jsonTableColumn", "jsonTableSpecificPlan", "jsonTablePathName", 
			"planPrimary", "jsonTableDefaultPlan", "tableFunctionCall", "tableFunctionArgument", 
			"tableArgument", "tableArgumentRelation", "descriptorArgument", "descriptorField", 
			"copartitionTables", "expression", "booleanExpression", "predicate", 
			"valueExpression", "primaryExpression", "jsonPathInvocation", "jsonValueExpression", 
			"jsonRepresentation", "jsonArgument", "jsonExistsErrorBehavior", "jsonValueBehavior", 
			"jsonQueryWrapperBehavior", "jsonQueryBehavior", "jsonObjectMember", 
			"processingMode", "nullTreatment", "string", "timeZoneSpecifier", "comparisonOperator", 
			"comparisonQuantifier", "booleanValue", "interval", "intervalField", 
			"normalForm", "type", "rowField", "typeParameter", "whenClause", "filter", 
			"mergeCase", "over", "windowFrame", "frameExtent", "frameBound", "rowPattern", 
			"patternPrimary", "patternQuantifier", "updateAssignment", "explainOption", 
			"transactionMode", "levelOfIsolation", "callArgument", "pathElement", 
			"pathSpecification", "functionSpecification", "functionDeclaration", 
			"parameterDeclaration", "returnsClause", "routineCharacteristic", "controlStatement", 
			"caseStatementWhenClause", "elseIfClause", "elseClause", "variableDeclaration", 
			"sqlStatementList", "privilege", "entityKind", "grantObject", "qualifiedName", 
			"queryPeriod", "rangeType", "grantor", "principal", "roles", "privilegeOrRole", 
			"identifier", "number", "authorizationUser", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'.'", "'('", "')'", "','", "'SKIP'", "'=>'", "'->'", "'['", "']'", 
			"':'", "'|'", "'^'", "'$'", "'{-'", "'-}'", "'{'", "'}'", "'ABSENT'", 
			"'ADD'", "'ADMIN'", "'AFTER'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", 
			"'ANY'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'AUTHORIZATION'", "'BEGIN'", 
			"'BERNOULLI'", "'BETWEEN'", "'BOTH'", "'BY'", "'CALL'", "'CALLED'", "'CASCADE'", 
			"'CASE'", "'CAST'", "'CATALOG'", "'CATALOGS'", "'COLUMN'", "'COLUMNS'", 
			"'COMMENT'", "'COMMIT'", "'COMMITTED'", "'CONDITIONAL'", "'CONSTRAINT'", 
			"'COUNT'", "'COPARTITION'", "'CREATE'", "'CROSS'", "'CUBE'", "'CURRENT'", 
			"'CURRENT_CATALOG'", "'CURRENT_DATE'", "'CURRENT_PATH'", "'CURRENT_ROLE'", 
			"'CURRENT_SCHEMA'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", "'CURRENT_USER'", 
			"'DATA'", "'DATE'", "'DAY'", "'DEALLOCATE'", "'DECLARE'", "'DEFAULT'", 
			"'DEFINE'", "'DEFINER'", "'DELETE'", "'DENY'", "'DESC'", "'DESCRIBE'", 
			"'DESCRIPTOR'", "'DETERMINISTIC'", "'DISTINCT'", "'DISTRIBUTED'", "'DO'", 
			"'DOUBLE'", "'DROP'", "'ELSE'", "'EMPTY'", "'ELSEIF'", "'ENCODING'", 
			"'END'", "'ERROR'", "'ESCAPE'", "'EXCEPT'", "'EXCLUDING'", "'EXECUTE'", 
			"'EXISTS'", "'EXPLAIN'", "'EXTRACT'", "'FALSE'", "'FETCH'", "'FILTER'", 
			"'FINAL'", "'FIRST'", "'FOLLOWING'", "'FOR'", "'FORMAT'", "'FROM'", "'FULL'", 
			"'FUNCTION'", "'FUNCTIONS'", "'GRACE'", "'GRANT'", "'GRANTED'", "'GRANTS'", 
			"'GRAPHVIZ'", "'GROUP'", "'GROUPING'", "'GROUPS'", "'HAVING'", "'HOUR'", 
			"'IF'", "'IGNORE'", "'IMMEDIATE'", "'IN'", "'INCLUDING'", "'INITIAL'", 
			"'INNER'", "'INPUT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", "'INTO'", 
			"'INVOKER'", "'IO'", "'IS'", "'ISOLATION'", "'ITERATE'", "'JOIN'", "'JSON'", 
			"'JSON_ARRAY'", "'JSON_EXISTS'", "'JSON_OBJECT'", "'JSON_QUERY'", "'JSON_TABLE'", 
			"'JSON_VALUE'", "'KEEP'", "'KEY'", "'KEYS'", "'LANGUAGE'", "'LAST'", 
			"'LATERAL'", "'LEADING'", "'LEAVE'", "'LEFT'", "'LEVEL'", "'LIKE'", "'LIMIT'", 
			"'LISTAGG'", "'LOCAL'", "'LOCALTIME'", "'LOCALTIMESTAMP'", "'LOGICAL'", 
			"'LOOP'", "'MAP'", "'MATCH'", "'MATCHED'", "'MATCHES'", "'MATCH_RECOGNIZE'", 
			"'MATERIALIZED'", "'MEASURES'", "'MERGE'", "'MINUTE'", "'MONTH'", "'NATURAL'", 
			"'NESTED'", "'NEXT'", "'NFC'", "'NFD'", "'NFKC'", "'NFKD'", "'NO'", "'NONE'", 
			"'NORMALIZE'", "'NOT'", "'NULL'", "'NULLIF'", "'NULLS'", "'OBJECT'", 
			"'OF'", "'OFFSET'", "'OMIT'", "'ON'", "'ONE'", "'ONLY'", "'OPTION'", 
			"'OR'", "'ORDER'", "'ORDINALITY'", "'OUTER'", "'OUTPUT'", "'OVER'", "'OVERFLOW'", 
			"'PARTITION'", "'PARTITIONS'", "'PASSING'", "'PAST'", "'PATH'", "'PATTERN'", 
			"'PER'", "'PERIOD'", "'PERMUTE'", "'PLAN'", "'POSITION'", "'PRECEDING'", 
			"'PRECISION'", "'PREPARE'", "'PRIVILEGES'", "'PROPERTIES'", "'PRUNE'", 
			"'QUOTES'", "'RANGE'", "'READ'", "'RECURSIVE'", "'REFRESH'", "'RENAME'", 
			"'REPEAT'", "'REPEATABLE'", "'REPLACE'", "'RESET'", "'RESPECT'", "'RESTRICT'", 
			"'RETURN'", "'RETURNING'", "'RETURNS'", "'REVOKE'", "'RIGHT'", "'ROLE'", 
			"'ROLES'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", "'ROWS'", "'RUNNING'", 
			"'SCALAR'", "'SCHEMA'", "'SCHEMAS'", "'SECOND'", "'SECURITY'", "'SEEK'", 
			"'SELECT'", "'SERIALIZABLE'", "'SESSION'", "'SET'", "'SETS'", "'SHOW'", 
			"'SOME'", "'START'", "'STATS'", "'SUBSET'", "'SUBSTRING'", "'SYSTEM'", 
			"'TABLE'", "'TABLES'", "'TABLESAMPLE'", "'TEXT'", "'STRING'", "'THEN'", 
			"'TIES'", "'TIME'", "'TIMESTAMP'", "'TO'", "'TRAILING'", "'TRANSACTION'", 
			"'TRIM'", "'TRUE'", "'TRUNCATE'", "'TRY_CAST'", "'TYPE'", "'UESCAPE'", 
			"'UNBOUNDED'", "'UNCOMMITTED'", "'UNCONDITIONAL'", "'UNION'", "'UNIQUE'", 
			"'UNKNOWN'", "'UNMATCHED'", "'UNNEST'", "'UNTIL'", "'UPDATE'", "'USE'", 
			"'USER'", "'USING'", "'UTF16'", "'UTF32'", "'UTF8'", "'VALIDATE'", "'VALUE'", 
			"'VALUES'", "'VERBOSE'", "'VERSION'", "'VIEW'", "'WHEN'", "'WHERE'", 
			"'WHILE'", "'WINDOW'", "'WITH'", "'WITHIN'", "'WITHOUT'", "'WORK'", "'WRAPPER'", 
			"'WRITE'", "'YEAR'", "'ZONE'", "'='", null, "'<'", "'<='", "'>'", "'>='", 
			"'+'", "'-'", "'*'", "'/'", "'%'", "'||'", "'?'", "';'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, "ABSENT", "ADD", "ADMIN", "AFTER", 
			"ALL", "ALTER", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "AT", 
			"AUTHORIZATION", "BEGIN", "BERNOULLI", "BETWEEN", "BOTH", "BY", "CALL", 
			"CALLED", "CASCADE", "CASE", "CAST", "CATALOG", "CATALOGS", "COLUMN", 
			"COLUMNS", "COMMENT", "COMMIT", "COMMITTED", "CONDITIONAL", "CONSTRAINT", 
			"COUNT", "COPARTITION", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_CATALOG", 
			"CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", 
			"CURRENT_TIMESTAMP", "CURRENT_USER", "DATA", "DATE", "DAY", "DEALLOCATE", 
			"DECLARE", "DEFAULT", "DEFINE", "DEFINER", "DELETE", "DENY", "DESC", 
			"DESCRIBE", "DESCRIPTOR", "DETERMINISTIC", "DISTINCT", "DISTRIBUTED", 
			"DO", "DOUBLE", "DROP", "ELSE", "EMPTY", "ELSEIF", "ENCODING", "END", 
			"ERROR", "ESCAPE", "EXCEPT", "EXCLUDING", "EXECUTE", "EXISTS", "EXPLAIN", 
			"EXTRACT", "FALSE", "FETCH", "FILTER", "FINAL", "FIRST", "FOLLOWING", 
			"FOR", "FORMAT", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GRACE", "GRANT", 
			"GRANTED", "GRANTS", "GRAPHVIZ", "GROUP", "GROUPING", "GROUPS", "HAVING", 
			"HOUR", "IF", "IGNORE", "IMMEDIATE", "IN", "INCLUDING", "INITIAL", "INNER", 
			"INPUT", "INSERT", "INTERSECT", "INTERVAL", "INTO", "INVOKER", "IO", 
			"IS", "ISOLATION", "ITERATE", "JOIN", "JSON", "JSON_ARRAY", "JSON_EXISTS", 
			"JSON_OBJECT", "JSON_QUERY", "JSON_TABLE", "JSON_VALUE", "KEEP", "KEY", 
			"KEYS", "LANGUAGE", "LAST", "LATERAL", "LEADING", "LEAVE", "LEFT", "LEVEL", 
			"LIKE", "LIMIT", "LISTAGG", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOGICAL", 
			"LOOP", "MAP", "MATCH", "MATCHED", "MATCHES", "MATCH_RECOGNIZE", "MATERIALIZED", 
			"MEASURES", "MERGE", "MINUTE", "MONTH", "NATURAL", "NESTED", "NEXT", 
			"NFC", "NFD", "NFKC", "NFKD", "NO", "NONE", "NORMALIZE", "NOT", "NULL", 
			"NULLIF", "NULLS", "OBJECT", "OF", "OFFSET", "OMIT", "ON", "ONE", "ONLY", 
			"OPTION", "OR", "ORDER", "ORDINALITY", "OUTER", "OUTPUT", "OVER", "OVERFLOW", 
			"PARTITION", "PARTITIONS", "PASSING", "PAST", "PATH", "PATTERN", "PER", 
			"PERIOD", "PERMUTE", "PLAN", "POSITION", "PRECEDING", "PRECISION", "PREPARE", 
			"PRIVILEGES", "PROPERTIES", "PRUNE", "QUOTES", "RANGE", "READ", "RECURSIVE", 
			"REFRESH", "RENAME", "REPEAT", "REPEATABLE", "REPLACE", "RESET", "RESPECT", 
			"RESTRICT", "RETURN", "RETURNING", "RETURNS", "REVOKE", "RIGHT", "ROLE", 
			"ROLES", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "RUNNING", "SCALAR", "SCHEMA", 
			"SCHEMAS", "SECOND", "SECURITY", "SEEK", "SELECT", "SERIALIZABLE", "SESSION", 
			"SET", "SETS", "SHOW", "SOME", "START", "STATS", "SUBSET", "SUBSTRING", 
			"SYSTEM", "TABLE", "TABLES", "TABLESAMPLE", "TEXT", "TEXT_STRING", "THEN", 
			"TIES", "TIME", "TIMESTAMP", "TO", "TRAILING", "TRANSACTION", "TRIM", 
			"TRUE", "TRUNCATE", "TRY_CAST", "TYPE", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", 
			"UNCONDITIONAL", "UNION", "UNIQUE", "UNKNOWN", "UNMATCHED", "UNNEST", 
			"UNTIL", "UPDATE", "USE", "USER", "USING", "UTF16", "UTF32", "UTF8", 
			"VALIDATE", "VALUE", "VALUES", "VERBOSE", "VERSION", "VIEW", "WHEN", 
			"WHERE", "WHILE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", 
			"WRITE", "YEAR", "ZONE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", 
			"MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", "QUESTION_MARK", "SEMICOLON", 
			"STRING", "UNICODE_STRING", "BINARY_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", 
			"DOUBLE_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER", 
			"BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", 
			"UNRECOGNIZED", "DELIMITER"
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
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(264);
			statement();
			setState(265);
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
	public static class StandaloneExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneExpressionContext standaloneExpression() throws RecognitionException {
		StandaloneExpressionContext _localctx = new StandaloneExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_standaloneExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(267);
			expression();
			setState(268);
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
	public static class StandalonePathSpecificationContext extends ParserRuleContext {
		public PathSpecificationContext pathSpecification() {
			return getRuleContext(PathSpecificationContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandalonePathSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standalonePathSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandalonePathSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandalonePathSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandalonePathSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandalonePathSpecificationContext standalonePathSpecification() throws RecognitionException {
		StandalonePathSpecificationContext _localctx = new StandalonePathSpecificationContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_standalonePathSpecification);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(270);
			pathSpecification();
			setState(271);
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
	public static class StandaloneTypeContext extends ParserRuleContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneTypeContext standaloneType() throws RecognitionException {
		StandaloneTypeContext _localctx = new StandaloneTypeContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_standaloneType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(273);
			type(0);
			setState(274);
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
	public static class StandaloneRowPatternContext extends ParserRuleContext {
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneRowPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneRowPattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneRowPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneRowPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneRowPattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneRowPatternContext standaloneRowPattern() throws RecognitionException {
		StandaloneRowPatternContext _localctx = new StandaloneRowPatternContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_standaloneRowPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(276);
			rowPattern(0);
			setState(277);
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
	public static class StandaloneFunctionSpecificationContext extends ParserRuleContext {
		public FunctionSpecificationContext functionSpecification() {
			return getRuleContext(FunctionSpecificationContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StandaloneFunctionSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_standaloneFunctionSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStandaloneFunctionSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStandaloneFunctionSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStandaloneFunctionSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StandaloneFunctionSpecificationContext standaloneFunctionSpecification() throws RecognitionException {
		StandaloneFunctionSpecificationContext _localctx = new StandaloneFunctionSpecificationContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_standaloneFunctionSpecification);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(279);
			functionSpecification();
			setState(280);
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
	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public List<ExplainOptionContext> explainOption() {
			return getRuleContexts(ExplainOptionContext.class);
		}
		public ExplainOptionContext explainOption(int i) {
			return getRuleContext(ExplainOptionContext.class,i);
		}
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PrepareContext extends StatementContext {
		public TerminalNode PREPARE() { return getToken(SqlBaseParser.PREPARE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public PrepareContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrepare(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrepare(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrepare(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropMaterializedViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetMaterializedViewPropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public SetMaterializedViewPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetMaterializedViewProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetMaterializedViewProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetMaterializedViewProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropNotNullConstraintContext extends StatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext columnName;
		public List<TerminalNode> ALTER() { return getTokens(SqlBaseParser.ALTER); }
		public TerminalNode ALTER(int i) {
			return getToken(SqlBaseParser.ALTER, i);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropNotNullConstraintContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropNotNullConstraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropNotNullConstraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropNotNullConstraint(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UseContext extends StatementContext {
		public IdentifierContext schema;
		public IdentifierContext catalog;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeallocateContext extends StatementContext {
		public TerminalNode DEALLOCATE() { return getToken(SqlBaseParser.DEALLOCATE, 0); }
		public TerminalNode PREPARE() { return getToken(SqlBaseParser.PREPARE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DeallocateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeallocate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeallocate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeallocate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommitContext extends StatementContext {
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public CommitContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommit(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateRoleContext extends StatementContext {
		public IdentifierContext name;
		public IdentifierContext catalog;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public CreateRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public QualifiedNameContext column;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public DropColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTablesContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetViewAuthorizationContext extends StatementContext {
		public QualifiedNameContext from;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetViewAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetViewAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetViewAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetViewAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCatalogsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowCatalogsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCatalogs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCatalogs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCatalogs(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowRolesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public ShowRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeContext extends StatementContext {
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<MergeCaseContext> mergeCase() {
			return getRuleContexts(MergeCaseContext.class);
		}
		public MergeCaseContext mergeCase(int i) {
			return getRuleContext(MergeCaseContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public MergeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMerge(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMerge(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMerge(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public QualifiedNameContext from;
		public IdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public RenameColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentColumnContext extends StatementContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RevokeRolesContext extends StatementContext {
		public IdentifierContext catalog;
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public List<PrivilegeOrRoleContext> privilegeOrRole() {
			return getRuleContexts(PrivilegeOrRoleContext.class);
		}
		public PrivilegeOrRoleContext privilegeOrRole(int i) {
			return getRuleContext(PrivilegeOrRoleContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<PrincipalContext> principal() {
			return getRuleContexts(PrincipalContext.class);
		}
		public PrincipalContext principal(int i) {
			return getRuleContext(PrincipalContext.class,i);
		}
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RevokeRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRevokeRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRevokeRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRevokeRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowColumnsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowRoleGrantsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public ShowRoleGrantsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowRoleGrants(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowRoleGrants(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowRoleGrants(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddColumnContext extends StatementContext {
		public QualifiedNameContext tableName;
		public ColumnDefinitionContext column;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public List<TerminalNode> EXISTS() { return getTokens(SqlBaseParser.EXISTS); }
		public TerminalNode EXISTS(int i) {
			return getToken(SqlBaseParser.EXISTS, i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public AddColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DenyContext extends StatementContext {
		public PrincipalContext grantee;
		public TerminalNode DENY() { return getToken(SqlBaseParser.DENY, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public GrantObjectContext grantObject() {
			return getRuleContext(GrantObjectContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public List<PrivilegeContext> privilege() {
			return getRuleContexts(PrivilegeContext.class);
		}
		public PrivilegeContext privilege(int i) {
			return getRuleContext(PrivilegeContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public DenyContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeny(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeny(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeny(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetSessionContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ResetSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertIntoContext extends StatementContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public RootQueryContext rootQuery() {
			return getRuleContext(RootQueryContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public InsertIntoContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertInto(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertInto(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertInto(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowSessionContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateSchemaContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetSessionAuthorizationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public AuthorizationUserContext authorizationUser() {
			return getRuleContext(AuthorizationUserContext.class,0);
		}
		public SetSessionAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetSessionAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetSessionAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetSessionAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainAnalyzeContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public ExplainAnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExecuteContext extends StatementContext {
		public TerminalNode EXECUTE() { return getToken(SqlBaseParser.EXECUTE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ExecuteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExecute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExecute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExecute(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameSchemaContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RenameSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropRoleContext extends StatementContext {
		public IdentifierContext name;
		public IdentifierContext catalog;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public DropRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetRoleContext extends StatementContext {
		public IdentifierContext role;
		public IdentifierContext catalog;
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode NONE() { return getToken(SqlBaseParser.NONE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public SetRoleContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetRole(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateFunctionContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public FunctionSpecificationContext functionSpecification() {
			return getRuleContext(FunctionSpecificationContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropCatalogContext extends StatementContext {
		public IdentifierContext catalog;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public DropCatalogContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropCatalog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropCatalog(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropCatalog(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowGrantsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public GrantObjectContext grantObject() {
			return getRuleContext(GrantObjectContext.class,0);
		}
		public ShowGrantsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowGrants(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowGrants(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowGrants(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropSchemaContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public DropSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetSessionAuthorizationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public ResetSessionAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetSessionAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetSessionAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetSessionAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTableAuthorizationContext extends StatementContext {
		public QualifiedNameContext tableName;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetTableAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateViewContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<TableElementContext> tableElement() {
			return getRuleContexts(TableElementContext.class);
		}
		public TableElementContext tableElement(int i) {
			return getRuleContext(TableElementContext.class,i);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GrantPrivilegesContext extends StatementContext {
		public List<TerminalNode> GRANT() { return getTokens(SqlBaseParser.GRANT); }
		public TerminalNode GRANT(int i) {
			return getToken(SqlBaseParser.GRANT, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public GrantObjectContext grantObject() {
			return getRuleContext(GrantObjectContext.class,0);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public List<PrivilegeOrRoleContext> privilegeOrRole() {
			return getRuleContexts(PrivilegeOrRoleContext.class);
		}
		public PrivilegeOrRoleContext privilegeOrRole(int i) {
			return getRuleContext(PrivilegeOrRoleContext.class,i);
		}
		public GrantPrivilegesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrantPrivileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrantPrivileges(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrantPrivileges(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StartTransactionContext extends StatementContext {
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public List<TransactionModeContext> transactionMode() {
			return getRuleContexts(TransactionModeContext.class);
		}
		public TransactionModeContext transactionMode(int i) {
			return getRuleContext(TransactionModeContext.class,i);
		}
		public StartTransactionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStartTransaction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStartTransaction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStartTransaction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableAsSelectContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public RootQueryContext rootQuery() {
			return getRuleContext(RootQueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public CreateTableAsSelectContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableAsSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableAsSelect(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableAsSelect(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentViewContext extends StatementContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowStatsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowStats(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowStats(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowStats(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateSchemaContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateSchemaContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RevokePrivilegesContext extends StatementContext {
		public PrincipalContext grantee;
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public GrantObjectContext grantObject() {
			return getRuleContext(GrantObjectContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public List<PrivilegeOrRoleContext> privilegeOrRole() {
			return getRuleContexts(PrivilegeOrRoleContext.class);
		}
		public PrivilegeOrRoleContext privilegeOrRole(int i) {
			return getRuleContext(PrivilegeOrRoleContext.class,i);
		}
		public RevokePrivilegesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRevokePrivileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRevokePrivileges(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRevokePrivileges(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public FunctionDeclarationContext functionDeclaration() {
			return getRuleContext(FunctionDeclarationContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UpdateContext extends StatementContext {
		public BooleanExpressionContext where;
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public List<UpdateAssignmentContext> updateAssignment() {
			return getRuleContexts(UpdateAssignmentContext.class);
		}
		public UpdateAssignmentContext updateAssignment(int i) {
			return getRuleContext(UpdateAssignmentContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public UpdateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUpdate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableExecuteContext extends StatementContext {
		public QualifiedNameContext tableName;
		public IdentifierContext procedureName;
		public BooleanExpressionContext where;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXECUTE() { return getToken(SqlBaseParser.EXECUTE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public List<CallArgumentContext> callArgument() {
			return getRuleContexts(CallArgumentContext.class);
		}
		public CallArgumentContext callArgument(int i) {
			return getRuleContext(CallArgumentContext.class,i);
		}
		public TableExecuteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableExecute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableExecute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableExecute(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeleteContext extends StatementContext {
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public DeleteContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDelete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDelete(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeInputContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DescribeInputContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeInput(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeInput(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeInput(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowStatsForQueryContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public RootQueryContext rootQuery() {
			return getRuleContext(RootQueryContext.class,0);
		}
		public ShowStatsForQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowStatsForQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowStatsForQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowStatsForQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetColumnTypeContext extends StatementContext {
		public QualifiedNameContext tableName;
		public QualifiedNameContext columnName;
		public List<TerminalNode> ALTER() { return getTokens(SqlBaseParser.ALTER); }
		public TerminalNode ALTER(int i) {
			return getToken(SqlBaseParser.ALTER, i);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public SetColumnTypeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetColumnType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetColumnType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetColumnType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StatementDefaultContext extends StatementContext {
		public RootQueryContext rootQuery() {
			return getRuleContext(RootQueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTimeZoneContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetTimeZoneContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTimeZone(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateMaterializedViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public RootQueryContext rootQuery() {
			return getRuleContext(RootQueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode GRACE() { return getToken(SqlBaseParser.GRACE, 0); }
		public TerminalNode PERIOD() { return getToken(SqlBaseParser.PERIOD, 0); }
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetSessionContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetSessionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetSession(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetSession(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetSession(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public RootQueryContext rootQuery() {
			return getRuleContext(RootQueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameMaterializedViewContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public RenameMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowSchemasContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowSchemasContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowSchemas(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowSchemas(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowSchemas(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetSchemaAuthorizationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public SetSchemaAuthorizationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetSchemaAuthorization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetSchemaAuthorization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetSchemaAuthorization(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollbackContext extends StatementContext {
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public RollbackContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollback(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollback(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollback(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentTableContext extends StatementContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExecuteImmediateContext extends StatementContext {
		public TerminalNode EXECUTE() { return getToken(SqlBaseParser.EXECUTE, 0); }
		public TerminalNode IMMEDIATE() { return getToken(SqlBaseParser.IMMEDIATE, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ExecuteImmediateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExecuteImmediate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExecuteImmediate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExecuteImmediate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameViewContext extends StatementContext {
		public QualifiedNameContext from;
		public QualifiedNameContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public RenameViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetPathContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public PathSpecificationContext pathSpecification() {
			return getRuleContext(PathSpecificationContext.class,0);
		}
		public SetPathContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetPath(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GrantRolesContext extends StatementContext {
		public IdentifierContext catalog;
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public List<PrivilegeOrRoleContext> privilegeOrRole() {
			return getRuleContexts(PrivilegeOrRoleContext.class);
		}
		public PrivilegeOrRoleContext privilegeOrRole(int i) {
			return getRuleContext(PrivilegeOrRoleContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PrincipalContext> principal() {
			return getRuleContexts(PrincipalContext.class);
		}
		public PrincipalContext principal(int i) {
			return getRuleContext(PrincipalContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GrantorContext grantor() {
			return getRuleContext(GrantorContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GrantRolesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrantRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrantRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrantRoles(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CallContext extends StatementContext {
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<CallArgumentContext> callArgument() {
			return getRuleContexts(CallArgumentContext.class);
		}
		public CallArgumentContext callArgument(int i) {
			return getRuleContext(CallArgumentContext.class,i);
		}
		public CallContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshMaterializedViewContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public RefreshMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateMaterializedViewContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowCreateMaterializedViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateMaterializedView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateMaterializedView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateMaterializedView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateCatalogContext extends StatementContext {
		public IdentifierContext catalog;
		public IdentifierContext connectorName;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public CreateCatalogContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateCatalog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateCatalog(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateCatalog(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowFunctionsContext extends StatementContext {
		public StringContext pattern;
		public StringContext escape;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeOutputContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DescribeOutputContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeOutput(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeOutput(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeOutput(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTablePropertiesContext extends StatementContext {
		public QualifiedNameContext tableName;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_statement);
		int _la;
		try {
			setState(1167);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(282);
				rootQuery();
				}
				break;
			case 2:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(283);
				match(USE);
				setState(284);
				((UseContext)_localctx).schema = identifier();
				}
				break;
			case 3:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(285);
				match(USE);
				setState(286);
				((UseContext)_localctx).catalog = identifier();
				setState(287);
				match(T__0);
				setState(288);
				((UseContext)_localctx).schema = identifier();
				}
				break;
			case 4:
				_localctx = new CreateCatalogContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(290);
				match(CREATE);
				setState(291);
				match(CATALOG);
				setState(295);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(292);
					match(IF);
					setState(293);
					match(NOT);
					setState(294);
					match(EXISTS);
					}
					break;
				}
				setState(297);
				((CreateCatalogContext)_localctx).catalog = identifier();
				setState(298);
				match(USING);
				setState(299);
				((CreateCatalogContext)_localctx).connectorName = identifier();
				setState(302);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(300);
					match(COMMENT);
					setState(301);
					string();
					}
				}

				setState(306);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AUTHORIZATION) {
					{
					setState(304);
					match(AUTHORIZATION);
					setState(305);
					principal();
					}
				}

				setState(310);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(308);
					match(WITH);
					setState(309);
					properties();
					}
				}

				}
				break;
			case 5:
				_localctx = new DropCatalogContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(312);
				match(DROP);
				setState(313);
				match(CATALOG);
				setState(316);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(314);
					match(IF);
					setState(315);
					match(EXISTS);
					}
					break;
				}
				setState(318);
				((DropCatalogContext)_localctx).catalog = identifier();
				setState(320);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(319);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 6:
				_localctx = new CreateSchemaContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(322);
				match(CREATE);
				setState(323);
				match(SCHEMA);
				setState(327);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(324);
					match(IF);
					setState(325);
					match(NOT);
					setState(326);
					match(EXISTS);
					}
					break;
				}
				setState(329);
				qualifiedName();
				setState(332);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AUTHORIZATION) {
					{
					setState(330);
					match(AUTHORIZATION);
					setState(331);
					principal();
					}
				}

				setState(336);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(334);
					match(WITH);
					setState(335);
					properties();
					}
				}

				}
				break;
			case 7:
				_localctx = new DropSchemaContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(338);
				match(DROP);
				setState(339);
				match(SCHEMA);
				setState(342);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
				case 1:
					{
					setState(340);
					match(IF);
					setState(341);
					match(EXISTS);
					}
					break;
				}
				setState(344);
				qualifiedName();
				setState(346);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(345);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 8:
				_localctx = new RenameSchemaContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(348);
				match(ALTER);
				setState(349);
				match(SCHEMA);
				setState(350);
				qualifiedName();
				setState(351);
				match(RENAME);
				setState(352);
				match(TO);
				setState(353);
				identifier();
				}
				break;
			case 9:
				_localctx = new SetSchemaAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(355);
				match(ALTER);
				setState(356);
				match(SCHEMA);
				setState(357);
				qualifiedName();
				setState(358);
				match(SET);
				setState(359);
				match(AUTHORIZATION);
				setState(360);
				principal();
				}
				break;
			case 10:
				_localctx = new CreateTableAsSelectContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(362);
				match(CREATE);
				setState(365);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(363);
					match(OR);
					setState(364);
					match(REPLACE);
					}
				}

				setState(367);
				match(TABLE);
				setState(371);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
				case 1:
					{
					setState(368);
					match(IF);
					setState(369);
					match(NOT);
					setState(370);
					match(EXISTS);
					}
					break;
				}
				setState(373);
				qualifiedName();
				setState(375);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(374);
					columnAliases();
					}
				}

				setState(379);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(377);
					match(COMMENT);
					setState(378);
					string();
					}
				}

				setState(383);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(381);
					match(WITH);
					setState(382);
					properties();
					}
				}

				setState(385);
				match(AS);
				setState(391);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
				case 1:
					{
					setState(386);
					rootQuery();
					}
					break;
				case 2:
					{
					setState(387);
					match(T__1);
					setState(388);
					rootQuery();
					setState(389);
					match(T__2);
					}
					break;
				}
				setState(398);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(393);
					match(WITH);
					setState(395);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==NO) {
						{
						setState(394);
						match(NO);
						}
					}

					setState(397);
					match(DATA);
					}
				}

				}
				break;
			case 11:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(400);
				match(CREATE);
				setState(403);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(401);
					match(OR);
					setState(402);
					match(REPLACE);
					}
				}

				setState(405);
				match(TABLE);
				setState(409);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
				case 1:
					{
					setState(406);
					match(IF);
					setState(407);
					match(NOT);
					setState(408);
					match(EXISTS);
					}
					break;
				}
				setState(411);
				qualifiedName();
				setState(412);
				match(T__1);
				setState(413);
				tableElement();
				setState(418);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(414);
					match(T__3);
					setState(415);
					tableElement();
					}
					}
					setState(420);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(421);
				match(T__2);
				setState(424);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(422);
					match(COMMENT);
					setState(423);
					string();
					}
				}

				setState(428);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(426);
					match(WITH);
					setState(427);
					properties();
					}
				}

				}
				break;
			case 12:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(430);
				match(DROP);
				setState(431);
				match(TABLE);
				setState(434);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
				case 1:
					{
					setState(432);
					match(IF);
					setState(433);
					match(EXISTS);
					}
					break;
				}
				setState(436);
				qualifiedName();
				}
				break;
			case 13:
				_localctx = new InsertIntoContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(437);
				match(INSERT);
				setState(438);
				match(INTO);
				setState(439);
				qualifiedName();
				setState(441);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(440);
					columnAliases();
					}
					break;
				}
				setState(443);
				rootQuery();
				}
				break;
			case 14:
				_localctx = new DeleteContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(445);
				match(DELETE);
				setState(446);
				match(FROM);
				setState(447);
				qualifiedName();
				setState(450);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(448);
					match(WHERE);
					setState(449);
					booleanExpression(0);
					}
				}

				}
				break;
			case 15:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(452);
				match(TRUNCATE);
				setState(453);
				match(TABLE);
				setState(454);
				qualifiedName();
				}
				break;
			case 16:
				_localctx = new CommentTableContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(455);
				match(COMMENT);
				setState(456);
				match(ON);
				setState(457);
				match(TABLE);
				setState(458);
				qualifiedName();
				setState(459);
				match(IS);
				setState(462);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case UNICODE_STRING:
					{
					setState(460);
					string();
					}
					break;
				case NULL:
					{
					setState(461);
					match(NULL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 17:
				_localctx = new CommentViewContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(464);
				match(COMMENT);
				setState(465);
				match(ON);
				setState(466);
				match(VIEW);
				setState(467);
				qualifiedName();
				setState(468);
				match(IS);
				setState(471);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case UNICODE_STRING:
					{
					setState(469);
					string();
					}
					break;
				case NULL:
					{
					setState(470);
					match(NULL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 18:
				_localctx = new CommentColumnContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(473);
				match(COMMENT);
				setState(474);
				match(ON);
				setState(475);
				match(COLUMN);
				setState(476);
				qualifiedName();
				setState(477);
				match(IS);
				setState(480);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
				case UNICODE_STRING:
					{
					setState(478);
					string();
					}
					break;
				case NULL:
					{
					setState(479);
					match(NULL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 19:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(482);
				match(ALTER);
				setState(483);
				match(TABLE);
				setState(486);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
				case 1:
					{
					setState(484);
					match(IF);
					setState(485);
					match(EXISTS);
					}
					break;
				}
				setState(488);
				((RenameTableContext)_localctx).from = qualifiedName();
				setState(489);
				match(RENAME);
				setState(490);
				match(TO);
				setState(491);
				((RenameTableContext)_localctx).to = qualifiedName();
				}
				break;
			case 20:
				_localctx = new AddColumnContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(493);
				match(ALTER);
				setState(494);
				match(TABLE);
				setState(497);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(495);
					match(IF);
					setState(496);
					match(EXISTS);
					}
					break;
				}
				setState(499);
				((AddColumnContext)_localctx).tableName = qualifiedName();
				setState(500);
				match(ADD);
				setState(501);
				match(COLUMN);
				setState(505);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
				case 1:
					{
					setState(502);
					match(IF);
					setState(503);
					match(NOT);
					setState(504);
					match(EXISTS);
					}
					break;
				}
				setState(507);
				((AddColumnContext)_localctx).column = columnDefinition();
				}
				break;
			case 21:
				_localctx = new RenameColumnContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(509);
				match(ALTER);
				setState(510);
				match(TABLE);
				setState(513);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
				case 1:
					{
					setState(511);
					match(IF);
					setState(512);
					match(EXISTS);
					}
					break;
				}
				setState(515);
				((RenameColumnContext)_localctx).tableName = qualifiedName();
				setState(516);
				match(RENAME);
				setState(517);
				match(COLUMN);
				setState(520);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
				case 1:
					{
					setState(518);
					match(IF);
					setState(519);
					match(EXISTS);
					}
					break;
				}
				setState(522);
				((RenameColumnContext)_localctx).from = qualifiedName();
				setState(523);
				match(TO);
				setState(524);
				((RenameColumnContext)_localctx).to = identifier();
				}
				break;
			case 22:
				_localctx = new DropColumnContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(526);
				match(ALTER);
				setState(527);
				match(TABLE);
				setState(530);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
				case 1:
					{
					setState(528);
					match(IF);
					setState(529);
					match(EXISTS);
					}
					break;
				}
				setState(532);
				((DropColumnContext)_localctx).tableName = qualifiedName();
				setState(533);
				match(DROP);
				setState(534);
				match(COLUMN);
				setState(537);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
				case 1:
					{
					setState(535);
					match(IF);
					setState(536);
					match(EXISTS);
					}
					break;
				}
				setState(539);
				((DropColumnContext)_localctx).column = qualifiedName();
				}
				break;
			case 23:
				_localctx = new SetColumnTypeContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(541);
				match(ALTER);
				setState(542);
				match(TABLE);
				setState(545);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
				case 1:
					{
					setState(543);
					match(IF);
					setState(544);
					match(EXISTS);
					}
					break;
				}
				setState(547);
				((SetColumnTypeContext)_localctx).tableName = qualifiedName();
				setState(548);
				match(ALTER);
				setState(549);
				match(COLUMN);
				setState(550);
				((SetColumnTypeContext)_localctx).columnName = qualifiedName();
				setState(551);
				match(SET);
				setState(552);
				match(DATA);
				setState(553);
				match(TYPE);
				setState(554);
				type(0);
				}
				break;
			case 24:
				_localctx = new DropNotNullConstraintContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(556);
				match(ALTER);
				setState(557);
				match(TABLE);
				setState(560);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
				case 1:
					{
					setState(558);
					match(IF);
					setState(559);
					match(EXISTS);
					}
					break;
				}
				setState(562);
				((DropNotNullConstraintContext)_localctx).tableName = qualifiedName();
				setState(563);
				match(ALTER);
				setState(564);
				match(COLUMN);
				setState(565);
				((DropNotNullConstraintContext)_localctx).columnName = identifier();
				setState(566);
				match(DROP);
				setState(567);
				match(NOT);
				setState(568);
				match(NULL);
				}
				break;
			case 25:
				_localctx = new SetTableAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(570);
				match(ALTER);
				setState(571);
				match(TABLE);
				setState(572);
				((SetTableAuthorizationContext)_localctx).tableName = qualifiedName();
				setState(573);
				match(SET);
				setState(574);
				match(AUTHORIZATION);
				setState(575);
				principal();
				}
				break;
			case 26:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(577);
				match(ALTER);
				setState(578);
				match(TABLE);
				setState(579);
				((SetTablePropertiesContext)_localctx).tableName = qualifiedName();
				setState(580);
				match(SET);
				setState(581);
				match(PROPERTIES);
				setState(582);
				propertyAssignments();
				}
				break;
			case 27:
				_localctx = new TableExecuteContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(584);
				match(ALTER);
				setState(585);
				match(TABLE);
				setState(586);
				((TableExecuteContext)_localctx).tableName = qualifiedName();
				setState(587);
				match(EXECUTE);
				setState(588);
				((TableExecuteContext)_localctx).procedureName = identifier();
				setState(601);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(589);
					match(T__1);
					setState(598);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
						{
						setState(590);
						callArgument();
						setState(595);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__3) {
							{
							{
							setState(591);
							match(T__3);
							setState(592);
							callArgument();
							}
							}
							setState(597);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(600);
					match(T__2);
					}
				}

				setState(605);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(603);
					match(WHERE);
					setState(604);
					((TableExecuteContext)_localctx).where = booleanExpression(0);
					}
				}

				}
				break;
			case 28:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(607);
				match(ANALYZE);
				setState(608);
				qualifiedName();
				setState(611);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(609);
					match(WITH);
					setState(610);
					properties();
					}
				}

				}
				break;
			case 29:
				_localctx = new CreateMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(613);
				match(CREATE);
				setState(616);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(614);
					match(OR);
					setState(615);
					match(REPLACE);
					}
				}

				setState(618);
				match(MATERIALIZED);
				setState(619);
				match(VIEW);
				setState(623);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
				case 1:
					{
					setState(620);
					match(IF);
					setState(621);
					match(NOT);
					setState(622);
					match(EXISTS);
					}
					break;
				}
				setState(625);
				qualifiedName();
				setState(629);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRACE) {
					{
					setState(626);
					match(GRACE);
					setState(627);
					match(PERIOD);
					setState(628);
					interval();
					}
				}

				setState(633);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(631);
					match(COMMENT);
					setState(632);
					string();
					}
				}

				setState(637);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(635);
					match(WITH);
					setState(636);
					properties();
					}
				}

				setState(639);
				match(AS);
				setState(640);
				rootQuery();
				}
				break;
			case 30:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(642);
				match(CREATE);
				setState(645);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(643);
					match(OR);
					setState(644);
					match(REPLACE);
					}
				}

				setState(647);
				match(VIEW);
				setState(648);
				qualifiedName();
				setState(651);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(649);
					match(COMMENT);
					setState(650);
					string();
					}
				}

				setState(655);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SECURITY) {
					{
					setState(653);
					match(SECURITY);
					setState(654);
					_la = _input.LA(1);
					if ( !(_la==DEFINER || _la==INVOKER) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(657);
				match(AS);
				setState(658);
				rootQuery();
				}
				break;
			case 31:
				_localctx = new RefreshMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(660);
				match(REFRESH);
				setState(661);
				match(MATERIALIZED);
				setState(662);
				match(VIEW);
				setState(663);
				qualifiedName();
				}
				break;
			case 32:
				_localctx = new DropMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(664);
				match(DROP);
				setState(665);
				match(MATERIALIZED);
				setState(666);
				match(VIEW);
				setState(669);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
				case 1:
					{
					setState(667);
					match(IF);
					setState(668);
					match(EXISTS);
					}
					break;
				}
				setState(671);
				qualifiedName();
				}
				break;
			case 33:
				_localctx = new RenameMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(672);
				match(ALTER);
				setState(673);
				match(MATERIALIZED);
				setState(674);
				match(VIEW);
				setState(677);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
				case 1:
					{
					setState(675);
					match(IF);
					setState(676);
					match(EXISTS);
					}
					break;
				}
				setState(679);
				((RenameMaterializedViewContext)_localctx).from = qualifiedName();
				setState(680);
				match(RENAME);
				setState(681);
				match(TO);
				setState(682);
				((RenameMaterializedViewContext)_localctx).to = qualifiedName();
				}
				break;
			case 34:
				_localctx = new SetMaterializedViewPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(684);
				match(ALTER);
				setState(685);
				match(MATERIALIZED);
				setState(686);
				match(VIEW);
				setState(687);
				qualifiedName();
				setState(688);
				match(SET);
				setState(689);
				match(PROPERTIES);
				setState(690);
				propertyAssignments();
				}
				break;
			case 35:
				_localctx = new DropViewContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(692);
				match(DROP);
				setState(693);
				match(VIEW);
				setState(696);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
				case 1:
					{
					setState(694);
					match(IF);
					setState(695);
					match(EXISTS);
					}
					break;
				}
				setState(698);
				qualifiedName();
				}
				break;
			case 36:
				_localctx = new RenameViewContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(699);
				match(ALTER);
				setState(700);
				match(VIEW);
				setState(701);
				((RenameViewContext)_localctx).from = qualifiedName();
				setState(702);
				match(RENAME);
				setState(703);
				match(TO);
				setState(704);
				((RenameViewContext)_localctx).to = qualifiedName();
				}
				break;
			case 37:
				_localctx = new SetViewAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(706);
				match(ALTER);
				setState(707);
				match(VIEW);
				setState(708);
				((SetViewAuthorizationContext)_localctx).from = qualifiedName();
				setState(709);
				match(SET);
				setState(710);
				match(AUTHORIZATION);
				setState(711);
				principal();
				}
				break;
			case 38:
				_localctx = new CallContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(713);
				match(CALL);
				setState(714);
				qualifiedName();
				setState(715);
				match(T__1);
				setState(724);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
					{
					setState(716);
					callArgument();
					setState(721);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(717);
						match(T__3);
						setState(718);
						callArgument();
						}
						}
						setState(723);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(726);
				match(T__2);
				}
				break;
			case 39:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(728);
				match(CREATE);
				setState(731);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(729);
					match(OR);
					setState(730);
					match(REPLACE);
					}
				}

				setState(733);
				functionSpecification();
				}
				break;
			case 40:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(734);
				match(DROP);
				setState(735);
				match(FUNCTION);
				setState(738);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
				case 1:
					{
					setState(736);
					match(IF);
					setState(737);
					match(EXISTS);
					}
					break;
				}
				setState(740);
				functionDeclaration();
				}
				break;
			case 41:
				_localctx = new CreateRoleContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(741);
				match(CREATE);
				setState(742);
				match(ROLE);
				setState(743);
				((CreateRoleContext)_localctx).name = identifier();
				setState(747);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(744);
					match(WITH);
					setState(745);
					match(ADMIN);
					setState(746);
					grantor();
					}
				}

				setState(751);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(749);
					match(IN);
					setState(750);
					((CreateRoleContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 42:
				_localctx = new DropRoleContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(753);
				match(DROP);
				setState(754);
				match(ROLE);
				setState(755);
				((DropRoleContext)_localctx).name = identifier();
				setState(758);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(756);
					match(IN);
					setState(757);
					((DropRoleContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 43:
				_localctx = new GrantRolesContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(760);
				match(GRANT);
				setState(761);
				privilegeOrRole();
				setState(766);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(762);
					match(T__3);
					setState(763);
					privilegeOrRole();
					}
					}
					setState(768);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(769);
				match(TO);
				setState(770);
				principal();
				setState(775);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(771);
					match(T__3);
					setState(772);
					principal();
					}
					}
					setState(777);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(781);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(778);
					match(WITH);
					setState(779);
					match(ADMIN);
					setState(780);
					match(OPTION);
					}
				}

				setState(786);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANTED) {
					{
					setState(783);
					match(GRANTED);
					setState(784);
					match(BY);
					setState(785);
					grantor();
					}
				}

				setState(790);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(788);
					match(IN);
					setState(789);
					((GrantRolesContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 44:
				_localctx = new GrantPrivilegesContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(792);
				match(GRANT);
				setState(803);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
				case 1:
					{
					{
					setState(793);
					privilegeOrRole();
					setState(798);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(794);
						match(T__3);
						setState(795);
						privilegeOrRole();
						}
						}
						setState(800);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					}
					break;
				case 2:
					{
					setState(801);
					match(ALL);
					setState(802);
					match(PRIVILEGES);
					}
					break;
				}
				setState(805);
				match(ON);
				setState(806);
				grantObject();
				setState(807);
				match(TO);
				setState(808);
				principal();
				setState(812);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(809);
					match(WITH);
					setState(810);
					match(GRANT);
					setState(811);
					match(OPTION);
					}
				}

				}
				break;
			case 45:
				_localctx = new RevokeRolesContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(814);
				match(REVOKE);
				setState(818);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(815);
					match(ADMIN);
					setState(816);
					match(OPTION);
					setState(817);
					match(FOR);
					}
					break;
				}
				setState(820);
				privilegeOrRole();
				setState(825);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(821);
					match(T__3);
					setState(822);
					privilegeOrRole();
					}
					}
					setState(827);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(828);
				match(FROM);
				setState(829);
				principal();
				setState(834);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(830);
					match(T__3);
					setState(831);
					principal();
					}
					}
					setState(836);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(840);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRANTED) {
					{
					setState(837);
					match(GRANTED);
					setState(838);
					match(BY);
					setState(839);
					grantor();
					}
				}

				setState(844);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(842);
					match(IN);
					setState(843);
					((RevokeRolesContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 46:
				_localctx = new RevokePrivilegesContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(846);
				match(REVOKE);
				setState(850);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
				case 1:
					{
					setState(847);
					match(GRANT);
					setState(848);
					match(OPTION);
					setState(849);
					match(FOR);
					}
					break;
				}
				setState(862);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					{
					setState(852);
					privilegeOrRole();
					setState(857);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(853);
						match(T__3);
						setState(854);
						privilegeOrRole();
						}
						}
						setState(859);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					}
					break;
				case 2:
					{
					setState(860);
					match(ALL);
					setState(861);
					match(PRIVILEGES);
					}
					break;
				}
				setState(864);
				match(ON);
				setState(865);
				grantObject();
				setState(866);
				match(FROM);
				setState(867);
				((RevokePrivilegesContext)_localctx).grantee = principal();
				}
				break;
			case 47:
				_localctx = new DenyContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(869);
				match(DENY);
				setState(880);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
				case 1:
					{
					setState(870);
					privilege();
					setState(875);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(871);
						match(T__3);
						setState(872);
						privilege();
						}
						}
						setState(877);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case 2:
					{
					setState(878);
					match(ALL);
					setState(879);
					match(PRIVILEGES);
					}
					break;
				}
				setState(882);
				match(ON);
				setState(883);
				grantObject();
				setState(884);
				match(TO);
				setState(885);
				((DenyContext)_localctx).grantee = principal();
				}
				break;
			case 48:
				_localctx = new SetRoleContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(887);
				match(SET);
				setState(888);
				match(ROLE);
				setState(892);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
				case 1:
					{
					setState(889);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(890);
					match(NONE);
					}
					break;
				case 3:
					{
					setState(891);
					((SetRoleContext)_localctx).role = identifier();
					}
					break;
				}
				setState(896);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IN) {
					{
					setState(894);
					match(IN);
					setState(895);
					((SetRoleContext)_localctx).catalog = identifier();
					}
				}

				}
				break;
			case 49:
				_localctx = new ShowGrantsContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(898);
				match(SHOW);
				setState(899);
				match(GRANTS);
				setState(902);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(900);
					match(ON);
					setState(901);
					grantObject();
					}
				}

				}
				break;
			case 50:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(904);
				match(EXPLAIN);
				setState(916);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
				case 1:
					{
					setState(905);
					match(T__1);
					setState(906);
					explainOption();
					setState(911);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(907);
						match(T__3);
						setState(908);
						explainOption();
						}
						}
						setState(913);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(914);
					match(T__2);
					}
					break;
				}
				setState(918);
				statement();
				}
				break;
			case 51:
				_localctx = new ExplainAnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(919);
				match(EXPLAIN);
				setState(920);
				match(ANALYZE);
				setState(922);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==VERBOSE) {
					{
					setState(921);
					match(VERBOSE);
					}
				}

				setState(924);
				statement();
				}
				break;
			case 52:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(925);
				match(SHOW);
				setState(926);
				match(CREATE);
				setState(927);
				match(TABLE);
				setState(928);
				qualifiedName();
				}
				break;
			case 53:
				_localctx = new ShowCreateSchemaContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(929);
				match(SHOW);
				setState(930);
				match(CREATE);
				setState(931);
				match(SCHEMA);
				setState(932);
				qualifiedName();
				}
				break;
			case 54:
				_localctx = new ShowCreateViewContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(933);
				match(SHOW);
				setState(934);
				match(CREATE);
				setState(935);
				match(VIEW);
				setState(936);
				qualifiedName();
				}
				break;
			case 55:
				_localctx = new ShowCreateMaterializedViewContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(937);
				match(SHOW);
				setState(938);
				match(CREATE);
				setState(939);
				match(MATERIALIZED);
				setState(940);
				match(VIEW);
				setState(941);
				qualifiedName();
				}
				break;
			case 56:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(942);
				match(SHOW);
				setState(943);
				match(TABLES);
				setState(946);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(944);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(945);
					qualifiedName();
					}
				}

				setState(954);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(948);
					match(LIKE);
					setState(949);
					((ShowTablesContext)_localctx).pattern = string();
					setState(952);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(950);
						match(ESCAPE);
						setState(951);
						((ShowTablesContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 57:
				_localctx = new ShowSchemasContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(956);
				match(SHOW);
				setState(957);
				match(SCHEMAS);
				setState(960);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(958);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(959);
					identifier();
					}
				}

				setState(968);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(962);
					match(LIKE);
					setState(963);
					((ShowSchemasContext)_localctx).pattern = string();
					setState(966);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(964);
						match(ESCAPE);
						setState(965);
						((ShowSchemasContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 58:
				_localctx = new ShowCatalogsContext(_localctx);
				enterOuterAlt(_localctx, 58);
				{
				setState(970);
				match(SHOW);
				setState(971);
				match(CATALOGS);
				setState(978);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(972);
					match(LIKE);
					setState(973);
					((ShowCatalogsContext)_localctx).pattern = string();
					setState(976);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(974);
						match(ESCAPE);
						setState(975);
						((ShowCatalogsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 59:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 59);
				{
				setState(980);
				match(SHOW);
				setState(981);
				match(COLUMNS);
				setState(982);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(983);
				qualifiedName();
				setState(990);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(984);
					match(LIKE);
					setState(985);
					((ShowColumnsContext)_localctx).pattern = string();
					setState(988);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(986);
						match(ESCAPE);
						setState(987);
						((ShowColumnsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 60:
				_localctx = new ShowStatsContext(_localctx);
				enterOuterAlt(_localctx, 60);
				{
				setState(992);
				match(SHOW);
				setState(993);
				match(STATS);
				setState(994);
				match(FOR);
				setState(995);
				qualifiedName();
				}
				break;
			case 61:
				_localctx = new ShowStatsForQueryContext(_localctx);
				enterOuterAlt(_localctx, 61);
				{
				setState(996);
				match(SHOW);
				setState(997);
				match(STATS);
				setState(998);
				match(FOR);
				setState(999);
				match(T__1);
				setState(1000);
				rootQuery();
				setState(1001);
				match(T__2);
				}
				break;
			case 62:
				_localctx = new ShowRolesContext(_localctx);
				enterOuterAlt(_localctx, 62);
				{
				setState(1003);
				match(SHOW);
				setState(1005);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CURRENT) {
					{
					setState(1004);
					match(CURRENT);
					}
				}

				setState(1007);
				match(ROLES);
				setState(1010);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(1008);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1009);
					identifier();
					}
				}

				}
				break;
			case 63:
				_localctx = new ShowRoleGrantsContext(_localctx);
				enterOuterAlt(_localctx, 63);
				{
				setState(1012);
				match(SHOW);
				setState(1013);
				match(ROLE);
				setState(1014);
				match(GRANTS);
				setState(1017);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(1015);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1016);
					identifier();
					}
				}

				}
				break;
			case 64:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 64);
				{
				setState(1019);
				match(DESCRIBE);
				setState(1020);
				qualifiedName();
				}
				break;
			case 65:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 65);
				{
				setState(1021);
				match(DESC);
				setState(1022);
				qualifiedName();
				}
				break;
			case 66:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 66);
				{
				setState(1023);
				match(SHOW);
				setState(1024);
				match(FUNCTIONS);
				setState(1027);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(1025);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1026);
					qualifiedName();
					}
				}

				setState(1035);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(1029);
					match(LIKE);
					setState(1030);
					((ShowFunctionsContext)_localctx).pattern = string();
					setState(1033);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(1031);
						match(ESCAPE);
						setState(1032);
						((ShowFunctionsContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 67:
				_localctx = new ShowSessionContext(_localctx);
				enterOuterAlt(_localctx, 67);
				{
				setState(1037);
				match(SHOW);
				setState(1038);
				match(SESSION);
				setState(1045);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(1039);
					match(LIKE);
					setState(1040);
					((ShowSessionContext)_localctx).pattern = string();
					setState(1043);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ESCAPE) {
						{
						setState(1041);
						match(ESCAPE);
						setState(1042);
						((ShowSessionContext)_localctx).escape = string();
						}
					}

					}
				}

				}
				break;
			case 68:
				_localctx = new SetSessionAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 68);
				{
				setState(1047);
				match(SET);
				setState(1048);
				match(SESSION);
				setState(1049);
				match(AUTHORIZATION);
				setState(1050);
				authorizationUser();
				}
				break;
			case 69:
				_localctx = new ResetSessionAuthorizationContext(_localctx);
				enterOuterAlt(_localctx, 69);
				{
				setState(1051);
				match(RESET);
				setState(1052);
				match(SESSION);
				setState(1053);
				match(AUTHORIZATION);
				}
				break;
			case 70:
				_localctx = new SetSessionContext(_localctx);
				enterOuterAlt(_localctx, 70);
				{
				setState(1054);
				match(SET);
				setState(1055);
				match(SESSION);
				setState(1056);
				qualifiedName();
				setState(1057);
				match(EQ);
				setState(1058);
				expression();
				}
				break;
			case 71:
				_localctx = new ResetSessionContext(_localctx);
				enterOuterAlt(_localctx, 71);
				{
				setState(1060);
				match(RESET);
				setState(1061);
				match(SESSION);
				setState(1062);
				qualifiedName();
				}
				break;
			case 72:
				_localctx = new StartTransactionContext(_localctx);
				enterOuterAlt(_localctx, 72);
				{
				setState(1063);
				match(START);
				setState(1064);
				match(TRANSACTION);
				setState(1073);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ISOLATION || _la==READ) {
					{
					setState(1065);
					transactionMode();
					setState(1070);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1066);
						match(T__3);
						setState(1067);
						transactionMode();
						}
						}
						setState(1072);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 73:
				_localctx = new CommitContext(_localctx);
				enterOuterAlt(_localctx, 73);
				{
				setState(1075);
				match(COMMIT);
				setState(1077);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WORK) {
					{
					setState(1076);
					match(WORK);
					}
				}

				}
				break;
			case 74:
				_localctx = new RollbackContext(_localctx);
				enterOuterAlt(_localctx, 74);
				{
				setState(1079);
				match(ROLLBACK);
				setState(1081);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WORK) {
					{
					setState(1080);
					match(WORK);
					}
				}

				}
				break;
			case 75:
				_localctx = new PrepareContext(_localctx);
				enterOuterAlt(_localctx, 75);
				{
				setState(1083);
				match(PREPARE);
				setState(1084);
				identifier();
				setState(1085);
				match(FROM);
				setState(1086);
				statement();
				}
				break;
			case 76:
				_localctx = new DeallocateContext(_localctx);
				enterOuterAlt(_localctx, 76);
				{
				setState(1088);
				match(DEALLOCATE);
				setState(1089);
				match(PREPARE);
				setState(1090);
				identifier();
				}
				break;
			case 77:
				_localctx = new ExecuteContext(_localctx);
				enterOuterAlt(_localctx, 77);
				{
				setState(1091);
				match(EXECUTE);
				setState(1092);
				identifier();
				setState(1102);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(1093);
					match(USING);
					setState(1094);
					expression();
					setState(1099);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1095);
						match(T__3);
						setState(1096);
						expression();
						}
						}
						setState(1101);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 78:
				_localctx = new ExecuteImmediateContext(_localctx);
				enterOuterAlt(_localctx, 78);
				{
				setState(1104);
				match(EXECUTE);
				setState(1105);
				match(IMMEDIATE);
				setState(1106);
				string();
				setState(1116);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(1107);
					match(USING);
					setState(1108);
					expression();
					setState(1113);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1109);
						match(T__3);
						setState(1110);
						expression();
						}
						}
						setState(1115);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 79:
				_localctx = new DescribeInputContext(_localctx);
				enterOuterAlt(_localctx, 79);
				{
				setState(1118);
				match(DESCRIBE);
				setState(1119);
				match(INPUT);
				setState(1120);
				identifier();
				}
				break;
			case 80:
				_localctx = new DescribeOutputContext(_localctx);
				enterOuterAlt(_localctx, 80);
				{
				setState(1121);
				match(DESCRIBE);
				setState(1122);
				match(OUTPUT);
				setState(1123);
				identifier();
				}
				break;
			case 81:
				_localctx = new SetPathContext(_localctx);
				enterOuterAlt(_localctx, 81);
				{
				setState(1124);
				match(SET);
				setState(1125);
				match(PATH);
				setState(1126);
				pathSpecification();
				}
				break;
			case 82:
				_localctx = new SetTimeZoneContext(_localctx);
				enterOuterAlt(_localctx, 82);
				{
				setState(1127);
				match(SET);
				setState(1128);
				match(TIME);
				setState(1129);
				match(ZONE);
				setState(1132);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
				case 1:
					{
					setState(1130);
					match(LOCAL);
					}
					break;
				case 2:
					{
					setState(1131);
					expression();
					}
					break;
				}
				}
				break;
			case 83:
				_localctx = new UpdateContext(_localctx);
				enterOuterAlt(_localctx, 83);
				{
				setState(1134);
				match(UPDATE);
				setState(1135);
				qualifiedName();
				setState(1136);
				match(SET);
				setState(1137);
				updateAssignment();
				setState(1142);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1138);
					match(T__3);
					setState(1139);
					updateAssignment();
					}
					}
					setState(1144);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1147);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1145);
					match(WHERE);
					setState(1146);
					((UpdateContext)_localctx).where = booleanExpression(0);
					}
				}

				}
				break;
			case 84:
				_localctx = new MergeContext(_localctx);
				enterOuterAlt(_localctx, 84);
				{
				setState(1149);
				match(MERGE);
				setState(1150);
				match(INTO);
				setState(1151);
				qualifiedName();
				setState(1156);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514849800353L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
					{
					setState(1153);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1152);
						match(AS);
						}
					}

					setState(1155);
					identifier();
					}
				}

				setState(1158);
				match(USING);
				setState(1159);
				relation(0);
				setState(1160);
				match(ON);
				setState(1161);
				expression();
				setState(1163); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1162);
					mergeCase();
					}
					}
					setState(1165); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
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
	public static class RootQueryContext extends ParserRuleContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public WithFunctionContext withFunction() {
			return getRuleContext(WithFunctionContext.class,0);
		}
		public RootQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rootQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRootQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRootQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRootQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RootQueryContext rootQuery() throws RecognitionException {
		RootQueryContext _localctx = new RootQueryContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_rootQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1170);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,119,_ctx) ) {
			case 1:
				{
				setState(1169);
				withFunction();
				}
				break;
			}
			setState(1172);
			query();
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
	public static class WithFunctionContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<FunctionSpecificationContext> functionSpecification() {
			return getRuleContexts(FunctionSpecificationContext.class);
		}
		public FunctionSpecificationContext functionSpecification(int i) {
			return getRuleContext(FunctionSpecificationContext.class,i);
		}
		public WithFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_withFunction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWithFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWithFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWithFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WithFunctionContext withFunction() throws RecognitionException {
		WithFunctionContext _localctx = new WithFunctionContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_withFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1174);
			match(WITH);
			setState(1175);
			functionSpecification();
			setState(1180);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1176);
				match(T__3);
				setState(1177);
				functionSpecification();
				}
				}
				setState(1182);
				_errHandler.sync(this);
				_la = _input.LA(1);
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
	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public WithContext with() {
			return getRuleContext(WithContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1184);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1183);
				with();
				}
			}

			setState(1186);
			queryNoWith();
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
	public static class WithContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public TerminalNode RECURSIVE() { return getToken(SqlBaseParser.RECURSIVE, 0); }
		public WithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WithContext with() throws RecognitionException {
		WithContext _localctx = new WithContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_with);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1188);
			match(WITH);
			setState(1190);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RECURSIVE) {
				{
				setState(1189);
				match(RECURSIVE);
				}
			}

			setState(1192);
			namedQuery();
			setState(1197);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1193);
				match(T__3);
				setState(1194);
				namedQuery();
				}
				}
				setState(1199);
				_errHandler.sync(this);
				_la = _input.LA(1);
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
	public static class TableElementContext extends ParserRuleContext {
		public ColumnDefinitionContext columnDefinition() {
			return getRuleContext(ColumnDefinitionContext.class,0);
		}
		public LikeClauseContext likeClause() {
			return getRuleContext(LikeClauseContext.class,0);
		}
		public TableElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableElement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableElement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableElementContext tableElement() throws RecognitionException {
		TableElementContext _localctx = new TableElementContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_tableElement);
		try {
			setState(1202);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1200);
				columnDefinition();
				}
				break;
			case LIKE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1201);
				likeClause();
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
	public static class ColumnDefinitionContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public PropertiesContext properties() {
			return getRuleContext(PropertiesContext.class,0);
		}
		public ColumnDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnDefinitionContext columnDefinition() throws RecognitionException {
		ColumnDefinitionContext _localctx = new ColumnDefinitionContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_columnDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1204);
			qualifiedName();
			setState(1205);
			type(0);
			setState(1208);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(1206);
				match(NOT);
				setState(1207);
				match(NULL);
				}
			}

			setState(1212);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1210);
				match(COMMENT);
				setState(1211);
				string();
				}
			}

			setState(1216);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1214);
				match(WITH);
				setState(1215);
				properties();
				}
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
	public static class LikeClauseContext extends ParserRuleContext {
		public Token optionType;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public LikeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_likeClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLikeClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLikeClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLikeClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LikeClauseContext likeClause() throws RecognitionException {
		LikeClauseContext _localctx = new LikeClauseContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_likeClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1218);
			match(LIKE);
			setState(1219);
			qualifiedName();
			setState(1222);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXCLUDING || _la==INCLUDING) {
				{
				setState(1220);
				((LikeClauseContext)_localctx).optionType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==EXCLUDING || _la==INCLUDING) ) {
					((LikeClauseContext)_localctx).optionType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1221);
				match(PROPERTIES);
				}
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
	public static class PropertiesContext extends ParserRuleContext {
		public PropertyAssignmentsContext propertyAssignments() {
			return getRuleContext(PropertyAssignmentsContext.class,0);
		}
		public PropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertiesContext properties() throws RecognitionException {
		PropertiesContext _localctx = new PropertiesContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_properties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1224);
			match(T__1);
			setState(1225);
			propertyAssignments();
			setState(1226);
			match(T__2);
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
	public static class PropertyAssignmentsContext extends ParserRuleContext {
		public List<PropertyContext> property() {
			return getRuleContexts(PropertyContext.class);
		}
		public PropertyContext property(int i) {
			return getRuleContext(PropertyContext.class,i);
		}
		public PropertyAssignmentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyAssignments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPropertyAssignments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPropertyAssignments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPropertyAssignments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyAssignmentsContext propertyAssignments() throws RecognitionException {
		PropertyAssignmentsContext _localctx = new PropertyAssignmentsContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_propertyAssignments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1228);
			property();
			setState(1233);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1229);
				match(T__3);
				setState(1230);
				property();
				}
				}
				setState(1235);
				_errHandler.sync(this);
				_la = _input.LA(1);
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
	public static class PropertyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public PropertyValueContext propertyValue() {
			return getRuleContext(PropertyValueContext.class,0);
		}
		public PropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyContext property() throws RecognitionException {
		PropertyContext _localctx = new PropertyContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_property);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1236);
			identifier();
			setState(1237);
			match(EQ);
			setState(1238);
			propertyValue();
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
	public static class PropertyValueContext extends ParserRuleContext {
		public PropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyValue; }
	 
		public PropertyValueContext() { }
		public void copyFrom(PropertyValueContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DefaultPropertyValueContext extends PropertyValueContext {
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public DefaultPropertyValueContext(PropertyValueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDefaultPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDefaultPropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDefaultPropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NonDefaultPropertyValueContext extends PropertyValueContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NonDefaultPropertyValueContext(PropertyValueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonDefaultPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonDefaultPropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonDefaultPropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyValueContext propertyValue() throws RecognitionException {
		PropertyValueContext _localctx = new PropertyValueContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_propertyValue);
		try {
			setState(1242);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,130,_ctx) ) {
			case 1:
				_localctx = new DefaultPropertyValueContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1240);
				match(DEFAULT);
				}
				break;
			case 2:
				_localctx = new NonDefaultPropertyValueContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1241);
				expression();
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
	public static class QueryNoWithContext extends ParserRuleContext {
		public RowCountContext offset;
		public LimitRowCountContext limit;
		public RowCountContext fetchFirst;
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode OFFSET() { return getToken(SqlBaseParser.OFFSET, 0); }
		public List<RowCountContext> rowCount() {
			return getRuleContexts(RowCountContext.class);
		}
		public RowCountContext rowCount(int i) {
			return getRuleContext(RowCountContext.class,i);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public LimitRowCountContext limitRowCount() {
			return getRuleContext(LimitRowCountContext.class,0);
		}
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode NEXT() { return getToken(SqlBaseParser.NEXT, 0); }
		public List<TerminalNode> ROW() { return getTokens(SqlBaseParser.ROW); }
		public TerminalNode ROW(int i) {
			return getToken(SqlBaseParser.ROW, i);
		}
		public List<TerminalNode> ROWS() { return getTokens(SqlBaseParser.ROWS); }
		public TerminalNode ROWS(int i) {
			return getToken(SqlBaseParser.ROWS, i);
		}
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode TIES() { return getToken(SqlBaseParser.TIES, 0); }
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryNoWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryNoWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryNoWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_queryNoWith);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1244);
			queryTerm(0);
			setState(1255);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1245);
				match(ORDER);
				setState(1246);
				match(BY);
				setState(1247);
				sortItem();
				setState(1252);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1248);
					match(T__3);
					setState(1249);
					sortItem();
					}
					}
					setState(1254);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1262);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(1257);
				match(OFFSET);
				setState(1258);
				((QueryNoWithContext)_localctx).offset = rowCount();
				setState(1260);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW || _la==ROWS) {
					{
					setState(1259);
					_la = _input.LA(1);
					if ( !(_la==ROW || _la==ROWS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
			}

			setState(1277);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LIMIT:
				{
				{
				setState(1264);
				match(LIMIT);
				setState(1265);
				((QueryNoWithContext)_localctx).limit = limitRowCount();
				}
				}
				break;
			case FETCH:
				{
				{
				setState(1266);
				match(FETCH);
				setState(1267);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==NEXT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1269);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==QUESTION_MARK || _la==INTEGER_VALUE) {
					{
					setState(1268);
					((QueryNoWithContext)_localctx).fetchFirst = rowCount();
					}
				}

				setState(1271);
				_la = _input.LA(1);
				if ( !(_la==ROW || _la==ROWS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1275);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ONLY:
					{
					setState(1272);
					match(ONLY);
					}
					break;
				case WITH:
					{
					setState(1273);
					match(WITH);
					setState(1274);
					match(TIES);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				break;
			case EOF:
			case T__2:
			case WITH:
				break;
			default:
				break;
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
	public static class LimitRowCountContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public RowCountContext rowCount() {
			return getRuleContext(RowCountContext.class,0);
		}
		public LimitRowCountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitRowCount; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLimitRowCount(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLimitRowCount(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLimitRowCount(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitRowCountContext limitRowCount() throws RecognitionException {
		LimitRowCountContext _localctx = new LimitRowCountContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_limitRowCount);
		try {
			setState(1281);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALL:
				enterOuterAlt(_localctx, 1);
				{
				setState(1279);
				match(ALL);
				}
				break;
			case QUESTION_MARK:
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1280);
				rowCount();
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
	public static class RowCountContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public RowCountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowCount; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowCount(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowCount(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowCount(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowCountContext rowCount() throws RecognitionException {
		RowCountContext _localctx = new RowCountContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_rowCount);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1283);
			_la = _input.LA(1);
			if ( !(_la==QUESTION_MARK || _la==INTEGER_VALUE) ) {
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
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 42;
		enterRecursionRule(_localctx, 42, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1286);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1302);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,142,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1300);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,141,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1288);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1289);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(1291);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1290);
							setQuantifier();
							}
						}

						setState(1293);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1294);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1295);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1297);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1296);
							setQuantifier();
							}
						}

						setState(1299);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(1304);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,142,_ctx);
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
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends QueryPrimaryContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public InlineTableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_queryPrimary);
		try {
			int _alt;
			setState(1321);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1305);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1306);
				match(TABLE);
				setState(1307);
				qualifiedName();
				}
				break;
			case VALUES:
				_localctx = new InlineTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1308);
				match(VALUES);
				setState(1309);
				expression();
				setState(1314);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,143,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1310);
						match(T__3);
						setState(1311);
						expression();
						}
						} 
					}
					setState(1316);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,143,_ctx);
				}
				}
				break;
			case T__1:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1317);
				match(T__1);
				setState(1318);
				queryNoWith();
				setState(1319);
				match(T__2);
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
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrdering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1323);
			expression();
			setState(1325);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1324);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1329);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1327);
				match(NULLS);
				setState(1328);
				((SortItemContext)_localctx).nullOrdering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
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
	public static class QuerySpecificationContext extends ParserRuleContext {
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<WindowDefinitionContext> windowDefinition() {
			return getRuleContexts(WindowDefinitionContext.class);
		}
		public WindowDefinitionContext windowDefinition(int i) {
			return getRuleContext(WindowDefinitionContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_querySpecification);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1331);
			match(SELECT);
			setState(1333);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,147,_ctx) ) {
			case 1:
				{
				setState(1332);
				setQuantifier();
				}
				break;
			}
			setState(1335);
			selectItem();
			setState(1340);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,148,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1336);
					match(T__3);
					setState(1337);
					selectItem();
					}
					} 
				}
				setState(1342);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,148,_ctx);
			}
			setState(1352);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,150,_ctx) ) {
			case 1:
				{
				setState(1343);
				match(FROM);
				setState(1344);
				relation(0);
				setState(1349);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,149,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1345);
						match(T__3);
						setState(1346);
						relation(0);
						}
						} 
					}
					setState(1351);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,149,_ctx);
				}
				}
				break;
			}
			setState(1356);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,151,_ctx) ) {
			case 1:
				{
				setState(1354);
				match(WHERE);
				setState(1355);
				((QuerySpecificationContext)_localctx).where = booleanExpression(0);
				}
				break;
			}
			setState(1361);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,152,_ctx) ) {
			case 1:
				{
				setState(1358);
				match(GROUP);
				setState(1359);
				match(BY);
				setState(1360);
				groupBy();
				}
				break;
			}
			setState(1365);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,153,_ctx) ) {
			case 1:
				{
				setState(1363);
				match(HAVING);
				setState(1364);
				((QuerySpecificationContext)_localctx).having = booleanExpression(0);
				}
				break;
			}
			setState(1376);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
			case 1:
				{
				setState(1367);
				match(WINDOW);
				setState(1368);
				windowDefinition();
				setState(1373);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,154,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1369);
						match(T__3);
						setState(1370);
						windowDefinition();
						}
						} 
					}
					setState(1375);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,154,_ctx);
				}
				}
				break;
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
	public static class GroupByContext extends ParserRuleContext {
		public List<GroupingElementContext> groupingElement() {
			return getRuleContexts(GroupingElementContext.class);
		}
		public GroupingElementContext groupingElement(int i) {
			return getRuleContext(GroupingElementContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public GroupByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupBy(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_groupBy);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1379);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,156,_ctx) ) {
			case 1:
				{
				setState(1378);
				setQuantifier();
				}
				break;
			}
			setState(1381);
			groupingElement();
			setState(1386);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,157,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1382);
					match(T__3);
					setState(1383);
					groupingElement();
					}
					} 
				}
				setState(1388);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,157,_ctx);
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
	public static class GroupingElementContext extends ParserRuleContext {
		public GroupingElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingElement; }
	 
		public GroupingElementContext() { }
		public void copyFrom(GroupingElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MultipleGroupingSetsContext extends GroupingElementContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public MultipleGroupingSetsContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipleGroupingSets(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipleGroupingSets(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultipleGroupingSets(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SingleGroupingSetContext extends GroupingElementContext {
		public GroupingSetContext groupingSet() {
			return getRuleContext(GroupingSetContext.class,0);
		}
		public SingleGroupingSetContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CubeContext extends GroupingElementContext {
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public CubeContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCube(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCube(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCube(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RollupContext extends GroupingElementContext {
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public RollupContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollup(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollup(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingElementContext groupingElement() throws RecognitionException {
		GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_groupingElement);
		int _la;
		try {
			setState(1429);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
			case 1:
				_localctx = new SingleGroupingSetContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1389);
				groupingSet();
				}
				break;
			case 2:
				_localctx = new RollupContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1390);
				match(ROLLUP);
				setState(1391);
				match(T__1);
				setState(1400);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
					{
					setState(1392);
					groupingSet();
					setState(1397);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1393);
						match(T__3);
						setState(1394);
						groupingSet();
						}
						}
						setState(1399);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1402);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new CubeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1403);
				match(CUBE);
				setState(1404);
				match(T__1);
				setState(1413);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
					{
					setState(1405);
					groupingSet();
					setState(1410);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1406);
						match(T__3);
						setState(1407);
						groupingSet();
						}
						}
						setState(1412);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1415);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new MultipleGroupingSetsContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1416);
				match(GROUPING);
				setState(1417);
				match(SETS);
				setState(1418);
				match(T__1);
				setState(1419);
				groupingSet();
				setState(1424);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1420);
					match(T__3);
					setState(1421);
					groupingSet();
					}
					}
					setState(1426);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1427);
				match(T__2);
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
	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_groupingSet);
		int _la;
		try {
			setState(1444);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1431);
				match(T__1);
				setState(1440);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
					{
					setState(1432);
					expression();
					setState(1437);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1433);
						match(T__3);
						setState(1434);
						expression();
						}
						}
						setState(1439);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1442);
				match(T__2);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1443);
				expression();
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
	public static class WindowDefinitionContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecificationContext windowSpecification() {
			return getRuleContext(WindowSpecificationContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowDefinitionContext windowDefinition() throws RecognitionException {
		WindowDefinitionContext _localctx = new WindowDefinitionContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_windowDefinition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1446);
			((WindowDefinitionContext)_localctx).name = identifier();
			setState(1447);
			match(AS);
			setState(1448);
			match(T__1);
			setState(1449);
			windowSpecification();
			setState(1450);
			match(T__2);
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
	public static class WindowSpecificationContext extends ParserRuleContext {
		public IdentifierContext existingWindowName;
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecificationContext windowSpecification() throws RecognitionException {
		WindowSpecificationContext _localctx = new WindowSpecificationContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_windowSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1453);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,167,_ctx) ) {
			case 1:
				{
				setState(1452);
				((WindowSpecificationContext)_localctx).existingWindowName = identifier();
				}
				break;
			}
			setState(1465);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(1455);
				match(PARTITION);
				setState(1456);
				match(BY);
				setState(1457);
				((WindowSpecificationContext)_localctx).expression = expression();
				((WindowSpecificationContext)_localctx).partition.add(((WindowSpecificationContext)_localctx).expression);
				setState(1462);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1458);
					match(T__3);
					setState(1459);
					((WindowSpecificationContext)_localctx).expression = expression();
					((WindowSpecificationContext)_localctx).partition.add(((WindowSpecificationContext)_localctx).expression);
					}
					}
					setState(1464);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1477);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1467);
				match(ORDER);
				setState(1468);
				match(BY);
				setState(1469);
				sortItem();
				setState(1474);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1470);
					match(T__3);
					setState(1471);
					sortItem();
					}
					}
					setState(1476);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1480);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUPS || _la==MEASURES || _la==RANGE || _la==ROWS) {
				{
				setState(1479);
				windowFrame();
				}
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
	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1482);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1484);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(1483);
				columnAliases();
				}
			}

			setState(1486);
			match(AS);
			setState(1487);
			match(T__1);
			setState(1488);
			query();
			setState(1489);
			match(T__2);
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
	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1491);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
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
	public static class SelectItemContext extends ParserRuleContext {
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
	 
		public SelectItemContext() { }
		public void copyFrom(SelectItemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectAllContext extends SelectItemContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public SelectAllContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectAll(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SelectSingleContext extends SelectItemContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public SelectSingleContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectSingle(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectSingle(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectSingle(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_selectItem);
		int _la;
		try {
			setState(1508);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
			case 1:
				_localctx = new SelectSingleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1493);
				expression();
				setState(1498);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
				case 1:
					{
					setState(1495);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1494);
						match(AS);
						}
					}

					setState(1497);
					identifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1500);
				primaryExpression(0);
				setState(1501);
				match(T__0);
				setState(1502);
				match(ASTERISK);
				setState(1505);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
				case 1:
					{
					setState(1503);
					match(AS);
					setState(1504);
					columnAliases();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1507);
				match(ASTERISK);
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
	public static class RelationContext extends ParserRuleContext {
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	 
		public RelationContext() { }
		public void copyFrom(RelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RelationDefaultContext extends RelationContext {
		public SampledRelationContext sampledRelation() {
			return getRuleContext(SampledRelationContext.class,0);
		}
		public RelationDefaultContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelationDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelationDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelationDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends RelationContext {
		public RelationContext left;
		public SampledRelationContext right;
		public RelationContext rightRelation;
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public SampledRelationContext sampledRelation() {
			return getRuleContext(SampledRelationContext.class,0);
		}
		public JoinRelationContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		return relation(0);
	}

	private RelationContext relation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RelationContext _localctx = new RelationContext(_ctx, _parentState);
		RelationContext _prevctx = _localctx;
		int _startState = 66;
		enterRecursionRule(_localctx, 66, RULE_relation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new RelationDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1511);
			sampledRelation();
			}
			_ctx.stop = _input.LT(-1);
			setState(1531);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,179,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
					((JoinRelationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_relation);
					setState(1513);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(1527);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case CROSS:
						{
						setState(1514);
						match(CROSS);
						setState(1515);
						match(JOIN);
						setState(1516);
						((JoinRelationContext)_localctx).right = sampledRelation();
						}
						break;
					case FULL:
					case INNER:
					case JOIN:
					case LEFT:
					case RIGHT:
						{
						setState(1517);
						joinType();
						setState(1518);
						match(JOIN);
						setState(1519);
						((JoinRelationContext)_localctx).rightRelation = relation(0);
						setState(1520);
						joinCriteria();
						}
						break;
					case NATURAL:
						{
						setState(1522);
						match(NATURAL);
						setState(1523);
						joinType();
						setState(1524);
						match(JOIN);
						setState(1525);
						((JoinRelationContext)_localctx).right = sampledRelation();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(1533);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,179,_ctx);
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
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_joinType);
		int _la;
		try {
			setState(1549);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case JOIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1535);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1534);
					match(INNER);
					}
				}

				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1537);
				match(LEFT);
				setState(1539);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1538);
					match(OUTER);
					}
				}

				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1541);
				match(RIGHT);
				setState(1543);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1542);
					match(OUTER);
					}
				}

				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(1545);
				match(FULL);
				setState(1547);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1546);
					match(OUTER);
					}
				}

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
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_joinCriteria);
		int _la;
		try {
			setState(1565);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1551);
				match(ON);
				setState(1552);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1553);
				match(USING);
				setState(1554);
				match(T__1);
				setState(1555);
				identifier();
				setState(1560);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1556);
					match(T__3);
					setState(1557);
					identifier();
					}
					}
					setState(1562);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1563);
				match(T__2);
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
	public static class SampledRelationContext extends ParserRuleContext {
		public ExpressionContext percentage;
		public PatternRecognitionContext patternRecognition() {
			return getRuleContext(PatternRecognitionContext.class,0);
		}
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public SampleTypeContext sampleType() {
			return getRuleContext(SampleTypeContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampledRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampledRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampledRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampledRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampledRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampledRelationContext sampledRelation() throws RecognitionException {
		SampledRelationContext _localctx = new SampledRelationContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_sampledRelation);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1567);
			patternRecognition();
			setState(1574);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
			case 1:
				{
				setState(1568);
				match(TABLESAMPLE);
				setState(1569);
				sampleType();
				setState(1570);
				match(T__1);
				setState(1571);
				((SampledRelationContext)_localctx).percentage = expression();
				setState(1572);
				match(T__2);
				}
				break;
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
	public static class SampleTypeContext extends ParserRuleContext {
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public SampleTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleTypeContext sampleType() throws RecognitionException {
		SampleTypeContext _localctx = new SampleTypeContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_sampleType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1576);
			_la = _input.LA(1);
			if ( !(_la==BERNOULLI || _la==SYSTEM) ) {
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
	public static class TrimsSpecificationContext extends ParserRuleContext {
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TrimsSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trimsSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTrimsSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTrimsSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTrimsSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TrimsSpecificationContext trimsSpecification() throws RecognitionException {
		TrimsSpecificationContext _localctx = new TrimsSpecificationContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_trimsSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1578);
			_la = _input.LA(1);
			if ( !(_la==BOTH || _la==LEADING || _la==TRAILING) ) {
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
	public static class ListAggOverflowBehaviorContext extends ParserRuleContext {
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public ListaggCountIndicationContext listaggCountIndication() {
			return getRuleContext(ListaggCountIndicationContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public ListAggOverflowBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listAggOverflowBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterListAggOverflowBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitListAggOverflowBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitListAggOverflowBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListAggOverflowBehaviorContext listAggOverflowBehavior() throws RecognitionException {
		ListAggOverflowBehaviorContext _localctx = new ListAggOverflowBehaviorContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_listAggOverflowBehavior);
		int _la;
		try {
			setState(1586);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ERROR:
				enterOuterAlt(_localctx, 1);
				{
				setState(1580);
				match(ERROR);
				}
				break;
			case TRUNCATE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1581);
				match(TRUNCATE);
				setState(1583);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STRING || _la==UNICODE_STRING) {
					{
					setState(1582);
					string();
					}
				}

				setState(1585);
				listaggCountIndication();
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
	public static class ListaggCountIndicationContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode COUNT() { return getToken(SqlBaseParser.COUNT, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public ListaggCountIndicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listaggCountIndication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterListaggCountIndication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitListaggCountIndication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitListaggCountIndication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListaggCountIndicationContext listaggCountIndication() throws RecognitionException {
		ListaggCountIndicationContext _localctx = new ListaggCountIndicationContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_listaggCountIndication);
		try {
			setState(1592);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WITH:
				enterOuterAlt(_localctx, 1);
				{
				setState(1588);
				match(WITH);
				setState(1589);
				match(COUNT);
				}
				break;
			case WITHOUT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1590);
				match(WITHOUT);
				setState(1591);
				match(COUNT);
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
	public static class PatternRecognitionContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public TerminalNode MATCH_RECOGNIZE() { return getToken(SqlBaseParser.MATCH_RECOGNIZE, 0); }
		public TerminalNode PATTERN() { return getToken(SqlBaseParser.PATTERN, 0); }
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public TerminalNode DEFINE() { return getToken(SqlBaseParser.DEFINE, 0); }
		public List<VariableDefinitionContext> variableDefinition() {
			return getRuleContexts(VariableDefinitionContext.class);
		}
		public VariableDefinitionContext variableDefinition(int i) {
			return getRuleContext(VariableDefinitionContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode MEASURES() { return getToken(SqlBaseParser.MEASURES, 0); }
		public List<MeasureDefinitionContext> measureDefinition() {
			return getRuleContexts(MeasureDefinitionContext.class);
		}
		public MeasureDefinitionContext measureDefinition(int i) {
			return getRuleContext(MeasureDefinitionContext.class,i);
		}
		public RowsPerMatchContext rowsPerMatch() {
			return getRuleContext(RowsPerMatchContext.class,0);
		}
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public SkipToContext skipTo() {
			return getRuleContext(SkipToContext.class,0);
		}
		public TerminalNode SUBSET() { return getToken(SqlBaseParser.SUBSET, 0); }
		public List<SubsetDefinitionContext> subsetDefinition() {
			return getRuleContexts(SubsetDefinitionContext.class);
		}
		public SubsetDefinitionContext subsetDefinition(int i) {
			return getRuleContext(SubsetDefinitionContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode INITIAL() { return getToken(SqlBaseParser.INITIAL, 0); }
		public TerminalNode SEEK() { return getToken(SqlBaseParser.SEEK, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public PatternRecognitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patternRecognition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternRecognition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternRecognition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternRecognition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatternRecognitionContext patternRecognition() throws RecognitionException {
		PatternRecognitionContext _localctx = new PatternRecognitionContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_patternRecognition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1594);
			aliasedRelation();
			setState(1677);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
			case 1:
				{
				setState(1595);
				match(MATCH_RECOGNIZE);
				setState(1596);
				match(T__1);
				setState(1607);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1597);
					match(PARTITION);
					setState(1598);
					match(BY);
					setState(1599);
					((PatternRecognitionContext)_localctx).expression = expression();
					((PatternRecognitionContext)_localctx).partition.add(((PatternRecognitionContext)_localctx).expression);
					setState(1604);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1600);
						match(T__3);
						setState(1601);
						((PatternRecognitionContext)_localctx).expression = expression();
						((PatternRecognitionContext)_localctx).partition.add(((PatternRecognitionContext)_localctx).expression);
						}
						}
						setState(1606);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1619);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(1609);
					match(ORDER);
					setState(1610);
					match(BY);
					setState(1611);
					sortItem();
					setState(1616);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1612);
						match(T__3);
						setState(1613);
						sortItem();
						}
						}
						setState(1618);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1630);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MEASURES) {
					{
					setState(1621);
					match(MEASURES);
					setState(1622);
					measureDefinition();
					setState(1627);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1623);
						match(T__3);
						setState(1624);
						measureDefinition();
						}
						}
						setState(1629);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1633);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ALL || _la==ONE) {
					{
					setState(1632);
					rowsPerMatch();
					}
				}

				setState(1638);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER) {
					{
					setState(1635);
					match(AFTER);
					setState(1636);
					match(MATCH);
					setState(1637);
					skipTo();
					}
				}

				setState(1641);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INITIAL || _la==SEEK) {
					{
					setState(1640);
					_la = _input.LA(1);
					if ( !(_la==INITIAL || _la==SEEK) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(1643);
				match(PATTERN);
				setState(1644);
				match(T__1);
				setState(1645);
				rowPattern(0);
				setState(1646);
				match(T__2);
				setState(1656);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SUBSET) {
					{
					setState(1647);
					match(SUBSET);
					setState(1648);
					subsetDefinition();
					setState(1653);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(1649);
						match(T__3);
						setState(1650);
						subsetDefinition();
						}
						}
						setState(1655);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1658);
				match(DEFINE);
				setState(1659);
				variableDefinition();
				setState(1664);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1660);
					match(T__3);
					setState(1661);
					variableDefinition();
					}
					}
					setState(1666);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1667);
				match(T__2);
				setState(1675);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,205,_ctx) ) {
				case 1:
					{
					setState(1669);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(1668);
						match(AS);
						}
					}

					setState(1671);
					identifier();
					setState(1673);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,204,_ctx) ) {
					case 1:
						{
						setState(1672);
						columnAliases();
						}
						break;
					}
					}
					break;
				}
				}
				break;
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
	public static class MeasureDefinitionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public MeasureDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_measureDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMeasureDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMeasureDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMeasureDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MeasureDefinitionContext measureDefinition() throws RecognitionException {
		MeasureDefinitionContext _localctx = new MeasureDefinitionContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_measureDefinition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1679);
			expression();
			setState(1680);
			match(AS);
			setState(1681);
			identifier();
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
	public static class RowsPerMatchContext extends ParserRuleContext {
		public TerminalNode ONE() { return getToken(SqlBaseParser.ONE, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode PER() { return getToken(SqlBaseParser.PER, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public EmptyMatchHandlingContext emptyMatchHandling() {
			return getRuleContext(EmptyMatchHandlingContext.class,0);
		}
		public RowsPerMatchContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowsPerMatch; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowsPerMatch(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowsPerMatch(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowsPerMatch(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowsPerMatchContext rowsPerMatch() throws RecognitionException {
		RowsPerMatchContext _localctx = new RowsPerMatchContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_rowsPerMatch);
		int _la;
		try {
			setState(1694);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1683);
				match(ONE);
				setState(1684);
				match(ROW);
				setState(1685);
				match(PER);
				setState(1686);
				match(MATCH);
				}
				break;
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1687);
				match(ALL);
				setState(1688);
				match(ROWS);
				setState(1689);
				match(PER);
				setState(1690);
				match(MATCH);
				setState(1692);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OMIT || _la==SHOW || _la==WITH) {
					{
					setState(1691);
					emptyMatchHandling();
					}
				}

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
	public static class EmptyMatchHandlingContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode MATCHES() { return getToken(SqlBaseParser.MATCHES, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode UNMATCHED() { return getToken(SqlBaseParser.UNMATCHED, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public EmptyMatchHandlingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_emptyMatchHandling; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterEmptyMatchHandling(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitEmptyMatchHandling(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitEmptyMatchHandling(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EmptyMatchHandlingContext emptyMatchHandling() throws RecognitionException {
		EmptyMatchHandlingContext _localctx = new EmptyMatchHandlingContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_emptyMatchHandling);
		try {
			setState(1705);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SHOW:
				enterOuterAlt(_localctx, 1);
				{
				setState(1696);
				match(SHOW);
				setState(1697);
				match(EMPTY);
				setState(1698);
				match(MATCHES);
				}
				break;
			case OMIT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1699);
				match(OMIT);
				setState(1700);
				match(EMPTY);
				setState(1701);
				match(MATCHES);
				}
				break;
			case WITH:
				enterOuterAlt(_localctx, 3);
				{
				setState(1702);
				match(WITH);
				setState(1703);
				match(UNMATCHED);
				setState(1704);
				match(ROWS);
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
	public static class SkipToContext extends ParserRuleContext {
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode NEXT() { return getToken(SqlBaseParser.NEXT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode PAST() { return getToken(SqlBaseParser.PAST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public SkipToContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skipTo; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSkipTo(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSkipTo(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSkipTo(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkipToContext skipTo() throws RecognitionException {
		SkipToContext _localctx = new SkipToContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_skipTo);
		try {
			setState(1726);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1707);
				match(T__4);
				setState(1708);
				match(TO);
				setState(1709);
				match(NEXT);
				setState(1710);
				match(ROW);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1711);
				match(T__4);
				setState(1712);
				match(PAST);
				setState(1713);
				match(LAST);
				setState(1714);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1715);
				match(T__4);
				setState(1716);
				match(TO);
				setState(1717);
				match(FIRST);
				setState(1718);
				identifier();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1719);
				match(T__4);
				setState(1720);
				match(TO);
				setState(1721);
				match(LAST);
				setState(1722);
				identifier();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1723);
				match(T__4);
				setState(1724);
				match(TO);
				setState(1725);
				identifier();
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
	public static class SubsetDefinitionContext extends ParserRuleContext {
		public IdentifierContext name;
		public IdentifierContext identifier;
		public List<IdentifierContext> union = new ArrayList<IdentifierContext>();
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public SubsetDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subsetDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubsetDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubsetDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubsetDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubsetDefinitionContext subsetDefinition() throws RecognitionException {
		SubsetDefinitionContext _localctx = new SubsetDefinitionContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_subsetDefinition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1728);
			((SubsetDefinitionContext)_localctx).name = identifier();
			setState(1729);
			match(EQ);
			setState(1730);
			match(T__1);
			setState(1731);
			((SubsetDefinitionContext)_localctx).identifier = identifier();
			((SubsetDefinitionContext)_localctx).union.add(((SubsetDefinitionContext)_localctx).identifier);
			setState(1736);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1732);
				match(T__3);
				setState(1733);
				((SubsetDefinitionContext)_localctx).identifier = identifier();
				((SubsetDefinitionContext)_localctx).union.add(((SubsetDefinitionContext)_localctx).identifier);
				}
				}
				setState(1738);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1739);
			match(T__2);
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
	public static class VariableDefinitionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public VariableDefinitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variableDefinition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterVariableDefinition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitVariableDefinition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitVariableDefinition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableDefinitionContext variableDefinition() throws RecognitionException {
		VariableDefinitionContext _localctx = new VariableDefinitionContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_variableDefinition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1741);
			identifier();
			setState(1742);
			match(AS);
			setState(1743);
			expression();
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
	public static class AliasedRelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasedRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AliasedRelationContext aliasedRelation() throws RecognitionException {
		AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_aliasedRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1745);
			relationPrimary();
			setState(1753);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
			case 1:
				{
				setState(1747);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1746);
					match(AS);
					}
				}

				setState(1749);
				identifier();
				setState(1751);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
				case 1:
					{
					setState(1750);
					columnAliases();
					}
					break;
				}
				}
				break;
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
	public static class ColumnAliasesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnAliases; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnAliases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnAliases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnAliases(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnAliasesContext columnAliases() throws RecognitionException {
		ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_columnAliases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1755);
			match(T__1);
			setState(1756);
			identifier();
			setState(1761);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1757);
				match(T__3);
				setState(1758);
				identifier();
				}
				}
				setState(1763);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1764);
			match(T__2);
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
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryRelationContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonTableContext extends RelationPrimaryContext {
		public TerminalNode JSON_TABLE() { return getToken(SqlBaseParser.JSON_TABLE, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public List<JsonTableColumnContext> jsonTableColumn() {
			return getRuleContexts(JsonTableColumnContext.class);
		}
		public JsonTableColumnContext jsonTableColumn(int i) {
			return getRuleContext(JsonTableColumnContext.class,i);
		}
		public TerminalNode PLAN() { return getToken(SqlBaseParser.PLAN, 0); }
		public JsonTableSpecificPlanContext jsonTableSpecificPlan() {
			return getRuleContext(JsonTableSpecificPlanContext.class,0);
		}
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public JsonTableDefaultPlanContext jsonTableDefaultPlan() {
			return getRuleContext(JsonTableDefaultPlanContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public List<TerminalNode> ERROR() { return getTokens(SqlBaseParser.ERROR); }
		public TerminalNode ERROR(int i) {
			return getToken(SqlBaseParser.ERROR, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public JsonTableContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public ParenthesizedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnnestContext extends RelationPrimaryContext {
		public TerminalNode UNNEST() { return getToken(SqlBaseParser.UNNEST, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public UnnestContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnnest(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnnest(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnnest(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableFunctionInvocationContext extends RelationPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableFunctionCallContext tableFunctionCall() {
			return getRuleContext(TableFunctionCallContext.class,0);
		}
		public TableFunctionInvocationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFunctionInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFunctionInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFunctionInvocation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LateralContext extends RelationPrimaryContext {
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public LateralContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLateral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public QueryPeriodContext queryPeriod() {
			return getRuleContext(QueryPeriodContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_relationPrimary);
		int _la;
		try {
			setState(1837);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1766);
				qualifiedName();
				setState(1768);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,216,_ctx) ) {
				case 1:
					{
					setState(1767);
					queryPeriod();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new SubqueryRelationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1770);
				match(T__1);
				setState(1771);
				query();
				setState(1772);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new UnnestContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1774);
				match(UNNEST);
				setState(1775);
				match(T__1);
				setState(1776);
				expression();
				setState(1781);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1777);
					match(T__3);
					setState(1778);
					expression();
					}
					}
					setState(1783);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1784);
				match(T__2);
				setState(1787);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
				case 1:
					{
					setState(1785);
					match(WITH);
					setState(1786);
					match(ORDINALITY);
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new LateralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1789);
				match(LATERAL);
				setState(1790);
				match(T__1);
				setState(1791);
				query();
				setState(1792);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new TableFunctionInvocationContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1794);
				match(TABLE);
				setState(1795);
				match(T__1);
				setState(1796);
				tableFunctionCall();
				setState(1797);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new ParenthesizedRelationContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1799);
				match(T__1);
				setState(1800);
				relation(0);
				setState(1801);
				match(T__2);
				}
				break;
			case 7:
				_localctx = new JsonTableContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1803);
				match(JSON_TABLE);
				setState(1804);
				match(T__1);
				setState(1805);
				jsonPathInvocation();
				setState(1806);
				match(COLUMNS);
				setState(1807);
				match(T__1);
				setState(1808);
				jsonTableColumn();
				setState(1813);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1809);
					match(T__3);
					setState(1810);
					jsonTableColumn();
					}
					}
					setState(1815);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1816);
				match(T__2);
				setState(1828);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,220,_ctx) ) {
				case 1:
					{
					setState(1817);
					match(PLAN);
					setState(1818);
					match(T__1);
					setState(1819);
					jsonTableSpecificPlan();
					setState(1820);
					match(T__2);
					}
					break;
				case 2:
					{
					setState(1822);
					match(PLAN);
					setState(1823);
					match(DEFAULT);
					setState(1824);
					match(T__1);
					setState(1825);
					jsonTableDefaultPlan();
					setState(1826);
					match(T__2);
					}
					break;
				}
				setState(1833);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EMPTY || _la==ERROR) {
					{
					setState(1830);
					_la = _input.LA(1);
					if ( !(_la==EMPTY || _la==ERROR) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1831);
					match(ON);
					setState(1832);
					match(ERROR);
					}
				}

				setState(1835);
				match(T__2);
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
	public static class JsonTableColumnContext extends ParserRuleContext {
		public JsonTableColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonTableColumn; }
	 
		public JsonTableColumnContext() { }
		public void copyFrom(JsonTableColumnContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryColumnContext extends JsonTableColumnContext {
		public JsonQueryBehaviorContext emptyBehavior;
		public JsonQueryBehaviorContext errorBehavior;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public JsonQueryWrapperBehaviorContext jsonQueryWrapperBehavior() {
			return getRuleContext(JsonQueryWrapperBehaviorContext.class,0);
		}
		public TerminalNode WRAPPER() { return getToken(SqlBaseParser.WRAPPER, 0); }
		public TerminalNode QUOTES() { return getToken(SqlBaseParser.QUOTES, 0); }
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public List<JsonQueryBehaviorContext> jsonQueryBehavior() {
			return getRuleContexts(JsonQueryBehaviorContext.class);
		}
		public JsonQueryBehaviorContext jsonQueryBehavior(int i) {
			return getRuleContext(JsonQueryBehaviorContext.class,i);
		}
		public TerminalNode SCALAR() { return getToken(SqlBaseParser.SCALAR, 0); }
		public TerminalNode TEXT_STRING() { return getToken(SqlBaseParser.TEXT_STRING, 0); }
		public QueryColumnContext(JsonTableColumnContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NestedColumnsContext extends JsonTableColumnContext {
		public TerminalNode NESTED() { return getToken(SqlBaseParser.NESTED, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public List<JsonTableColumnContext> jsonTableColumn() {
			return getRuleContexts(JsonTableColumnContext.class);
		}
		public JsonTableColumnContext jsonTableColumn(int i) {
			return getRuleContext(JsonTableColumnContext.class,i);
		}
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public NestedColumnsContext(JsonTableColumnContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNestedColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNestedColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNestedColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueColumnContext extends JsonTableColumnContext {
		public JsonValueBehaviorContext emptyBehavior;
		public JsonValueBehaviorContext errorBehavior;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public List<JsonValueBehaviorContext> jsonValueBehavior() {
			return getRuleContexts(JsonValueBehaviorContext.class);
		}
		public JsonValueBehaviorContext jsonValueBehavior(int i) {
			return getRuleContext(JsonValueBehaviorContext.class,i);
		}
		public ValueColumnContext(JsonTableColumnContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OrdinalityColumnContext extends JsonTableColumnContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public OrdinalityColumnContext(JsonTableColumnContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrdinalityColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrdinalityColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrdinalityColumn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonTableColumnContext jsonTableColumn() throws RecognitionException {
		JsonTableColumnContext _localctx = new JsonTableColumnContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_jsonTableColumn);
		int _la;
		try {
			setState(1916);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
			case 1:
				_localctx = new OrdinalityColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1839);
				identifier();
				setState(1840);
				match(FOR);
				setState(1841);
				match(ORDINALITY);
				}
				break;
			case 2:
				_localctx = new ValueColumnContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1843);
				identifier();
				setState(1844);
				type(0);
				setState(1847);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PATH) {
					{
					setState(1845);
					match(PATH);
					setState(1846);
					string();
					}
				}

				setState(1853);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,224,_ctx) ) {
				case 1:
					{
					setState(1849);
					((ValueColumnContext)_localctx).emptyBehavior = jsonValueBehavior();
					setState(1850);
					match(ON);
					setState(1851);
					match(EMPTY);
					}
					break;
				}
				setState(1859);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DEFAULT || _la==ERROR || _la==NULL) {
					{
					setState(1855);
					((ValueColumnContext)_localctx).errorBehavior = jsonValueBehavior();
					setState(1856);
					match(ON);
					setState(1857);
					match(ERROR);
					}
				}

				}
				break;
			case 3:
				_localctx = new QueryColumnContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1861);
				identifier();
				setState(1862);
				type(0);
				setState(1863);
				match(FORMAT);
				setState(1864);
				jsonRepresentation();
				setState(1867);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PATH) {
					{
					setState(1865);
					match(PATH);
					setState(1866);
					string();
					}
				}

				setState(1872);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH || _la==WITHOUT) {
					{
					setState(1869);
					jsonQueryWrapperBehavior();
					setState(1870);
					match(WRAPPER);
					}
				}

				setState(1881);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==KEEP || _la==OMIT) {
					{
					setState(1874);
					_la = _input.LA(1);
					if ( !(_la==KEEP || _la==OMIT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1875);
					match(QUOTES);
					setState(1879);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ON) {
						{
						setState(1876);
						match(ON);
						setState(1877);
						match(SCALAR);
						setState(1878);
						match(TEXT_STRING);
						}
					}

					}
				}

				setState(1887);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,230,_ctx) ) {
				case 1:
					{
					setState(1883);
					((QueryColumnContext)_localctx).emptyBehavior = jsonQueryBehavior();
					setState(1884);
					match(ON);
					setState(1885);
					match(EMPTY);
					}
					break;
				}
				setState(1893);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EMPTY || _la==ERROR || _la==NULL) {
					{
					setState(1889);
					((QueryColumnContext)_localctx).errorBehavior = jsonQueryBehavior();
					setState(1890);
					match(ON);
					setState(1891);
					match(ERROR);
					}
				}

				}
				break;
			case 4:
				_localctx = new NestedColumnsContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1895);
				match(NESTED);
				setState(1897);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PATH) {
					{
					setState(1896);
					match(PATH);
					}
				}

				setState(1899);
				string();
				setState(1902);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1900);
					match(AS);
					setState(1901);
					identifier();
					}
				}

				setState(1904);
				match(COLUMNS);
				setState(1905);
				match(T__1);
				setState(1906);
				jsonTableColumn();
				setState(1911);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1907);
					match(T__3);
					setState(1908);
					jsonTableColumn();
					}
					}
					setState(1913);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1914);
				match(T__2);
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
	public static class JsonTableSpecificPlanContext extends ParserRuleContext {
		public JsonTableSpecificPlanContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonTableSpecificPlan; }
	 
		public JsonTableSpecificPlanContext() { }
		public void copyFrom(JsonTableSpecificPlanContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CrossPlanContext extends JsonTableSpecificPlanContext {
		public List<PlanPrimaryContext> planPrimary() {
			return getRuleContexts(PlanPrimaryContext.class);
		}
		public PlanPrimaryContext planPrimary(int i) {
			return getRuleContext(PlanPrimaryContext.class,i);
		}
		public List<TerminalNode> CROSS() { return getTokens(SqlBaseParser.CROSS); }
		public TerminalNode CROSS(int i) {
			return getToken(SqlBaseParser.CROSS, i);
		}
		public CrossPlanContext(JsonTableSpecificPlanContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCrossPlan(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCrossPlan(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCrossPlan(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JoinPlanContext extends JsonTableSpecificPlanContext {
		public JsonTablePathNameContext jsonTablePathName() {
			return getRuleContext(JsonTablePathNameContext.class,0);
		}
		public PlanPrimaryContext planPrimary() {
			return getRuleContext(PlanPrimaryContext.class,0);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public JoinPlanContext(JsonTableSpecificPlanContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinPlan(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinPlan(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinPlan(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LeafPlanContext extends JsonTableSpecificPlanContext {
		public JsonTablePathNameContext jsonTablePathName() {
			return getRuleContext(JsonTablePathNameContext.class,0);
		}
		public LeafPlanContext(JsonTableSpecificPlanContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLeafPlan(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLeafPlan(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLeafPlan(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnionPlanContext extends JsonTableSpecificPlanContext {
		public List<PlanPrimaryContext> planPrimary() {
			return getRuleContexts(PlanPrimaryContext.class);
		}
		public PlanPrimaryContext planPrimary(int i) {
			return getRuleContext(PlanPrimaryContext.class,i);
		}
		public List<TerminalNode> UNION() { return getTokens(SqlBaseParser.UNION); }
		public TerminalNode UNION(int i) {
			return getToken(SqlBaseParser.UNION, i);
		}
		public UnionPlanContext(JsonTableSpecificPlanContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnionPlan(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnionPlan(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnionPlan(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonTableSpecificPlanContext jsonTableSpecificPlan() throws RecognitionException {
		JsonTableSpecificPlanContext _localctx = new JsonTableSpecificPlanContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_jsonTableSpecificPlan);
		int _la;
		try {
			setState(1943);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
			case 1:
				_localctx = new LeafPlanContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1918);
				jsonTablePathName();
				}
				break;
			case 2:
				_localctx = new JoinPlanContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1919);
				jsonTablePathName();
				setState(1920);
				_la = _input.LA(1);
				if ( !(_la==INNER || _la==OUTER) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1921);
				planPrimary();
				}
				break;
			case 3:
				_localctx = new UnionPlanContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1923);
				planPrimary();
				setState(1924);
				match(UNION);
				setState(1925);
				planPrimary();
				setState(1930);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==UNION) {
					{
					{
					setState(1926);
					match(UNION);
					setState(1927);
					planPrimary();
					}
					}
					setState(1932);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 4:
				_localctx = new CrossPlanContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1933);
				planPrimary();
				setState(1934);
				match(CROSS);
				setState(1935);
				planPrimary();
				setState(1940);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==CROSS) {
					{
					{
					setState(1936);
					match(CROSS);
					setState(1937);
					planPrimary();
					}
					}
					setState(1942);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
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
	public static class JsonTablePathNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public JsonTablePathNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonTablePathName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonTablePathName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonTablePathName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonTablePathName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonTablePathNameContext jsonTablePathName() throws RecognitionException {
		JsonTablePathNameContext _localctx = new JsonTablePathNameContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_jsonTablePathName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1945);
			identifier();
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
	public static class PlanPrimaryContext extends ParserRuleContext {
		public JsonTablePathNameContext jsonTablePathName() {
			return getRuleContext(JsonTablePathNameContext.class,0);
		}
		public JsonTableSpecificPlanContext jsonTableSpecificPlan() {
			return getRuleContext(JsonTableSpecificPlanContext.class,0);
		}
		public PlanPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_planPrimary; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPlanPrimary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPlanPrimary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPlanPrimary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PlanPrimaryContext planPrimary() throws RecognitionException {
		PlanPrimaryContext _localctx = new PlanPrimaryContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_planPrimary);
		try {
			setState(1952);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1947);
				jsonTablePathName();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				setState(1948);
				match(T__1);
				setState(1949);
				jsonTableSpecificPlan();
				setState(1950);
				match(T__2);
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
	public static class JsonTableDefaultPlanContext extends ParserRuleContext {
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public JsonTableDefaultPlanContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonTableDefaultPlan; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonTableDefaultPlan(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonTableDefaultPlan(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonTableDefaultPlan(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonTableDefaultPlanContext jsonTableDefaultPlan() throws RecognitionException {
		JsonTableDefaultPlanContext _localctx = new JsonTableDefaultPlanContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_jsonTableDefaultPlan);
		int _la;
		try {
			setState(1964);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INNER:
			case OUTER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1954);
				_la = _input.LA(1);
				if ( !(_la==INNER || _la==OUTER) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1957);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(1955);
					match(T__3);
					setState(1956);
					_la = _input.LA(1);
					if ( !(_la==CROSS || _la==UNION) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case CROSS:
			case UNION:
				enterOuterAlt(_localctx, 2);
				{
				setState(1959);
				_la = _input.LA(1);
				if ( !(_la==CROSS || _la==UNION) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1962);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(1960);
					match(T__3);
					setState(1961);
					_la = _input.LA(1);
					if ( !(_la==INNER || _la==OUTER) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

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
	public static class TableFunctionCallContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<TableFunctionArgumentContext> tableFunctionArgument() {
			return getRuleContexts(TableFunctionArgumentContext.class);
		}
		public TableFunctionArgumentContext tableFunctionArgument(int i) {
			return getRuleContext(TableFunctionArgumentContext.class,i);
		}
		public TerminalNode COPARTITION() { return getToken(SqlBaseParser.COPARTITION, 0); }
		public List<CopartitionTablesContext> copartitionTables() {
			return getRuleContexts(CopartitionTablesContext.class);
		}
		public CopartitionTablesContext copartitionTables(int i) {
			return getRuleContext(CopartitionTablesContext.class,i);
		}
		public TableFunctionCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableFunctionCall; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableFunctionCallContext tableFunctionCall() throws RecognitionException {
		TableFunctionCallContext _localctx = new TableFunctionCallContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_tableFunctionCall);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1966);
			qualifiedName();
			setState(1967);
			match(T__1);
			setState(1976);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,244,_ctx) ) {
			case 1:
				{
				setState(1968);
				tableFunctionArgument();
				setState(1973);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1969);
					match(T__3);
					setState(1970);
					tableFunctionArgument();
					}
					}
					setState(1975);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(1987);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COPARTITION) {
				{
				setState(1978);
				match(COPARTITION);
				setState(1979);
				copartitionTables();
				setState(1984);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1980);
					match(T__3);
					setState(1981);
					copartitionTables();
					}
					}
					setState(1986);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1989);
			match(T__2);
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
	public static class TableFunctionArgumentContext extends ParserRuleContext {
		public TableArgumentContext tableArgument() {
			return getRuleContext(TableArgumentContext.class,0);
		}
		public DescriptorArgumentContext descriptorArgument() {
			return getRuleContext(DescriptorArgumentContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TableFunctionArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableFunctionArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFunctionArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFunctionArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFunctionArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableFunctionArgumentContext tableFunctionArgument() throws RecognitionException {
		TableFunctionArgumentContext _localctx = new TableFunctionArgumentContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_tableFunctionArgument);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1994);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
			case 1:
				{
				setState(1991);
				identifier();
				setState(1992);
				match(T__5);
				}
				break;
			}
			setState(1999);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,248,_ctx) ) {
			case 1:
				{
				setState(1996);
				tableArgument();
				}
				break;
			case 2:
				{
				setState(1997);
				descriptorArgument();
				}
				break;
			case 3:
				{
				setState(1998);
				expression();
				}
				break;
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
	public static class TableArgumentContext extends ParserRuleContext {
		public TableArgumentRelationContext tableArgumentRelation() {
			return getRuleContext(TableArgumentRelationContext.class,0);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode PRUNE() { return getToken(SqlBaseParser.PRUNE, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TableArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableArgumentContext tableArgument() throws RecognitionException {
		TableArgumentContext _localctx = new TableArgumentContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_tableArgument);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2001);
			tableArgumentRelation();
			setState(2019);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(2002);
				match(PARTITION);
				setState(2003);
				match(BY);
				setState(2017);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,251,_ctx) ) {
				case 1:
					{
					setState(2004);
					match(T__1);
					setState(2013);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
						{
						setState(2005);
						expression();
						setState(2010);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__3) {
							{
							{
							setState(2006);
							match(T__3);
							setState(2007);
							expression();
							}
							}
							setState(2012);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2015);
					match(T__2);
					}
					break;
				case 2:
					{
					setState(2016);
					expression();
					}
					break;
				}
				}
			}

			setState(2027);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PRUNE:
				{
				setState(2021);
				match(PRUNE);
				setState(2022);
				match(WHEN);
				setState(2023);
				match(EMPTY);
				}
				break;
			case KEEP:
				{
				setState(2024);
				match(KEEP);
				setState(2025);
				match(WHEN);
				setState(2026);
				match(EMPTY);
				}
				break;
			case T__2:
			case T__3:
			case COPARTITION:
			case ORDER:
				break;
			default:
				break;
			}
			setState(2045);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(2029);
				match(ORDER);
				setState(2030);
				match(BY);
				setState(2043);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,255,_ctx) ) {
				case 1:
					{
					setState(2031);
					match(T__1);
					setState(2032);
					sortItem();
					setState(2037);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2033);
						match(T__3);
						setState(2034);
						sortItem();
						}
						}
						setState(2039);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2040);
					match(T__2);
					}
					break;
				case 2:
					{
					setState(2042);
					sortItem();
					}
					break;
				}
				}
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
	public static class TableArgumentRelationContext extends ParserRuleContext {
		public TableArgumentRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableArgumentRelation; }
	 
		public TableArgumentRelationContext() { }
		public void copyFrom(TableArgumentRelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableArgumentQueryContext extends TableArgumentRelationContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TableArgumentQueryContext(TableArgumentRelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableArgumentQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableArgumentQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableArgumentQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableArgumentTableContext extends TableArgumentRelationContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public TableArgumentTableContext(TableArgumentRelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableArgumentTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableArgumentTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableArgumentTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableArgumentRelationContext tableArgumentRelation() throws RecognitionException {
		TableArgumentRelationContext _localctx = new TableArgumentRelationContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_tableArgumentRelation);
		int _la;
		try {
			setState(2073);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,263,_ctx) ) {
			case 1:
				_localctx = new TableArgumentTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2047);
				match(TABLE);
				setState(2048);
				match(T__1);
				setState(2049);
				qualifiedName();
				setState(2050);
				match(T__2);
				setState(2058);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
				case 1:
					{
					setState(2052);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(2051);
						match(AS);
						}
					}

					setState(2054);
					identifier();
					setState(2056);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__1) {
						{
						setState(2055);
						columnAliases();
						}
					}

					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new TableArgumentQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2060);
				match(TABLE);
				setState(2061);
				match(T__1);
				setState(2062);
				query();
				setState(2063);
				match(T__2);
				setState(2071);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,262,_ctx) ) {
				case 1:
					{
					setState(2065);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(2064);
						match(AS);
						}
					}

					setState(2067);
					identifier();
					setState(2069);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__1) {
						{
						setState(2068);
						columnAliases();
						}
					}

					}
					break;
				}
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
	public static class DescriptorArgumentContext extends ParserRuleContext {
		public TerminalNode DESCRIPTOR() { return getToken(SqlBaseParser.DESCRIPTOR, 0); }
		public List<DescriptorFieldContext> descriptorField() {
			return getRuleContexts(DescriptorFieldContext.class);
		}
		public DescriptorFieldContext descriptorField(int i) {
			return getRuleContext(DescriptorFieldContext.class,i);
		}
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DescriptorArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descriptorArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescriptorArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescriptorArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescriptorArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescriptorArgumentContext descriptorArgument() throws RecognitionException {
		DescriptorArgumentContext _localctx = new DescriptorArgumentContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_descriptorArgument);
		int _la;
		try {
			setState(2093);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DESCRIPTOR:
				enterOuterAlt(_localctx, 1);
				{
				setState(2075);
				match(DESCRIPTOR);
				setState(2076);
				match(T__1);
				setState(2077);
				descriptorField();
				setState(2082);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2078);
					match(T__3);
					setState(2079);
					descriptorField();
					}
					}
					setState(2084);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2085);
				match(T__2);
				}
				break;
			case CAST:
				enterOuterAlt(_localctx, 2);
				{
				setState(2087);
				match(CAST);
				setState(2088);
				match(T__1);
				setState(2089);
				match(NULL);
				setState(2090);
				match(AS);
				setState(2091);
				match(DESCRIPTOR);
				setState(2092);
				match(T__2);
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
	public static class DescriptorFieldContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public DescriptorFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descriptorField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescriptorField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescriptorField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescriptorField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescriptorFieldContext descriptorField() throws RecognitionException {
		DescriptorFieldContext _localctx = new DescriptorFieldContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_descriptorField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2095);
			identifier();
			setState(2097);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514849801377L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
				{
				setState(2096);
				type(0);
				}
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
	public static class CopartitionTablesContext extends ParserRuleContext {
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public CopartitionTablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_copartitionTables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCopartitionTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCopartitionTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCopartitionTables(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CopartitionTablesContext copartitionTables() throws RecognitionException {
		CopartitionTablesContext _localctx = new CopartitionTablesContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_copartitionTables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2099);
			match(T__1);
			setState(2100);
			qualifiedName();
			setState(2101);
			match(T__3);
			setState(2102);
			qualifiedName();
			setState(2107);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(2103);
				match(T__3);
				setState(2104);
				qualifiedName();
				}
				}
				setState(2109);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2110);
			match(T__2);
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
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2112);
			booleanExpression(0);
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
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OrContext extends BooleanExpressionContext {
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public OrContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOr(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AndContext extends BooleanExpressionContext {
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public AndContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnd(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnd(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2121);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CASE:
			case CAST:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case CURRENT_CATALOG:
			case CURRENT_DATE:
			case CURRENT_PATH:
			case CURRENT_SCHEMA:
			case CURRENT_TIME:
			case CURRENT_TIMESTAMP:
			case CURRENT_USER:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXISTS:
			case EXPLAIN:
			case EXTRACT:
			case FALSE:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPING:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case JSON_ARRAY:
			case JSON_EXISTS:
			case JSON_OBJECT:
			case JSON_QUERY:
			case JSON_VALUE:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LISTAGG:
			case LOCAL:
			case LOCALTIME:
			case LOCALTIMESTAMP:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NORMALIZE:
			case NULL:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRIM:
			case TRUE:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case PLUS:
			case MINUS:
			case QUESTION_MARK:
			case STRING:
			case UNICODE_STRING:
			case BINARY_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_VALUE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2115);
				((PredicatedContext)_localctx).valueExpression = valueExpression(0);
				setState(2117);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,268,_ctx) ) {
				case 1:
					{
					setState(2116);
					predicate(((PredicatedContext)_localctx).valueExpression);
					}
					break;
				}
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2119);
				match(NOT);
				setState(2120);
				booleanExpression(3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(2131);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,271,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2129);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,270,_ctx) ) {
					case 1:
						{
						_localctx = new AndContext(new BooleanExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2123);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2124);
						match(AND);
						setState(2125);
						booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new OrContext(new BooleanExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2126);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2127);
						match(OR);
						setState(2128);
						booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(2133);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,271,_ctx);
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
	public static class PredicateContext extends ParserRuleContext {
		public ParserRuleContext value;
		public PredicateContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
		public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
			super(parent, invokingState);
			this.value = value;
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
			this.value = ctx.value;
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends PredicateContext {
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LikeContext extends PredicateContext {
		public ValueExpressionContext pattern;
		public ValueExpressionContext escape;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public LikeContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLike(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InSubqueryContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InSubqueryContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DistinctFromContext extends PredicateContext {
		public ValueExpressionContext right;
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DistinctFromContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDistinctFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDistinctFrom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDistinctFrom(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InListContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InListContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInList(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullPredicateContext extends PredicateContext {
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BetweenContext extends PredicateContext {
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BetweenContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBetween(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBetween(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuantifiedComparisonContext extends PredicateContext {
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ComparisonQuantifierContext comparisonQuantifier() {
			return getRuleContext(ComparisonQuantifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QuantifiedComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuantifiedComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuantifiedComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuantifiedComparison(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
		enterRule(_localctx, 130, RULE_predicate);
		int _la;
		try {
			setState(2195);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,280,_ctx) ) {
			case 1:
				_localctx = new ComparisonContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2134);
				comparisonOperator();
				setState(2135);
				((ComparisonContext)_localctx).right = valueExpression(0);
				}
				break;
			case 2:
				_localctx = new QuantifiedComparisonContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2137);
				comparisonOperator();
				setState(2138);
				comparisonQuantifier();
				setState(2139);
				match(T__1);
				setState(2140);
				query();
				setState(2141);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new BetweenContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2144);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2143);
					match(NOT);
					}
				}

				setState(2146);
				match(BETWEEN);
				setState(2147);
				((BetweenContext)_localctx).lower = valueExpression(0);
				setState(2148);
				match(AND);
				setState(2149);
				((BetweenContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 4:
				_localctx = new InListContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2152);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2151);
					match(NOT);
					}
				}

				setState(2154);
				match(IN);
				setState(2155);
				match(T__1);
				setState(2156);
				expression();
				setState(2161);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2157);
					match(T__3);
					setState(2158);
					expression();
					}
					}
					setState(2163);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2164);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new InSubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2167);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2166);
					match(NOT);
					}
				}

				setState(2169);
				match(IN);
				setState(2170);
				match(T__1);
				setState(2171);
				query();
				setState(2172);
				match(T__2);
				}
				break;
			case 6:
				_localctx = new LikeContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2175);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2174);
					match(NOT);
					}
				}

				setState(2177);
				match(LIKE);
				setState(2178);
				((LikeContext)_localctx).pattern = valueExpression(0);
				setState(2181);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,277,_ctx) ) {
				case 1:
					{
					setState(2179);
					match(ESCAPE);
					setState(2180);
					((LikeContext)_localctx).escape = valueExpression(0);
					}
					break;
				}
				}
				break;
			case 7:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2183);
				match(IS);
				setState(2185);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2184);
					match(NOT);
					}
				}

				setState(2187);
				match(NULL);
				}
				break;
			case 8:
				_localctx = new DistinctFromContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(2188);
				match(IS);
				setState(2190);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2189);
					match(NOT);
					}
				}

				setState(2192);
				match(DISTINCT);
				setState(2193);
				match(FROM);
				setState(2194);
				((DistinctFromContext)_localctx).right = valueExpression(0);
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
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConcatenationContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public TerminalNode CONCAT() { return getToken(SqlBaseParser.CONCAT, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ConcatenationContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AtTimeZoneContext extends ValueExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TimeZoneSpecifierContext timeZoneSpecifier() {
			return getRuleContext(TimeZoneSpecifierContext.class,0);
		}
		public AtTimeZoneContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAtTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAtTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAtTimeZone(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 132;
		enterRecursionRule(_localctx, 132, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2201);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,281,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2198);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2199);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2200);
				valueExpression(4);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2217);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,283,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2215);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,282,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2203);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2204);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 320)) & ~0x3f) == 0 && ((1L << (_la - 320)) & 7L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2205);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2206);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2207);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2208);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(new ValueExpressionContext(_parentctx, _parentState));
						((ConcatenationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2209);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2210);
						match(CONCAT);
						setState(2211);
						((ConcatenationContext)_localctx).right = valueExpression(2);
						}
						break;
					case 4:
						{
						_localctx = new AtTimeZoneContext(new ValueExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2212);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2213);
						match(AT);
						setState(2214);
						timeZoneSpecifier();
						}
						break;
					}
					} 
				}
				setState(2219);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,283,_ctx);
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
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeConstructorContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode DOUBLE() { return getToken(SqlBaseParser.DOUBLE, 0); }
		public TerminalNode PRECISION() { return getToken(SqlBaseParser.PRECISION, 0); }
		public TypeConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonValueContext extends PrimaryExpressionContext {
		public JsonValueBehaviorContext emptyBehavior;
		public JsonValueBehaviorContext errorBehavior;
		public TerminalNode JSON_VALUE() { return getToken(SqlBaseParser.JSON_VALUE, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public List<JsonValueBehaviorContext> jsonValueBehavior() {
			return getRuleContexts(JsonValueBehaviorContext.class);
		}
		public JsonValueBehaviorContext jsonValueBehavior(int i) {
			return getRuleContext(JsonValueBehaviorContext.class,i);
		}
		public JsonValueContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonValue(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentDateContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public CurrentDateContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentDate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentDate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentDate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubstringContext extends PrimaryExpressionContext {
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubstring(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubstring(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrimContext extends PrimaryExpressionContext {
		public ValueExpressionContext trimChar;
		public ValueExpressionContext trimSource;
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TrimsSpecificationContext trimsSpecification() {
			return getRuleContext(TrimsSpecificationContext.class,0);
		}
		public TrimContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTrim(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTrim(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTrim(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParameterContext extends PrimaryExpressionContext {
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public ParameterContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParameter(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NormalizeContext extends PrimaryExpressionContext {
		public TerminalNode NORMALIZE() { return getToken(SqlBaseParser.NORMALIZE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public NormalFormContext normalForm() {
			return getRuleContext(NormalFormContext.class,0);
		}
		public NormalizeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalize(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalize(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LocalTimestampContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode LOCALTIMESTAMP() { return getToken(SqlBaseParser.LOCALTIMESTAMP, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public LocalTimestampContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLocalTimestamp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLocalTimestamp(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLocalTimestamp(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonObjectContext extends PrimaryExpressionContext {
		public TerminalNode JSON_OBJECT() { return getToken(SqlBaseParser.JSON_OBJECT, 0); }
		public List<JsonObjectMemberContext> jsonObjectMember() {
			return getRuleContexts(JsonObjectMemberContext.class);
		}
		public JsonObjectMemberContext jsonObjectMember(int i) {
			return getRuleContext(JsonObjectMemberContext.class,i);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<TerminalNode> NULL() { return getTokens(SqlBaseParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(SqlBaseParser.NULL, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode ABSENT() { return getToken(SqlBaseParser.ABSENT, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public JsonObjectContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonObject(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonObject(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalLiteralContext extends PrimaryExpressionContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends PrimaryExpressionContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends PrimaryExpressionContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonArrayContext extends PrimaryExpressionContext {
		public TerminalNode JSON_ARRAY() { return getToken(SqlBaseParser.JSON_ARRAY, 0); }
		public List<JsonValueExpressionContext> jsonValueExpression() {
			return getRuleContexts(JsonValueExpressionContext.class);
		}
		public JsonValueExpressionContext jsonValueExpression(int i) {
			return getRuleContext(JsonValueExpressionContext.class,i);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public List<TerminalNode> NULL() { return getTokens(SqlBaseParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(SqlBaseParser.NULL, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode ABSENT() { return getToken(SqlBaseParser.ABSENT, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public JsonArrayContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonArray(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonArray(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext operand;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends PrimaryExpressionContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonExistsContext extends PrimaryExpressionContext {
		public TerminalNode JSON_EXISTS() { return getToken(SqlBaseParser.JSON_EXISTS, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public JsonExistsErrorBehaviorContext jsonExistsErrorBehavior() {
			return getRuleContext(JsonExistsErrorBehaviorContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public JsonExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentPathContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_PATH() { return getToken(SqlBaseParser.CURRENT_PATH, 0); }
		public CurrentPathContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentPath(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryLiteralContext extends PrimaryExpressionContext {
		public TerminalNode BINARY_LITERAL() { return getToken(SqlBaseParser.BINARY_LITERAL, 0); }
		public BinaryLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBinaryLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBinaryLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBinaryLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentTimeContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public CurrentTimeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentTime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentTime(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentTime(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LocalTimeContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode LOCALTIME() { return getToken(SqlBaseParser.LOCALTIME, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public LocalTimeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLocalTime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLocalTime(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLocalTime(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentUserContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentUser(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentUser(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonQueryContext extends PrimaryExpressionContext {
		public JsonQueryBehaviorContext emptyBehavior;
		public JsonQueryBehaviorContext errorBehavior;
		public TerminalNode JSON_QUERY() { return getToken(SqlBaseParser.JSON_QUERY, 0); }
		public JsonPathInvocationContext jsonPathInvocation() {
			return getRuleContext(JsonPathInvocationContext.class,0);
		}
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public JsonQueryWrapperBehaviorContext jsonQueryWrapperBehavior() {
			return getRuleContext(JsonQueryWrapperBehaviorContext.class,0);
		}
		public TerminalNode WRAPPER() { return getToken(SqlBaseParser.WRAPPER, 0); }
		public TerminalNode QUOTES() { return getToken(SqlBaseParser.QUOTES, 0); }
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public List<JsonQueryBehaviorContext> jsonQueryBehavior() {
			return getRuleContexts(JsonQueryBehaviorContext.class);
		}
		public JsonQueryBehaviorContext jsonQueryBehavior(int i) {
			return getRuleContext(JsonQueryBehaviorContext.class,i);
		}
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public TerminalNode SCALAR() { return getToken(SqlBaseParser.SCALAR, 0); }
		public TerminalNode TEXT_STRING() { return getToken(SqlBaseParser.TEXT_STRING, 0); }
		public JsonQueryContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MeasureContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public OverContext over() {
			return getRuleContext(OverContext.class,0);
		}
		public MeasureContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMeasure(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMeasure(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMeasure(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExtractContext extends PrimaryExpressionContext {
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends PrimaryExpressionContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayConstructorContext extends PrimaryExpressionContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ArrayConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArrayConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArrayConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArrayConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public IdentifierContext label;
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public ProcessingModeContext processingMode() {
			return getRuleContext(ProcessingModeContext.class,0);
		}
		public FilterContext filter() {
			return getRuleContext(FilterContext.class,0);
		}
		public OverContext over() {
			return getRuleContext(OverContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public NullTreatmentContext nullTreatment() {
			return getRuleContext(NullTreatmentContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentTimestampContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public CurrentTimestampContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentTimestamp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentTimestamp(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentTimestamp(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentSchemaContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_SCHEMA() { return getToken(SqlBaseParser.CURRENT_SCHEMA, 0); }
		public CurrentSchemaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentSchema(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends PrimaryExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionContext extends PrimaryExpressionContext {
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ListaggContext extends PrimaryExpressionContext {
		public Token name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode LISTAGG() { return getToken(SqlBaseParser.LISTAGG, 0); }
		public TerminalNode WITHIN() { return getToken(SqlBaseParser.WITHIN, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode OVERFLOW() { return getToken(SqlBaseParser.OVERFLOW, 0); }
		public ListAggOverflowBehaviorContext listAggOverflowBehavior() {
			return getRuleContext(ListAggOverflowBehaviorContext.class,0);
		}
		public FilterContext filter() {
			return getRuleContext(FilterContext.class,0);
		}
		public ListaggContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterListagg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitListagg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitListagg(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentCatalogContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_CATALOG() { return getToken(SqlBaseParser.CURRENT_CATALOG, 0); }
		public CurrentCatalogContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentCatalog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentCatalog(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentCatalog(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GroupingOperationContext extends PrimaryExpressionContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public GroupingOperationContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 134;
		enterRecursionRule(_localctx, 134, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2673);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,347,_ctx) ) {
			case 1:
				{
				_localctx = new NullLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2221);
				match(NULL);
				}
				break;
			case 2:
				{
				_localctx = new IntervalLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2222);
				interval();
				}
				break;
			case 3:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2223);
				identifier();
				setState(2224);
				string();
				}
				break;
			case 4:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2226);
				match(DOUBLE);
				setState(2227);
				match(PRECISION);
				setState(2228);
				string();
				}
				break;
			case 5:
				{
				_localctx = new NumericLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2229);
				number();
				}
				break;
			case 6:
				{
				_localctx = new BooleanLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2230);
				booleanValue();
				}
				break;
			case 7:
				{
				_localctx = new StringLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2231);
				string();
				}
				break;
			case 8:
				{
				_localctx = new BinaryLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2232);
				match(BINARY_LITERAL);
				}
				break;
			case 9:
				{
				_localctx = new ParameterContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2233);
				match(QUESTION_MARK);
				}
				break;
			case 10:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2234);
				match(POSITION);
				setState(2235);
				match(T__1);
				setState(2236);
				valueExpression(0);
				setState(2237);
				match(IN);
				setState(2238);
				valueExpression(0);
				setState(2239);
				match(T__2);
				}
				break;
			case 11:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2241);
				match(T__1);
				setState(2242);
				expression();
				setState(2245); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2243);
					match(T__3);
					setState(2244);
					expression();
					}
					}
					setState(2247); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__3 );
				setState(2249);
				match(T__2);
				}
				break;
			case 12:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2251);
				match(ROW);
				setState(2252);
				match(T__1);
				setState(2253);
				expression();
				setState(2258);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2254);
					match(T__3);
					setState(2255);
					expression();
					}
					}
					setState(2260);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2261);
				match(T__2);
				}
				break;
			case 13:
				{
				_localctx = new ListaggContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2263);
				((ListaggContext)_localctx).name = match(LISTAGG);
				setState(2264);
				match(T__1);
				setState(2266);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,286,_ctx) ) {
				case 1:
					{
					setState(2265);
					setQuantifier();
					}
					break;
				}
				setState(2268);
				expression();
				setState(2271);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(2269);
					match(T__3);
					setState(2270);
					string();
					}
				}

				setState(2276);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(2273);
					match(ON);
					setState(2274);
					match(OVERFLOW);
					setState(2275);
					listAggOverflowBehavior();
					}
				}

				setState(2278);
				match(T__2);
				{
				setState(2279);
				match(WITHIN);
				setState(2280);
				match(GROUP);
				setState(2281);
				match(T__1);
				setState(2282);
				match(ORDER);
				setState(2283);
				match(BY);
				setState(2284);
				sortItem();
				setState(2289);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2285);
					match(T__3);
					setState(2286);
					sortItem();
					}
					}
					setState(2291);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2292);
				match(T__2);
				}
				setState(2295);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,290,_ctx) ) {
				case 1:
					{
					setState(2294);
					filter();
					}
					break;
				}
				}
				break;
			case 14:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2298);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,291,_ctx) ) {
				case 1:
					{
					setState(2297);
					processingMode();
					}
					break;
				}
				setState(2300);
				qualifiedName();
				setState(2301);
				match(T__1);
				setState(2305);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514849801377L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
					{
					setState(2302);
					((FunctionCallContext)_localctx).label = identifier();
					setState(2303);
					match(T__0);
					}
				}

				setState(2307);
				match(ASTERISK);
				setState(2308);
				match(T__2);
				setState(2310);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,293,_ctx) ) {
				case 1:
					{
					setState(2309);
					filter();
					}
					break;
				}
				setState(2313);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,294,_ctx) ) {
				case 1:
					{
					setState(2312);
					over();
					}
					break;
				}
				}
				break;
			case 15:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2316);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,295,_ctx) ) {
				case 1:
					{
					setState(2315);
					processingMode();
					}
					break;
				}
				setState(2318);
				qualifiedName();
				setState(2319);
				match(T__1);
				setState(2331);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745653743L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
					{
					setState(2321);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,296,_ctx) ) {
					case 1:
						{
						setState(2320);
						setQuantifier();
						}
						break;
					}
					setState(2323);
					expression();
					setState(2328);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2324);
						match(T__3);
						setState(2325);
						expression();
						}
						}
						setState(2330);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2343);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ORDER) {
					{
					setState(2333);
					match(ORDER);
					setState(2334);
					match(BY);
					setState(2335);
					sortItem();
					setState(2340);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2336);
						match(T__3);
						setState(2337);
						sortItem();
						}
						}
						setState(2342);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2345);
				match(T__2);
				setState(2347);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,301,_ctx) ) {
				case 1:
					{
					setState(2346);
					filter();
					}
					break;
				}
				setState(2353);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,303,_ctx) ) {
				case 1:
					{
					setState(2350);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IGNORE || _la==RESPECT) {
						{
						setState(2349);
						nullTreatment();
						}
					}

					setState(2352);
					over();
					}
					break;
				}
				}
				break;
			case 16:
				{
				_localctx = new MeasureContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2355);
				identifier();
				setState(2356);
				over();
				}
				break;
			case 17:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2358);
				identifier();
				setState(2359);
				match(T__6);
				setState(2360);
				expression();
				}
				break;
			case 18:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2362);
				match(T__1);
				setState(2371);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514849801377L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
					{
					setState(2363);
					identifier();
					setState(2368);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2364);
						match(T__3);
						setState(2365);
						identifier();
						}
						}
						setState(2370);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2373);
				match(T__2);
				setState(2374);
				match(T__6);
				setState(2375);
				expression();
				}
				break;
			case 19:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2376);
				match(T__1);
				setState(2377);
				query();
				setState(2378);
				match(T__2);
				}
				break;
			case 20:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2380);
				match(EXISTS);
				setState(2381);
				match(T__1);
				setState(2382);
				query();
				setState(2383);
				match(T__2);
				}
				break;
			case 21:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2385);
				match(CASE);
				setState(2386);
				((SimpleCaseContext)_localctx).operand = expression();
				setState(2388); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2387);
					whenClause();
					}
					}
					setState(2390); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2394);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2392);
					match(ELSE);
					setState(2393);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2396);
				match(END);
				}
				break;
			case 22:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2398);
				match(CASE);
				setState(2400); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2399);
					whenClause();
					}
					}
					setState(2402); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2406);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2404);
					match(ELSE);
					setState(2405);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2408);
				match(END);
				}
				break;
			case 23:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2410);
				match(CAST);
				setState(2411);
				match(T__1);
				setState(2412);
				expression();
				setState(2413);
				match(AS);
				setState(2414);
				type(0);
				setState(2415);
				match(T__2);
				}
				break;
			case 24:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2417);
				match(TRY_CAST);
				setState(2418);
				match(T__1);
				setState(2419);
				expression();
				setState(2420);
				match(AS);
				setState(2421);
				type(0);
				setState(2422);
				match(T__2);
				}
				break;
			case 25:
				{
				_localctx = new ArrayConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2424);
				match(ARRAY);
				setState(2425);
				match(T__7);
				setState(2434);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2305851805348667539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
					{
					setState(2426);
					expression();
					setState(2431);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2427);
						match(T__3);
						setState(2428);
						expression();
						}
						}
						setState(2433);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2436);
				match(T__8);
				}
				break;
			case 26:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2437);
				identifier();
				}
				break;
			case 27:
				{
				_localctx = new CurrentDateContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2438);
				((CurrentDateContext)_localctx).name = match(CURRENT_DATE);
				}
				break;
			case 28:
				{
				_localctx = new CurrentTimeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2439);
				((CurrentTimeContext)_localctx).name = match(CURRENT_TIME);
				setState(2443);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,312,_ctx) ) {
				case 1:
					{
					setState(2440);
					match(T__1);
					setState(2441);
					((CurrentTimeContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2442);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 29:
				{
				_localctx = new CurrentTimestampContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2445);
				((CurrentTimestampContext)_localctx).name = match(CURRENT_TIMESTAMP);
				setState(2449);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,313,_ctx) ) {
				case 1:
					{
					setState(2446);
					match(T__1);
					setState(2447);
					((CurrentTimestampContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2448);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 30:
				{
				_localctx = new LocalTimeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2451);
				((LocalTimeContext)_localctx).name = match(LOCALTIME);
				setState(2455);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,314,_ctx) ) {
				case 1:
					{
					setState(2452);
					match(T__1);
					setState(2453);
					((LocalTimeContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2454);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 31:
				{
				_localctx = new LocalTimestampContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2457);
				((LocalTimestampContext)_localctx).name = match(LOCALTIMESTAMP);
				setState(2461);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,315,_ctx) ) {
				case 1:
					{
					setState(2458);
					match(T__1);
					setState(2459);
					((LocalTimestampContext)_localctx).precision = match(INTEGER_VALUE);
					setState(2460);
					match(T__2);
					}
					break;
				}
				}
				break;
			case 32:
				{
				_localctx = new CurrentUserContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2463);
				((CurrentUserContext)_localctx).name = match(CURRENT_USER);
				}
				break;
			case 33:
				{
				_localctx = new CurrentCatalogContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2464);
				((CurrentCatalogContext)_localctx).name = match(CURRENT_CATALOG);
				}
				break;
			case 34:
				{
				_localctx = new CurrentSchemaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2465);
				((CurrentSchemaContext)_localctx).name = match(CURRENT_SCHEMA);
				}
				break;
			case 35:
				{
				_localctx = new CurrentPathContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2466);
				((CurrentPathContext)_localctx).name = match(CURRENT_PATH);
				}
				break;
			case 36:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2467);
				match(TRIM);
				setState(2468);
				match(T__1);
				setState(2476);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,318,_ctx) ) {
				case 1:
					{
					setState(2470);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,316,_ctx) ) {
					case 1:
						{
						setState(2469);
						trimsSpecification();
						}
						break;
					}
					setState(2473);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1217097885506863100L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 6619158404745620975L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2314859004603408531L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 6953390140260613879L) != 0) || ((((_la - 324)) & ~0x3f) == 0 && ((1L << (_la - 324)) & 4093L) != 0)) {
						{
						setState(2472);
						((TrimContext)_localctx).trimChar = valueExpression(0);
						}
					}

					setState(2475);
					match(FROM);
					}
					break;
				}
				setState(2478);
				((TrimContext)_localctx).trimSource = valueExpression(0);
				setState(2479);
				match(T__2);
				}
				break;
			case 37:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2481);
				match(TRIM);
				setState(2482);
				match(T__1);
				setState(2483);
				((TrimContext)_localctx).trimSource = valueExpression(0);
				setState(2484);
				match(T__3);
				setState(2485);
				((TrimContext)_localctx).trimChar = valueExpression(0);
				setState(2486);
				match(T__2);
				}
				break;
			case 38:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2488);
				match(SUBSTRING);
				setState(2489);
				match(T__1);
				setState(2490);
				valueExpression(0);
				setState(2491);
				match(FROM);
				setState(2492);
				valueExpression(0);
				setState(2495);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2493);
					match(FOR);
					setState(2494);
					valueExpression(0);
					}
				}

				setState(2497);
				match(T__2);
				}
				break;
			case 39:
				{
				_localctx = new NormalizeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2499);
				match(NORMALIZE);
				setState(2500);
				match(T__1);
				setState(2501);
				valueExpression(0);
				setState(2504);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(2502);
					match(T__3);
					setState(2503);
					normalForm();
					}
				}

				setState(2506);
				match(T__2);
				}
				break;
			case 40:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2508);
				match(EXTRACT);
				setState(2509);
				match(T__1);
				setState(2510);
				identifier();
				setState(2511);
				match(FROM);
				setState(2512);
				valueExpression(0);
				setState(2513);
				match(T__2);
				}
				break;
			case 41:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2515);
				match(T__1);
				setState(2516);
				expression();
				setState(2517);
				match(T__2);
				}
				break;
			case 42:
				{
				_localctx = new GroupingOperationContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2519);
				match(GROUPING);
				setState(2520);
				match(T__1);
				setState(2529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514849801377L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
					{
					setState(2521);
					qualifiedName();
					setState(2526);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2522);
						match(T__3);
						setState(2523);
						qualifiedName();
						}
						}
						setState(2528);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2531);
				match(T__2);
				}
				break;
			case 43:
				{
				_localctx = new JsonExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2532);
				match(JSON_EXISTS);
				setState(2533);
				match(T__1);
				setState(2534);
				jsonPathInvocation();
				setState(2539);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ERROR || _la==FALSE || _la==TRUE || _la==UNKNOWN) {
					{
					setState(2535);
					jsonExistsErrorBehavior();
					setState(2536);
					match(ON);
					setState(2537);
					match(ERROR);
					}
				}

				setState(2541);
				match(T__2);
				}
				break;
			case 44:
				{
				_localctx = new JsonValueContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2543);
				match(JSON_VALUE);
				setState(2544);
				match(T__1);
				setState(2545);
				jsonPathInvocation();
				setState(2548);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2546);
					match(RETURNING);
					setState(2547);
					type(0);
					}
				}

				setState(2554);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,325,_ctx) ) {
				case 1:
					{
					setState(2550);
					((JsonValueContext)_localctx).emptyBehavior = jsonValueBehavior();
					setState(2551);
					match(ON);
					setState(2552);
					match(EMPTY);
					}
					break;
				}
				setState(2560);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DEFAULT || _la==ERROR || _la==NULL) {
					{
					setState(2556);
					((JsonValueContext)_localctx).errorBehavior = jsonValueBehavior();
					setState(2557);
					match(ON);
					setState(2558);
					match(ERROR);
					}
				}

				setState(2562);
				match(T__2);
				}
				break;
			case 45:
				{
				_localctx = new JsonQueryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2564);
				match(JSON_QUERY);
				setState(2565);
				match(T__1);
				setState(2566);
				jsonPathInvocation();
				setState(2573);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2567);
					match(RETURNING);
					setState(2568);
					type(0);
					setState(2571);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==FORMAT) {
						{
						setState(2569);
						match(FORMAT);
						setState(2570);
						jsonRepresentation();
						}
					}

					}
				}

				setState(2578);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH || _la==WITHOUT) {
					{
					setState(2575);
					jsonQueryWrapperBehavior();
					setState(2576);
					match(WRAPPER);
					}
				}

				setState(2587);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==KEEP || _la==OMIT) {
					{
					setState(2580);
					_la = _input.LA(1);
					if ( !(_la==KEEP || _la==OMIT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(2581);
					match(QUOTES);
					setState(2585);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ON) {
						{
						setState(2582);
						match(ON);
						setState(2583);
						match(SCALAR);
						setState(2584);
						match(TEXT_STRING);
						}
					}

					}
				}

				setState(2593);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,332,_ctx) ) {
				case 1:
					{
					setState(2589);
					((JsonQueryContext)_localctx).emptyBehavior = jsonQueryBehavior();
					setState(2590);
					match(ON);
					setState(2591);
					match(EMPTY);
					}
					break;
				}
				setState(2599);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EMPTY || _la==ERROR || _la==NULL) {
					{
					setState(2595);
					((JsonQueryContext)_localctx).errorBehavior = jsonQueryBehavior();
					setState(2596);
					match(ON);
					setState(2597);
					match(ERROR);
					}
				}

				setState(2601);
				match(T__2);
				}
				break;
			case 46:
				{
				_localctx = new JsonObjectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2603);
				match(JSON_OBJECT);
				setState(2604);
				match(T__1);
				setState(2633);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,339,_ctx) ) {
				case 1:
					{
					setState(2605);
					jsonObjectMember();
					setState(2610);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2606);
						match(T__3);
						setState(2607);
						jsonObjectMember();
						}
						}
						setState(2612);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2619);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case NULL:
						{
						setState(2613);
						match(NULL);
						setState(2614);
						match(ON);
						setState(2615);
						match(NULL);
						}
						break;
					case ABSENT:
						{
						setState(2616);
						match(ABSENT);
						setState(2617);
						match(ON);
						setState(2618);
						match(NULL);
						}
						break;
					case T__2:
					case RETURNING:
					case WITH:
					case WITHOUT:
						break;
					default:
						break;
					}
					setState(2631);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case WITH:
						{
						setState(2621);
						match(WITH);
						setState(2622);
						match(UNIQUE);
						setState(2624);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==KEYS) {
							{
							setState(2623);
							match(KEYS);
							}
						}

						}
						break;
					case WITHOUT:
						{
						setState(2626);
						match(WITHOUT);
						setState(2627);
						match(UNIQUE);
						setState(2629);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==KEYS) {
							{
							setState(2628);
							match(KEYS);
							}
						}

						}
						break;
					case T__2:
					case RETURNING:
						break;
					default:
						break;
					}
					}
					break;
				}
				setState(2641);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2635);
					match(RETURNING);
					setState(2636);
					type(0);
					setState(2639);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==FORMAT) {
						{
						setState(2637);
						match(FORMAT);
						setState(2638);
						jsonRepresentation();
						}
					}

					}
				}

				setState(2643);
				match(T__2);
				}
				break;
			case 47:
				{
				_localctx = new JsonArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2644);
				match(JSON_ARRAY);
				setState(2645);
				match(T__1);
				setState(2662);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,344,_ctx) ) {
				case 1:
					{
					setState(2646);
					jsonValueExpression();
					setState(2651);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2647);
						match(T__3);
						setState(2648);
						jsonValueExpression();
						}
						}
						setState(2653);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2660);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case NULL:
						{
						setState(2654);
						match(NULL);
						setState(2655);
						match(ON);
						setState(2656);
						match(NULL);
						}
						break;
					case ABSENT:
						{
						setState(2657);
						match(ABSENT);
						setState(2658);
						match(ON);
						setState(2659);
						match(NULL);
						}
						break;
					case T__2:
					case RETURNING:
						break;
					default:
						break;
					}
					}
					break;
				}
				setState(2670);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETURNING) {
					{
					setState(2664);
					match(RETURNING);
					setState(2665);
					type(0);
					setState(2668);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==FORMAT) {
						{
						setState(2666);
						match(FORMAT);
						setState(2667);
						jsonRepresentation();
						}
					}

					}
				}

				setState(2672);
				match(T__2);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2685);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,349,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2683);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,348,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2675);
						if (!(precpred(_ctx, 24))) throw new FailedPredicateException(this, "precpred(_ctx, 24)");
						setState(2676);
						match(T__7);
						setState(2677);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2678);
						match(T__8);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2680);
						if (!(precpred(_ctx, 22))) throw new FailedPredicateException(this, "precpred(_ctx, 22)");
						setState(2681);
						match(T__0);
						setState(2682);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2687);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,349,_ctx);
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
	public static class JsonPathInvocationContext extends ParserRuleContext {
		public StringContext path;
		public IdentifierContext pathName;
		public JsonValueExpressionContext jsonValueExpression() {
			return getRuleContext(JsonValueExpressionContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode PASSING() { return getToken(SqlBaseParser.PASSING, 0); }
		public List<JsonArgumentContext> jsonArgument() {
			return getRuleContexts(JsonArgumentContext.class);
		}
		public JsonArgumentContext jsonArgument(int i) {
			return getRuleContext(JsonArgumentContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public JsonPathInvocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonPathInvocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonPathInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonPathInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonPathInvocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonPathInvocationContext jsonPathInvocation() throws RecognitionException {
		JsonPathInvocationContext _localctx = new JsonPathInvocationContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_jsonPathInvocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2688);
			jsonValueExpression();
			setState(2689);
			match(T__3);
			setState(2690);
			((JsonPathInvocationContext)_localctx).path = string();
			setState(2693);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(2691);
				match(AS);
				setState(2692);
				((JsonPathInvocationContext)_localctx).pathName = identifier();
				}
			}

			setState(2704);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PASSING) {
				{
				setState(2695);
				match(PASSING);
				setState(2696);
				jsonArgument();
				setState(2701);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2697);
					match(T__3);
					setState(2698);
					jsonArgument();
					}
					}
					setState(2703);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
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
	public static class JsonValueExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public JsonRepresentationContext jsonRepresentation() {
			return getRuleContext(JsonRepresentationContext.class,0);
		}
		public JsonValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonValueExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonValueExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonValueExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonValueExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonValueExpressionContext jsonValueExpression() throws RecognitionException {
		JsonValueExpressionContext _localctx = new JsonValueExpressionContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_jsonValueExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2706);
			expression();
			setState(2709);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FORMAT) {
				{
				setState(2707);
				match(FORMAT);
				setState(2708);
				jsonRepresentation();
				}
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
	public static class JsonRepresentationContext extends ParserRuleContext {
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public TerminalNode ENCODING() { return getToken(SqlBaseParser.ENCODING, 0); }
		public TerminalNode UTF8() { return getToken(SqlBaseParser.UTF8, 0); }
		public TerminalNode UTF16() { return getToken(SqlBaseParser.UTF16, 0); }
		public TerminalNode UTF32() { return getToken(SqlBaseParser.UTF32, 0); }
		public JsonRepresentationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonRepresentation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonRepresentation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonRepresentation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonRepresentation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonRepresentationContext jsonRepresentation() throws RecognitionException {
		JsonRepresentationContext _localctx = new JsonRepresentationContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_jsonRepresentation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2711);
			match(JSON);
			setState(2714);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ENCODING) {
				{
				setState(2712);
				match(ENCODING);
				setState(2713);
				_la = _input.LA(1);
				if ( !(((((_la - 291)) & ~0x3f) == 0 && ((1L << (_la - 291)) & 7L) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
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
	public static class JsonArgumentContext extends ParserRuleContext {
		public JsonValueExpressionContext jsonValueExpression() {
			return getRuleContext(JsonValueExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public JsonArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonArgumentContext jsonArgument() throws RecognitionException {
		JsonArgumentContext _localctx = new JsonArgumentContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_jsonArgument);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2716);
			jsonValueExpression();
			setState(2717);
			match(AS);
			setState(2718);
			identifier();
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
	public static class JsonExistsErrorBehaviorContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public JsonExistsErrorBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonExistsErrorBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonExistsErrorBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonExistsErrorBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonExistsErrorBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonExistsErrorBehaviorContext jsonExistsErrorBehavior() throws RecognitionException {
		JsonExistsErrorBehaviorContext _localctx = new JsonExistsErrorBehaviorContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_jsonExistsErrorBehavior);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2720);
			_la = _input.LA(1);
			if ( !(_la==ERROR || _la==FALSE || _la==TRUE || _la==UNKNOWN) ) {
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
	public static class JsonValueBehaviorContext extends ParserRuleContext {
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public JsonValueBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonValueBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonValueBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonValueBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonValueBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonValueBehaviorContext jsonValueBehavior() throws RecognitionException {
		JsonValueBehaviorContext _localctx = new JsonValueBehaviorContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_jsonValueBehavior);
		try {
			setState(2726);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ERROR:
				enterOuterAlt(_localctx, 1);
				{
				setState(2722);
				match(ERROR);
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2723);
				match(NULL);
				}
				break;
			case DEFAULT:
				enterOuterAlt(_localctx, 3);
				{
				setState(2724);
				match(DEFAULT);
				setState(2725);
				expression();
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
	public static class JsonQueryWrapperBehaviorContext extends ParserRuleContext {
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode CONDITIONAL() { return getToken(SqlBaseParser.CONDITIONAL, 0); }
		public TerminalNode UNCONDITIONAL() { return getToken(SqlBaseParser.UNCONDITIONAL, 0); }
		public JsonQueryWrapperBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonQueryWrapperBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonQueryWrapperBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonQueryWrapperBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonQueryWrapperBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonQueryWrapperBehaviorContext jsonQueryWrapperBehavior() throws RecognitionException {
		JsonQueryWrapperBehaviorContext _localctx = new JsonQueryWrapperBehaviorContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_jsonQueryWrapperBehavior);
		int _la;
		try {
			setState(2739);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WITHOUT:
				enterOuterAlt(_localctx, 1);
				{
				setState(2728);
				match(WITHOUT);
				setState(2730);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ARRAY) {
					{
					setState(2729);
					match(ARRAY);
					}
				}

				}
				break;
			case WITH:
				enterOuterAlt(_localctx, 2);
				{
				setState(2732);
				match(WITH);
				setState(2734);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CONDITIONAL || _la==UNCONDITIONAL) {
					{
					setState(2733);
					_la = _input.LA(1);
					if ( !(_la==CONDITIONAL || _la==UNCONDITIONAL) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2737);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ARRAY) {
					{
					setState(2736);
					match(ARRAY);
					}
				}

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
	public static class JsonQueryBehaviorContext extends ParserRuleContext {
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode OBJECT() { return getToken(SqlBaseParser.OBJECT, 0); }
		public JsonQueryBehaviorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonQueryBehavior; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonQueryBehavior(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonQueryBehavior(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonQueryBehavior(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonQueryBehaviorContext jsonQueryBehavior() throws RecognitionException {
		JsonQueryBehaviorContext _localctx = new JsonQueryBehaviorContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_jsonQueryBehavior);
		try {
			setState(2747);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,360,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2741);
				match(ERROR);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2742);
				match(NULL);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2743);
				match(EMPTY);
				setState(2744);
				match(ARRAY);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2745);
				match(EMPTY);
				setState(2746);
				match(OBJECT);
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
	public static class JsonObjectMemberContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode VALUE() { return getToken(SqlBaseParser.VALUE, 0); }
		public JsonValueExpressionContext jsonValueExpression() {
			return getRuleContext(JsonValueExpressionContext.class,0);
		}
		public TerminalNode KEY() { return getToken(SqlBaseParser.KEY, 0); }
		public JsonObjectMemberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsonObjectMember; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJsonObjectMember(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJsonObjectMember(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJsonObjectMember(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JsonObjectMemberContext jsonObjectMember() throws RecognitionException {
		JsonObjectMemberContext _localctx = new JsonObjectMemberContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_jsonObjectMember);
		try {
			setState(2760);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,362,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2750);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,361,_ctx) ) {
				case 1:
					{
					setState(2749);
					match(KEY);
					}
					break;
				}
				setState(2752);
				expression();
				setState(2753);
				match(VALUE);
				setState(2754);
				jsonValueExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2756);
				expression();
				setState(2757);
				match(T__9);
				setState(2758);
				jsonValueExpression();
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
	public static class ProcessingModeContext extends ParserRuleContext {
		public TerminalNode RUNNING() { return getToken(SqlBaseParser.RUNNING, 0); }
		public TerminalNode FINAL() { return getToken(SqlBaseParser.FINAL, 0); }
		public ProcessingModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_processingMode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterProcessingMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitProcessingMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitProcessingMode(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ProcessingModeContext processingMode() throws RecognitionException {
		ProcessingModeContext _localctx = new ProcessingModeContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_processingMode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2762);
			_la = _input.LA(1);
			if ( !(_la==FINAL || _la==RUNNING) ) {
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
	public static class NullTreatmentContext extends ParserRuleContext {
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public NullTreatmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullTreatment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullTreatment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullTreatment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullTreatment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullTreatmentContext nullTreatment() throws RecognitionException {
		NullTreatmentContext _localctx = new NullTreatmentContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_nullTreatment);
		try {
			setState(2768);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IGNORE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2764);
				match(IGNORE);
				setState(2765);
				match(NULLS);
				}
				break;
			case RESPECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(2766);
				match(RESPECT);
				setState(2767);
				match(NULLS);
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
	public static class StringContext extends ParserRuleContext {
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
	 
		public StringContext() { }
		public void copyFrom(StringContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnicodeStringLiteralContext extends StringContext {
		public TerminalNode UNICODE_STRING() { return getToken(SqlBaseParser.UNICODE_STRING, 0); }
		public TerminalNode UESCAPE() { return getToken(SqlBaseParser.UESCAPE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public UnicodeStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnicodeStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnicodeStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnicodeStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BasicStringLiteralContext extends StringContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public BasicStringLiteralContext(StringContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBasicStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBasicStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBasicStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_string);
		try {
			setState(2776);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				_localctx = new BasicStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2770);
				match(STRING);
				}
				break;
			case UNICODE_STRING:
				_localctx = new UnicodeStringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2771);
				match(UNICODE_STRING);
				setState(2774);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,364,_ctx) ) {
				case 1:
					{
					setState(2772);
					match(UESCAPE);
					setState(2773);
					match(STRING);
					}
					break;
				}
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
	public static class TimeZoneSpecifierContext extends ParserRuleContext {
		public TimeZoneSpecifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeZoneSpecifier; }
	 
		public TimeZoneSpecifierContext() { }
		public void copyFrom(TimeZoneSpecifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneIntervalContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public TimeZoneIntervalContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneInterval(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeZoneStringContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TimeZoneStringContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneString(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneString(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeZoneSpecifierContext timeZoneSpecifier() throws RecognitionException {
		TimeZoneSpecifierContext _localctx = new TimeZoneSpecifierContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_timeZoneSpecifier);
		try {
			setState(2784);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,366,_ctx) ) {
			case 1:
				_localctx = new TimeZoneIntervalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2778);
				match(TIME);
				setState(2779);
				match(ZONE);
				setState(2780);
				interval();
				}
				break;
			case 2:
				_localctx = new TimeZoneStringContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2781);
				match(TIME);
				setState(2782);
				match(ZONE);
				setState(2783);
				string();
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
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2786);
			_la = _input.LA(1);
			if ( !(((((_la - 312)) & ~0x3f) == 0 && ((1L << (_la - 312)) & 63L) != 0)) ) {
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
	public static class ComparisonQuantifierContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public ComparisonQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonQuantifierContext comparisonQuantifier() throws RecognitionException {
		ComparisonQuantifierContext _localctx = new ComparisonQuantifierContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_comparisonQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2788);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
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
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2790);
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
	public static class IntervalContext extends ParserRuleContext {
		public Token sign;
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_interval);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2792);
			match(INTERVAL);
			setState(2794);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(2793);
				((IntervalContext)_localctx).sign = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(2796);
			string();
			setState(2797);
			((IntervalContext)_localctx).from = intervalField();
			setState(2800);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,368,_ctx) ) {
			case 1:
				{
				setState(2798);
				match(TO);
				setState(2799);
				((IntervalContext)_localctx).to = intervalField();
				}
				break;
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
	public static class IntervalFieldContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_intervalField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2802);
			_la = _input.LA(1);
			if ( !(_la==DAY || _la==HOUR || _la==MINUTE || _la==MONTH || _la==SECOND || _la==YEAR) ) {
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
	public static class NormalFormContext extends ParserRuleContext {
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public NormalFormContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_normalForm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalForm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalForm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalForm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NormalFormContext normalForm() throws RecognitionException {
		NormalFormContext _localctx = new NormalFormContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_normalForm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2804);
			_la = _input.LA(1);
			if ( !(((((_la - 175)) & ~0x3f) == 0 && ((1L << (_la - 175)) & 15L) != 0)) ) {
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
	public static class TypeContext extends ParserRuleContext {
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
	 
		public TypeContext() { }
		public void copyFrom(TypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowTypeContext extends TypeContext {
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public List<RowFieldContext> rowField() {
			return getRuleContexts(RowFieldContext.class);
		}
		public RowFieldContext rowField(int i) {
			return getRuleContext(RowFieldContext.class,i);
		}
		public RowTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalTypeContext extends TypeContext {
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayTypeContext extends TypeContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public ArrayTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArrayType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArrayType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArrayType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoublePrecisionTypeContext extends TypeContext {
		public TerminalNode DOUBLE() { return getToken(SqlBaseParser.DOUBLE, 0); }
		public TerminalNode PRECISION() { return getToken(SqlBaseParser.PRECISION, 0); }
		public DoublePrecisionTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoublePrecisionType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoublePrecisionType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoublePrecisionType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LegacyArrayTypeContext extends TypeContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public LegacyArrayTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLegacyArrayType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLegacyArrayType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLegacyArrayType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GenericTypeContext extends TypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TypeParameterContext> typeParameter() {
			return getRuleContexts(TypeParameterContext.class);
		}
		public TypeParameterContext typeParameter(int i) {
			return getRuleContext(TypeParameterContext.class,i);
		}
		public GenericTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGenericType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGenericType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGenericType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DateTimeTypeContext extends TypeContext {
		public Token base;
		public TypeParameterContext precision;
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public List<TerminalNode> TIME() { return getTokens(SqlBaseParser.TIME); }
		public TerminalNode TIME(int i) {
			return getToken(SqlBaseParser.TIME, i);
		}
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public TypeParameterContext typeParameter() {
			return getRuleContext(TypeParameterContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public DateTimeTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDateTimeType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDateTimeType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDateTimeType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LegacyMapTypeContext extends TypeContext {
		public TypeContext keyType;
		public TypeContext valueType;
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public LegacyMapTypeContext(TypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLegacyMapType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLegacyMapType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLegacyMapType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeContext type() throws RecognitionException {
		return type(0);
	}

	private TypeContext type(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TypeContext _localctx = new TypeContext(_ctx, _parentState);
		TypeContext _prevctx = _localctx;
		int _startState = 174;
		enterRecursionRule(_localctx, 174, RULE_type, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2897);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,379,_ctx) ) {
			case 1:
				{
				_localctx = new RowTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2807);
				match(ROW);
				setState(2808);
				match(T__1);
				setState(2809);
				rowField();
				setState(2814);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2810);
					match(T__3);
					setState(2811);
					rowField();
					}
					}
					setState(2816);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2817);
				match(T__2);
				}
				break;
			case 2:
				{
				_localctx = new IntervalTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2819);
				match(INTERVAL);
				setState(2820);
				((IntervalTypeContext)_localctx).from = intervalField();
				setState(2823);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,370,_ctx) ) {
				case 1:
					{
					setState(2821);
					match(TO);
					setState(2822);
					((IntervalTypeContext)_localctx).to = intervalField();
					}
					break;
				}
				}
				break;
			case 3:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2825);
				((DateTimeTypeContext)_localctx).base = match(TIMESTAMP);
				setState(2830);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,371,_ctx) ) {
				case 1:
					{
					setState(2826);
					match(T__1);
					setState(2827);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2828);
					match(T__2);
					}
					break;
				}
				setState(2835);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,372,_ctx) ) {
				case 1:
					{
					setState(2832);
					match(WITHOUT);
					setState(2833);
					match(TIME);
					setState(2834);
					match(ZONE);
					}
					break;
				}
				}
				break;
			case 4:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2837);
				((DateTimeTypeContext)_localctx).base = match(TIMESTAMP);
				setState(2842);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(2838);
					match(T__1);
					setState(2839);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2840);
					match(T__2);
					}
				}

				setState(2844);
				match(WITH);
				setState(2845);
				match(TIME);
				setState(2846);
				match(ZONE);
				}
				break;
			case 5:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2847);
				((DateTimeTypeContext)_localctx).base = match(TIME);
				setState(2852);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,374,_ctx) ) {
				case 1:
					{
					setState(2848);
					match(T__1);
					setState(2849);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2850);
					match(T__2);
					}
					break;
				}
				setState(2857);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,375,_ctx) ) {
				case 1:
					{
					setState(2854);
					match(WITHOUT);
					setState(2855);
					match(TIME);
					setState(2856);
					match(ZONE);
					}
					break;
				}
				}
				break;
			case 6:
				{
				_localctx = new DateTimeTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2859);
				((DateTimeTypeContext)_localctx).base = match(TIME);
				setState(2864);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(2860);
					match(T__1);
					setState(2861);
					((DateTimeTypeContext)_localctx).precision = typeParameter();
					setState(2862);
					match(T__2);
					}
				}

				setState(2866);
				match(WITH);
				setState(2867);
				match(TIME);
				setState(2868);
				match(ZONE);
				}
				break;
			case 7:
				{
				_localctx = new DoublePrecisionTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2869);
				match(DOUBLE);
				setState(2870);
				match(PRECISION);
				}
				break;
			case 8:
				{
				_localctx = new LegacyArrayTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2871);
				match(ARRAY);
				setState(2872);
				match(LT);
				setState(2873);
				type(0);
				setState(2874);
				match(GT);
				}
				break;
			case 9:
				{
				_localctx = new LegacyMapTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2876);
				match(MAP);
				setState(2877);
				match(LT);
				setState(2878);
				((LegacyMapTypeContext)_localctx).keyType = type(0);
				setState(2879);
				match(T__3);
				setState(2880);
				((LegacyMapTypeContext)_localctx).valueType = type(0);
				setState(2881);
				match(GT);
				}
				break;
			case 10:
				{
				_localctx = new GenericTypeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2883);
				identifier();
				setState(2895);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,378,_ctx) ) {
				case 1:
					{
					setState(2884);
					match(T__1);
					setState(2885);
					typeParameter();
					setState(2890);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2886);
						match(T__3);
						setState(2887);
						typeParameter();
						}
						}
						setState(2892);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2893);
					match(T__2);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2908);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,381,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new ArrayTypeContext(new TypeContext(_parentctx, _parentState));
					pushNewRecursionContext(_localctx, _startState, RULE_type);
					setState(2899);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(2900);
					match(ARRAY);
					setState(2904);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,380,_ctx) ) {
					case 1:
						{
						setState(2901);
						match(T__7);
						setState(2902);
						match(INTEGER_VALUE);
						setState(2903);
						match(T__8);
						}
						break;
					}
					}
					} 
				}
				setState(2910);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,381,_ctx);
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
	public static class RowFieldContext extends ParserRuleContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RowFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFieldContext rowField() throws RecognitionException {
		RowFieldContext _localctx = new RowFieldContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_rowField);
		try {
			setState(2915);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,382,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2911);
				type(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2912);
				identifier();
				setState(2913);
				type(0);
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
	public static class TypeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TypeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeParameterContext typeParameter() throws RecognitionException {
		TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_typeParameter);
		try {
			setState(2919);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2917);
				match(INTEGER_VALUE);
				}
				break;
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(2918);
				type(0);
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
	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2921);
			match(WHEN);
			setState(2922);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2923);
			match(THEN);
			setState(2924);
			((WhenClauseContext)_localctx).result = expression();
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
	public static class FilterContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public FilterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFilter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFilter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFilter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterContext filter() throws RecognitionException {
		FilterContext _localctx = new FilterContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_filter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2926);
			match(FILTER);
			setState(2927);
			match(T__1);
			setState(2928);
			match(WHERE);
			setState(2929);
			booleanExpression(0);
			setState(2930);
			match(T__2);
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
	public static class MergeCaseContext extends ParserRuleContext {
		public MergeCaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mergeCase; }
	 
		public MergeCaseContext() { }
		public void copyFrom(MergeCaseContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeInsertContext extends MergeCaseContext {
		public ExpressionContext condition;
		public IdentifierContext identifier;
		public List<IdentifierContext> targets = new ArrayList<IdentifierContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> values = new ArrayList<ExpressionContext>();
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public MergeInsertContext(MergeCaseContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeInsert(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeInsert(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMergeInsert(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeUpdateContext extends MergeCaseContext {
		public ExpressionContext condition;
		public IdentifierContext identifier;
		public List<IdentifierContext> targets = new ArrayList<IdentifierContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> values = new ArrayList<ExpressionContext>();
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public List<TerminalNode> EQ() { return getTokens(SqlBaseParser.EQ); }
		public TerminalNode EQ(int i) {
			return getToken(SqlBaseParser.EQ, i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public MergeUpdateContext(MergeCaseContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeUpdate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMergeUpdate(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeDeleteContext extends MergeCaseContext {
		public ExpressionContext condition;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public MergeDeleteContext(MergeCaseContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMergeDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMergeDelete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMergeDelete(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MergeCaseContext mergeCase() throws RecognitionException {
		MergeCaseContext _localctx = new MergeCaseContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_mergeCase);
		int _la;
		try {
			setState(2996);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,391,_ctx) ) {
			case 1:
				_localctx = new MergeUpdateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2932);
				match(WHEN);
				setState(2933);
				match(MATCHED);
				setState(2936);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AND) {
					{
					setState(2934);
					match(AND);
					setState(2935);
					((MergeUpdateContext)_localctx).condition = expression();
					}
				}

				setState(2938);
				match(THEN);
				setState(2939);
				match(UPDATE);
				setState(2940);
				match(SET);
				setState(2941);
				((MergeUpdateContext)_localctx).identifier = identifier();
				((MergeUpdateContext)_localctx).targets.add(((MergeUpdateContext)_localctx).identifier);
				setState(2942);
				match(EQ);
				setState(2943);
				((MergeUpdateContext)_localctx).expression = expression();
				((MergeUpdateContext)_localctx).values.add(((MergeUpdateContext)_localctx).expression);
				setState(2951);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2944);
					match(T__3);
					setState(2945);
					((MergeUpdateContext)_localctx).identifier = identifier();
					((MergeUpdateContext)_localctx).targets.add(((MergeUpdateContext)_localctx).identifier);
					setState(2946);
					match(EQ);
					setState(2947);
					((MergeUpdateContext)_localctx).expression = expression();
					((MergeUpdateContext)_localctx).values.add(((MergeUpdateContext)_localctx).expression);
					}
					}
					setState(2953);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				_localctx = new MergeDeleteContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2954);
				match(WHEN);
				setState(2955);
				match(MATCHED);
				setState(2958);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AND) {
					{
					setState(2956);
					match(AND);
					setState(2957);
					((MergeDeleteContext)_localctx).condition = expression();
					}
				}

				setState(2960);
				match(THEN);
				setState(2961);
				match(DELETE);
				}
				break;
			case 3:
				_localctx = new MergeInsertContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2962);
				match(WHEN);
				setState(2963);
				match(NOT);
				setState(2964);
				match(MATCHED);
				setState(2967);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AND) {
					{
					setState(2965);
					match(AND);
					setState(2966);
					((MergeInsertContext)_localctx).condition = expression();
					}
				}

				setState(2969);
				match(THEN);
				setState(2970);
				match(INSERT);
				setState(2982);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(2971);
					match(T__1);
					setState(2972);
					((MergeInsertContext)_localctx).identifier = identifier();
					((MergeInsertContext)_localctx).targets.add(((MergeInsertContext)_localctx).identifier);
					setState(2977);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(2973);
						match(T__3);
						setState(2974);
						((MergeInsertContext)_localctx).identifier = identifier();
						((MergeInsertContext)_localctx).targets.add(((MergeInsertContext)_localctx).identifier);
						}
						}
						setState(2979);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2980);
					match(T__2);
					}
				}

				setState(2984);
				match(VALUES);
				setState(2985);
				match(T__1);
				setState(2986);
				((MergeInsertContext)_localctx).expression = expression();
				((MergeInsertContext)_localctx).values.add(((MergeInsertContext)_localctx).expression);
				setState(2991);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(2987);
					match(T__3);
					setState(2988);
					((MergeInsertContext)_localctx).expression = expression();
					((MergeInsertContext)_localctx).values.add(((MergeInsertContext)_localctx).expression);
					}
					}
					setState(2993);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2994);
				match(T__2);
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
	public static class OverContext extends ParserRuleContext {
		public IdentifierContext windowName;
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecificationContext windowSpecification() {
			return getRuleContext(WindowSpecificationContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public OverContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_over; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOver(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOver(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOver(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OverContext over() throws RecognitionException {
		OverContext _localctx = new OverContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_over);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2998);
			match(OVER);
			setState(3004);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				{
				setState(2999);
				((OverContext)_localctx).windowName = identifier();
				}
				break;
			case T__1:
				{
				setState(3000);
				match(T__1);
				setState(3001);
				windowSpecification();
				setState(3002);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
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
	public static class WindowFrameContext extends ParserRuleContext {
		public FrameExtentContext frameExtent() {
			return getRuleContext(FrameExtentContext.class,0);
		}
		public TerminalNode MEASURES() { return getToken(SqlBaseParser.MEASURES, 0); }
		public List<MeasureDefinitionContext> measureDefinition() {
			return getRuleContexts(MeasureDefinitionContext.class);
		}
		public MeasureDefinitionContext measureDefinition(int i) {
			return getRuleContext(MeasureDefinitionContext.class,i);
		}
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public SkipToContext skipTo() {
			return getRuleContext(SkipToContext.class,0);
		}
		public TerminalNode PATTERN() { return getToken(SqlBaseParser.PATTERN, 0); }
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public TerminalNode SUBSET() { return getToken(SqlBaseParser.SUBSET, 0); }
		public List<SubsetDefinitionContext> subsetDefinition() {
			return getRuleContexts(SubsetDefinitionContext.class);
		}
		public SubsetDefinitionContext subsetDefinition(int i) {
			return getRuleContext(SubsetDefinitionContext.class,i);
		}
		public TerminalNode DEFINE() { return getToken(SqlBaseParser.DEFINE, 0); }
		public List<VariableDefinitionContext> variableDefinition() {
			return getRuleContexts(VariableDefinitionContext.class);
		}
		public VariableDefinitionContext variableDefinition(int i) {
			return getRuleContext(VariableDefinitionContext.class,i);
		}
		public TerminalNode INITIAL() { return getToken(SqlBaseParser.INITIAL, 0); }
		public TerminalNode SEEK() { return getToken(SqlBaseParser.SEEK, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_windowFrame);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3015);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MEASURES) {
				{
				setState(3006);
				match(MEASURES);
				setState(3007);
				measureDefinition();
				setState(3012);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(3008);
					match(T__3);
					setState(3009);
					measureDefinition();
					}
					}
					setState(3014);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(3017);
			frameExtent();
			setState(3021);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AFTER) {
				{
				setState(3018);
				match(AFTER);
				setState(3019);
				match(MATCH);
				setState(3020);
				skipTo();
				}
			}

			setState(3024);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INITIAL || _la==SEEK) {
				{
				setState(3023);
				_la = _input.LA(1);
				if ( !(_la==INITIAL || _la==SEEK) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(3031);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PATTERN) {
				{
				setState(3026);
				match(PATTERN);
				setState(3027);
				match(T__1);
				setState(3028);
				rowPattern(0);
				setState(3029);
				match(T__2);
				}
			}

			setState(3042);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SUBSET) {
				{
				setState(3033);
				match(SUBSET);
				setState(3034);
				subsetDefinition();
				setState(3039);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(3035);
					match(T__3);
					setState(3036);
					subsetDefinition();
					}
					}
					setState(3041);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(3053);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFINE) {
				{
				setState(3044);
				match(DEFINE);
				setState(3045);
				variableDefinition();
				setState(3050);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(3046);
					match(T__3);
					setState(3047);
					variableDefinition();
					}
					}
					setState(3052);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
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
	public static class FrameExtentContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode GROUPS() { return getToken(SqlBaseParser.GROUPS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public FrameExtentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameExtent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFrameExtent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFrameExtent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFrameExtent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameExtentContext frameExtent() throws RecognitionException {
		FrameExtentContext _localctx = new FrameExtentContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_frameExtent);
		try {
			setState(3079);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,402,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3055);
				((FrameExtentContext)_localctx).frameType = match(RANGE);
				setState(3056);
				((FrameExtentContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3057);
				((FrameExtentContext)_localctx).frameType = match(ROWS);
				setState(3058);
				((FrameExtentContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3059);
				((FrameExtentContext)_localctx).frameType = match(GROUPS);
				setState(3060);
				((FrameExtentContext)_localctx).start = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(3061);
				((FrameExtentContext)_localctx).frameType = match(RANGE);
				setState(3062);
				match(BETWEEN);
				setState(3063);
				((FrameExtentContext)_localctx).start = frameBound();
				setState(3064);
				match(AND);
				setState(3065);
				((FrameExtentContext)_localctx).end = frameBound();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(3067);
				((FrameExtentContext)_localctx).frameType = match(ROWS);
				setState(3068);
				match(BETWEEN);
				setState(3069);
				((FrameExtentContext)_localctx).start = frameBound();
				setState(3070);
				match(AND);
				setState(3071);
				((FrameExtentContext)_localctx).end = frameBound();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(3073);
				((FrameExtentContext)_localctx).frameType = match(GROUPS);
				setState(3074);
				match(BETWEEN);
				setState(3075);
				((FrameExtentContext)_localctx).start = frameBound();
				setState(3076);
				match(AND);
				setState(3077);
				((FrameExtentContext)_localctx).end = frameBound();
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
	public static class FrameBoundContext extends ParserRuleContext {
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
	 
		public FrameBoundContext() { }
		public void copyFrom(FrameBoundContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BoundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public BoundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBoundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBoundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBoundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnboundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public UnboundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnboundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnboundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnboundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRowBoundContext extends FrameBoundContext {
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public CurrentRowBoundContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRowBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRowBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRowBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_frameBound);
		int _la;
		try {
			setState(3090);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,403,_ctx) ) {
			case 1:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3081);
				match(UNBOUNDED);
				setState(3082);
				((UnboundedFrameContext)_localctx).boundType = match(PRECEDING);
				}
				break;
			case 2:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3083);
				match(UNBOUNDED);
				setState(3084);
				((UnboundedFrameContext)_localctx).boundType = match(FOLLOWING);
				}
				break;
			case 3:
				_localctx = new CurrentRowBoundContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3085);
				match(CURRENT);
				setState(3086);
				match(ROW);
				}
				break;
			case 4:
				_localctx = new BoundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3087);
				expression();
				setState(3088);
				((BoundedFrameContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((BoundedFrameContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class RowPatternContext extends ParserRuleContext {
		public RowPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowPattern; }
	 
		public RowPatternContext() { }
		public void copyFrom(RowPatternContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuantifiedPrimaryContext extends RowPatternContext {
		public PatternPrimaryContext patternPrimary() {
			return getRuleContext(PatternPrimaryContext.class,0);
		}
		public PatternQuantifierContext patternQuantifier() {
			return getRuleContext(PatternQuantifierContext.class,0);
		}
		public QuantifiedPrimaryContext(RowPatternContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuantifiedPrimary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuantifiedPrimary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuantifiedPrimary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternConcatenationContext extends RowPatternContext {
		public List<RowPatternContext> rowPattern() {
			return getRuleContexts(RowPatternContext.class);
		}
		public RowPatternContext rowPattern(int i) {
			return getRuleContext(RowPatternContext.class,i);
		}
		public PatternConcatenationContext(RowPatternContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternAlternationContext extends RowPatternContext {
		public List<RowPatternContext> rowPattern() {
			return getRuleContexts(RowPatternContext.class);
		}
		public RowPatternContext rowPattern(int i) {
			return getRuleContext(RowPatternContext.class,i);
		}
		public PatternAlternationContext(RowPatternContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternAlternation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternAlternation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternAlternation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowPatternContext rowPattern() throws RecognitionException {
		return rowPattern(0);
	}

	private RowPatternContext rowPattern(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RowPatternContext _localctx = new RowPatternContext(_ctx, _parentState);
		RowPatternContext _prevctx = _localctx;
		int _startState = 194;
		enterRecursionRule(_localctx, 194, RULE_rowPattern, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QuantifiedPrimaryContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(3093);
			patternPrimary();
			setState(3095);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,404,_ctx) ) {
			case 1:
				{
				setState(3094);
				patternQuantifier();
				}
				break;
			}
			}
			_ctx.stop = _input.LT(-1);
			setState(3104);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,406,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(3102);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,405,_ctx) ) {
					case 1:
						{
						_localctx = new PatternConcatenationContext(new RowPatternContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_rowPattern);
						setState(3097);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(3098);
						rowPattern(3);
						}
						break;
					case 2:
						{
						_localctx = new PatternAlternationContext(new RowPatternContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_rowPattern);
						setState(3099);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(3100);
						match(T__10);
						setState(3101);
						rowPattern(2);
						}
						break;
					}
					} 
				}
				setState(3106);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,406,_ctx);
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
	public static class PatternPrimaryContext extends ParserRuleContext {
		public PatternPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patternPrimary; }
	 
		public PatternPrimaryContext() { }
		public void copyFrom(PatternPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternPermutationContext extends PatternPrimaryContext {
		public TerminalNode PERMUTE() { return getToken(SqlBaseParser.PERMUTE, 0); }
		public List<RowPatternContext> rowPattern() {
			return getRuleContexts(RowPatternContext.class);
		}
		public RowPatternContext rowPattern(int i) {
			return getRuleContext(RowPatternContext.class,i);
		}
		public PatternPermutationContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternPermutation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternPermutation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternPermutation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PartitionEndAnchorContext extends PatternPrimaryContext {
		public PartitionEndAnchorContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionEndAnchor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionEndAnchor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionEndAnchor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PatternVariableContext extends PatternPrimaryContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PatternVariableContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPatternVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPatternVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPatternVariable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExcludedPatternContext extends PatternPrimaryContext {
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public ExcludedPatternContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExcludedPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExcludedPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExcludedPattern(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PartitionStartAnchorContext extends PatternPrimaryContext {
		public PartitionStartAnchorContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionStartAnchor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionStartAnchor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionStartAnchor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class EmptyPatternContext extends PatternPrimaryContext {
		public EmptyPatternContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterEmptyPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitEmptyPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitEmptyPattern(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GroupedPatternContext extends PatternPrimaryContext {
		public RowPatternContext rowPattern() {
			return getRuleContext(RowPatternContext.class,0);
		}
		public GroupedPatternContext(PatternPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupedPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupedPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupedPattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatternPrimaryContext patternPrimary() throws RecognitionException {
		PatternPrimaryContext _localctx = new PatternPrimaryContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_patternPrimary);
		int _la;
		try {
			setState(3132);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,408,_ctx) ) {
			case 1:
				_localctx = new PatternVariableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3107);
				identifier();
				}
				break;
			case 2:
				_localctx = new EmptyPatternContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3108);
				match(T__1);
				setState(3109);
				match(T__2);
				}
				break;
			case 3:
				_localctx = new PatternPermutationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3110);
				match(PERMUTE);
				setState(3111);
				match(T__1);
				setState(3112);
				rowPattern(0);
				setState(3117);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(3113);
					match(T__3);
					setState(3114);
					rowPattern(0);
					}
					}
					setState(3119);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(3120);
				match(T__2);
				}
				break;
			case 4:
				_localctx = new GroupedPatternContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3122);
				match(T__1);
				setState(3123);
				rowPattern(0);
				setState(3124);
				match(T__2);
				}
				break;
			case 5:
				_localctx = new PartitionStartAnchorContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3126);
				match(T__11);
				}
				break;
			case 6:
				_localctx = new PartitionEndAnchorContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(3127);
				match(T__12);
				}
				break;
			case 7:
				_localctx = new ExcludedPatternContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(3128);
				match(T__13);
				setState(3129);
				rowPattern(0);
				setState(3130);
				match(T__14);
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
	public static class PatternQuantifierContext extends ParserRuleContext {
		public PatternQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patternQuantifier; }
	 
		public PatternQuantifierContext() { }
		public void copyFrom(PatternQuantifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ZeroOrMoreQuantifierContext extends PatternQuantifierContext {
		public Token reluctant;
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public ZeroOrMoreQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterZeroOrMoreQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitZeroOrMoreQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitZeroOrMoreQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OneOrMoreQuantifierContext extends PatternQuantifierContext {
		public Token reluctant;
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public OneOrMoreQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOneOrMoreQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOneOrMoreQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOneOrMoreQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ZeroOrOneQuantifierContext extends PatternQuantifierContext {
		public Token reluctant;
		public List<TerminalNode> QUESTION_MARK() { return getTokens(SqlBaseParser.QUESTION_MARK); }
		public TerminalNode QUESTION_MARK(int i) {
			return getToken(SqlBaseParser.QUESTION_MARK, i);
		}
		public ZeroOrOneQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterZeroOrOneQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitZeroOrOneQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitZeroOrOneQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RangeQuantifierContext extends PatternQuantifierContext {
		public Token exactly;
		public Token reluctant;
		public Token atLeast;
		public Token atMost;
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode QUESTION_MARK() { return getToken(SqlBaseParser.QUESTION_MARK, 0); }
		public RangeQuantifierContext(PatternQuantifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRangeQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRangeQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRangeQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatternQuantifierContext patternQuantifier() throws RecognitionException {
		PatternQuantifierContext _localctx = new PatternQuantifierContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_patternQuantifier);
		int _la;
		try {
			setState(3164);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,416,_ctx) ) {
			case 1:
				_localctx = new ZeroOrMoreQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3134);
				match(ASTERISK);
				setState(3136);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,409,_ctx) ) {
				case 1:
					{
					setState(3135);
					((ZeroOrMoreQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new OneOrMoreQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3138);
				match(PLUS);
				setState(3140);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,410,_ctx) ) {
				case 1:
					{
					setState(3139);
					((OneOrMoreQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new ZeroOrOneQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3142);
				match(QUESTION_MARK);
				setState(3144);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,411,_ctx) ) {
				case 1:
					{
					setState(3143);
					((ZeroOrOneQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new RangeQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3146);
				match(T__15);
				setState(3147);
				((RangeQuantifierContext)_localctx).exactly = match(INTEGER_VALUE);
				setState(3148);
				match(T__16);
				setState(3150);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,412,_ctx) ) {
				case 1:
					{
					setState(3149);
					((RangeQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
				}
				break;
			case 5:
				_localctx = new RangeQuantifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3152);
				match(T__15);
				setState(3154);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTEGER_VALUE) {
					{
					setState(3153);
					((RangeQuantifierContext)_localctx).atLeast = match(INTEGER_VALUE);
					}
				}

				setState(3156);
				match(T__3);
				setState(3158);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INTEGER_VALUE) {
					{
					setState(3157);
					((RangeQuantifierContext)_localctx).atMost = match(INTEGER_VALUE);
					}
				}

				setState(3160);
				match(T__16);
				setState(3162);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,415,_ctx) ) {
				case 1:
					{
					setState(3161);
					((RangeQuantifierContext)_localctx).reluctant = match(QUESTION_MARK);
					}
					break;
				}
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
	public static class UpdateAssignmentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public UpdateAssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateAssignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUpdateAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUpdateAssignment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUpdateAssignment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateAssignmentContext updateAssignment() throws RecognitionException {
		UpdateAssignmentContext _localctx = new UpdateAssignmentContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_updateAssignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3166);
			identifier();
			setState(3167);
			match(EQ);
			setState(3168);
			expression();
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
	public static class ExplainOptionContext extends ParserRuleContext {
		public ExplainOptionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explainOption; }
	 
		public ExplainOptionContext() { }
		public void copyFrom(ExplainOptionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainFormatContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public ExplainFormatContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainTypeContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode IO() { return getToken(SqlBaseParser.IO, 0); }
		public ExplainTypeContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExplainOptionContext explainOption() throws RecognitionException {
		ExplainOptionContext _localctx = new ExplainOptionContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_explainOption);
		int _la;
		try {
			setState(3174);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FORMAT:
				_localctx = new ExplainFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3170);
				match(FORMAT);
				setState(3171);
				((ExplainFormatContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==GRAPHVIZ || _la==JSON || _la==TEXT) ) {
					((ExplainFormatContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case TYPE:
				_localctx = new ExplainTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3172);
				match(TYPE);
				setState(3173);
				((ExplainTypeContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DISTRIBUTED || _la==IO || _la==LOGICAL || _la==VALIDATE) ) {
					((ExplainTypeContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class TransactionModeContext extends ParserRuleContext {
		public TransactionModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transactionMode; }
	 
		public TransactionModeContext() { }
		public void copyFrom(TransactionModeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TransactionAccessModeContext extends TransactionModeContext {
		public Token accessMode;
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TransactionAccessModeContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransactionAccessMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransactionAccessMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTransactionAccessMode(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IsolationLevelContext extends TransactionModeContext {
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public LevelOfIsolationContext levelOfIsolation() {
			return getRuleContext(LevelOfIsolationContext.class,0);
		}
		public IsolationLevelContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIsolationLevel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIsolationLevel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIsolationLevel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransactionModeContext transactionMode() throws RecognitionException {
		TransactionModeContext _localctx = new TransactionModeContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_transactionMode);
		int _la;
		try {
			setState(3181);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ISOLATION:
				_localctx = new IsolationLevelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3176);
				match(ISOLATION);
				setState(3177);
				match(LEVEL);
				setState(3178);
				levelOfIsolation();
				}
				break;
			case READ:
				_localctx = new TransactionAccessModeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3179);
				match(READ);
				setState(3180);
				((TransactionAccessModeContext)_localctx).accessMode = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ONLY || _la==WRITE) ) {
					((TransactionAccessModeContext)_localctx).accessMode = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class LevelOfIsolationContext extends ParserRuleContext {
		public LevelOfIsolationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_levelOfIsolation; }
	 
		public LevelOfIsolationContext() { }
		public void copyFrom(LevelOfIsolationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReadUncommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public ReadUncommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadUncommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadUncommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadUncommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SerializableContext extends LevelOfIsolationContext {
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public SerializableContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSerializable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSerializable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSerializable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReadCommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public ReadCommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadCommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadCommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadCommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepeatableReadContext extends LevelOfIsolationContext {
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public RepeatableReadContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepeatableRead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepeatableRead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepeatableRead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LevelOfIsolationContext levelOfIsolation() throws RecognitionException {
		LevelOfIsolationContext _localctx = new LevelOfIsolationContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_levelOfIsolation);
		try {
			setState(3190);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,419,_ctx) ) {
			case 1:
				_localctx = new ReadUncommittedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3183);
				match(READ);
				setState(3184);
				match(UNCOMMITTED);
				}
				break;
			case 2:
				_localctx = new ReadCommittedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3185);
				match(READ);
				setState(3186);
				match(COMMITTED);
				}
				break;
			case 3:
				_localctx = new RepeatableReadContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3187);
				match(REPEATABLE);
				setState(3188);
				match(READ);
				}
				break;
			case 4:
				_localctx = new SerializableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3189);
				match(SERIALIZABLE);
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
	public static class CallArgumentContext extends ParserRuleContext {
		public CallArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_callArgument; }
	 
		public CallArgumentContext() { }
		public void copyFrom(CallArgumentContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionalArgumentContext extends CallArgumentContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PositionalArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPositionalArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPositionalArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPositionalArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NamedArgumentContext extends CallArgumentContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NamedArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CallArgumentContext callArgument() throws RecognitionException {
		CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_callArgument);
		try {
			setState(3197);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,420,_ctx) ) {
			case 1:
				_localctx = new PositionalArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3192);
				expression();
				}
				break;
			case 2:
				_localctx = new NamedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3193);
				identifier();
				setState(3194);
				match(T__5);
				setState(3195);
				expression();
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
	public static class PathElementContext extends ParserRuleContext {
		public PathElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathElement; }
	 
		public PathElementContext() { }
		public void copyFrom(PathElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedArgumentContext extends PathElementContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedArgumentContext(PathElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnqualifiedArgumentContext extends PathElementContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnqualifiedArgumentContext(PathElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnqualifiedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnqualifiedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnqualifiedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathElementContext pathElement() throws RecognitionException {
		PathElementContext _localctx = new PathElementContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_pathElement);
		try {
			setState(3204);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,421,_ctx) ) {
			case 1:
				_localctx = new QualifiedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3199);
				identifier();
				setState(3200);
				match(T__0);
				setState(3201);
				identifier();
				}
				break;
			case 2:
				_localctx = new UnqualifiedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3203);
				identifier();
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
	public static class PathSpecificationContext extends ParserRuleContext {
		public List<PathElementContext> pathElement() {
			return getRuleContexts(PathElementContext.class);
		}
		public PathElementContext pathElement(int i) {
			return getRuleContext(PathElementContext.class,i);
		}
		public PathSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPathSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPathSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPathSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathSpecificationContext pathSpecification() throws RecognitionException {
		PathSpecificationContext _localctx = new PathSpecificationContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_pathSpecification);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3206);
			pathElement();
			setState(3211);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(3207);
				match(T__3);
				setState(3208);
				pathElement();
				}
				}
				setState(3213);
				_errHandler.sync(this);
				_la = _input.LA(1);
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
	public static class FunctionSpecificationContext extends ParserRuleContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public FunctionDeclarationContext functionDeclaration() {
			return getRuleContext(FunctionDeclarationContext.class,0);
		}
		public ReturnsClauseContext returnsClause() {
			return getRuleContext(ReturnsClauseContext.class,0);
		}
		public ControlStatementContext controlStatement() {
			return getRuleContext(ControlStatementContext.class,0);
		}
		public List<RoutineCharacteristicContext> routineCharacteristic() {
			return getRuleContexts(RoutineCharacteristicContext.class);
		}
		public RoutineCharacteristicContext routineCharacteristic(int i) {
			return getRuleContext(RoutineCharacteristicContext.class,i);
		}
		public FunctionSpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionSpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionSpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionSpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionSpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionSpecificationContext functionSpecification() throws RecognitionException {
		FunctionSpecificationContext _localctx = new FunctionSpecificationContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_functionSpecification);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3214);
			match(FUNCTION);
			setState(3215);
			functionDeclaration();
			setState(3216);
			returnsClause();
			setState(3220);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,423,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(3217);
					routineCharacteristic();
					}
					} 
				}
				setState(3222);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,423,_ctx);
			}
			setState(3223);
			controlStatement();
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
	public static class FunctionDeclarationContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<ParameterDeclarationContext> parameterDeclaration() {
			return getRuleContexts(ParameterDeclarationContext.class);
		}
		public ParameterDeclarationContext parameterDeclaration(int i) {
			return getRuleContext(ParameterDeclarationContext.class,i);
		}
		public FunctionDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionDeclaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionDeclarationContext functionDeclaration() throws RecognitionException {
		FunctionDeclarationContext _localctx = new FunctionDeclarationContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_functionDeclaration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3225);
			qualifiedName();
			setState(3226);
			match(T__1);
			setState(3235);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514849801377L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
				{
				setState(3227);
				parameterDeclaration();
				setState(3232);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(3228);
					match(T__3);
					setState(3229);
					parameterDeclaration();
					}
					}
					setState(3234);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(3237);
			match(T__2);
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
	public static class ParameterDeclarationContext extends ParserRuleContext {
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ParameterDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameterDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParameterDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParameterDeclaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParameterDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParameterDeclarationContext parameterDeclaration() throws RecognitionException {
		ParameterDeclarationContext _localctx = new ParameterDeclarationContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_parameterDeclaration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3240);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,426,_ctx) ) {
			case 1:
				{
				setState(3239);
				identifier();
				}
				break;
			}
			setState(3242);
			type(0);
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
	public static class ReturnsClauseContext extends ParserRuleContext {
		public TerminalNode RETURNS() { return getToken(SqlBaseParser.RETURNS, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public ReturnsClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_returnsClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReturnsClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReturnsClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReturnsClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ReturnsClauseContext returnsClause() throws RecognitionException {
		ReturnsClauseContext _localctx = new ReturnsClauseContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_returnsClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3244);
			match(RETURNS);
			setState(3245);
			type(0);
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
	public static class RoutineCharacteristicContext extends ParserRuleContext {
		public RoutineCharacteristicContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_routineCharacteristic; }
	 
		public RoutineCharacteristicContext() { }
		public void copyFrom(RoutineCharacteristicContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReturnsNullOnNullInputCharacteristicContext extends RoutineCharacteristicContext {
		public TerminalNode RETURNS() { return getToken(SqlBaseParser.RETURNS, 0); }
		public List<TerminalNode> NULL() { return getTokens(SqlBaseParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(SqlBaseParser.NULL, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public ReturnsNullOnNullInputCharacteristicContext(RoutineCharacteristicContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReturnsNullOnNullInputCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReturnsNullOnNullInputCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReturnsNullOnNullInputCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SecurityCharacteristicContext extends RoutineCharacteristicContext {
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public SecurityCharacteristicContext(RoutineCharacteristicContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSecurityCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSecurityCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSecurityCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CalledOnNullInputCharacteristicContext extends RoutineCharacteristicContext {
		public TerminalNode CALLED() { return getToken(SqlBaseParser.CALLED, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public CalledOnNullInputCharacteristicContext(RoutineCharacteristicContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCalledOnNullInputCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCalledOnNullInputCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCalledOnNullInputCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentCharacteristicContext extends RoutineCharacteristicContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public CommentCharacteristicContext(RoutineCharacteristicContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCommentCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCommentCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCommentCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LanguageCharacteristicContext extends RoutineCharacteristicContext {
		public TerminalNode LANGUAGE() { return getToken(SqlBaseParser.LANGUAGE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public LanguageCharacteristicContext(RoutineCharacteristicContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLanguageCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLanguageCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLanguageCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeterministicCharacteristicContext extends RoutineCharacteristicContext {
		public TerminalNode DETERMINISTIC() { return getToken(SqlBaseParser.DETERMINISTIC, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DeterministicCharacteristicContext(RoutineCharacteristicContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDeterministicCharacteristic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDeterministicCharacteristic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDeterministicCharacteristic(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RoutineCharacteristicContext routineCharacteristic() throws RecognitionException {
		RoutineCharacteristicContext _localctx = new RoutineCharacteristicContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_routineCharacteristic);
		int _la;
		try {
			setState(3266);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LANGUAGE:
				_localctx = new LanguageCharacteristicContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3247);
				match(LANGUAGE);
				setState(3248);
				identifier();
				}
				break;
			case DETERMINISTIC:
			case NOT:
				_localctx = new DeterministicCharacteristicContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3250);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(3249);
					match(NOT);
					}
				}

				setState(3252);
				match(DETERMINISTIC);
				}
				break;
			case RETURNS:
				_localctx = new ReturnsNullOnNullInputCharacteristicContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3253);
				match(RETURNS);
				setState(3254);
				match(NULL);
				setState(3255);
				match(ON);
				setState(3256);
				match(NULL);
				setState(3257);
				match(INPUT);
				}
				break;
			case CALLED:
				_localctx = new CalledOnNullInputCharacteristicContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3258);
				match(CALLED);
				setState(3259);
				match(ON);
				setState(3260);
				match(NULL);
				setState(3261);
				match(INPUT);
				}
				break;
			case SECURITY:
				_localctx = new SecurityCharacteristicContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3262);
				match(SECURITY);
				setState(3263);
				_la = _input.LA(1);
				if ( !(_la==DEFINER || _la==INVOKER) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case COMMENT:
				_localctx = new CommentCharacteristicContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(3264);
				match(COMMENT);
				setState(3265);
				string();
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
	public static class ControlStatementContext extends ParserRuleContext {
		public ControlStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_controlStatement; }
	 
		public ControlStatementContext() { }
		public void copyFrom(ControlStatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WhileStatementContext extends ControlStatementContext {
		public IdentifierContext label;
		public List<TerminalNode> WHILE() { return getTokens(SqlBaseParser.WHILE); }
		public TerminalNode WHILE(int i) {
			return getToken(SqlBaseParser.WHILE, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode DO() { return getToken(SqlBaseParser.DO, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WhileStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhileStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhileStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhileStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseStatementContext extends ControlStatementContext {
		public List<TerminalNode> CASE() { return getTokens(SqlBaseParser.CASE); }
		public TerminalNode CASE(int i) {
			return getToken(SqlBaseParser.CASE, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<CaseStatementWhenClauseContext> caseStatementWhenClause() {
			return getRuleContexts(CaseStatementWhenClauseContext.class);
		}
		public CaseStatementWhenClauseContext caseStatementWhenClause(int i) {
			return getRuleContext(CaseStatementWhenClauseContext.class,i);
		}
		public ElseClauseContext elseClause() {
			return getRuleContext(ElseClauseContext.class,0);
		}
		public SimpleCaseStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCaseStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCaseStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCaseStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepeatStatementContext extends ControlStatementContext {
		public IdentifierContext label;
		public List<TerminalNode> REPEAT() { return getTokens(SqlBaseParser.REPEAT); }
		public TerminalNode REPEAT(int i) {
			return getToken(SqlBaseParser.REPEAT, i);
		}
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode UNTIL() { return getToken(SqlBaseParser.UNTIL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RepeatStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepeatStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepeatStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepeatStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AssignmentStatementContext extends ControlStatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public AssignmentStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAssignmentStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAssignmentStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAssignmentStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LeaveStatementContext extends ControlStatementContext {
		public TerminalNode LEAVE() { return getToken(SqlBaseParser.LEAVE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public LeaveStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLeaveStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLeaveStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLeaveStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CompoundStatementContext extends ControlStatementContext {
		public TerminalNode BEGIN() { return getToken(SqlBaseParser.BEGIN, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<VariableDeclarationContext> variableDeclaration() {
			return getRuleContexts(VariableDeclarationContext.class);
		}
		public VariableDeclarationContext variableDeclaration(int i) {
			return getRuleContext(VariableDeclarationContext.class,i);
		}
		public List<TerminalNode> SEMICOLON() { return getTokens(SqlBaseParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(SqlBaseParser.SEMICOLON, i);
		}
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public CompoundStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCompoundStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCompoundStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCompoundStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IterateStatementContext extends ControlStatementContext {
		public TerminalNode ITERATE() { return getToken(SqlBaseParser.ITERATE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IterateStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIterateStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIterateStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIterateStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LoopStatementContext extends ControlStatementContext {
		public IdentifierContext label;
		public List<TerminalNode> LOOP() { return getTokens(SqlBaseParser.LOOP); }
		public TerminalNode LOOP(int i) {
			return getToken(SqlBaseParser.LOOP, i);
		}
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public LoopStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLoopStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLoopStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLoopStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReturnStatementContext extends ControlStatementContext {
		public TerminalNode RETURN() { return getToken(SqlBaseParser.RETURN, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ReturnStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReturnStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReturnStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReturnStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IfStatementContext extends ControlStatementContext {
		public List<TerminalNode> IF() { return getTokens(SqlBaseParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(SqlBaseParser.IF, i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ElseIfClauseContext> elseIfClause() {
			return getRuleContexts(ElseIfClauseContext.class);
		}
		public ElseIfClauseContext elseIfClause(int i) {
			return getRuleContext(ElseIfClauseContext.class,i);
		}
		public ElseClauseContext elseClause() {
			return getRuleContext(ElseClauseContext.class,0);
		}
		public IfStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIfStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIfStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIfStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseStatementContext extends ControlStatementContext {
		public List<TerminalNode> CASE() { return getTokens(SqlBaseParser.CASE); }
		public TerminalNode CASE(int i) {
			return getToken(SqlBaseParser.CASE, i);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<CaseStatementWhenClauseContext> caseStatementWhenClause() {
			return getRuleContexts(CaseStatementWhenClauseContext.class);
		}
		public CaseStatementWhenClauseContext caseStatementWhenClause(int i) {
			return getRuleContext(CaseStatementWhenClauseContext.class,i);
		}
		public ElseClauseContext elseClause() {
			return getRuleContext(ElseClauseContext.class,0);
		}
		public SearchedCaseStatementContext(ControlStatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCaseStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCaseStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCaseStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ControlStatementContext controlStatement() throws RecognitionException {
		ControlStatementContext _localctx = new ControlStatementContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_controlStatement);
		int _la;
		try {
			int _alt;
			setState(3367);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,440,_ctx) ) {
			case 1:
				_localctx = new ReturnStatementContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3268);
				match(RETURN);
				setState(3269);
				valueExpression(0);
				}
				break;
			case 2:
				_localctx = new AssignmentStatementContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3270);
				match(SET);
				setState(3271);
				identifier();
				setState(3272);
				match(EQ);
				setState(3273);
				expression();
				}
				break;
			case 3:
				_localctx = new SimpleCaseStatementContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3275);
				match(CASE);
				setState(3276);
				expression();
				setState(3278); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(3277);
					caseStatementWhenClause();
					}
					}
					setState(3280); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(3283);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(3282);
					elseClause();
					}
				}

				setState(3285);
				match(END);
				setState(3286);
				match(CASE);
				}
				break;
			case 4:
				_localctx = new SearchedCaseStatementContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3288);
				match(CASE);
				setState(3290); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(3289);
					caseStatementWhenClause();
					}
					}
					setState(3292); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(3295);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(3294);
					elseClause();
					}
				}

				setState(3297);
				match(END);
				setState(3298);
				match(CASE);
				}
				break;
			case 5:
				_localctx = new IfStatementContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3300);
				match(IF);
				setState(3301);
				expression();
				setState(3302);
				match(THEN);
				setState(3303);
				sqlStatementList();
				setState(3307);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==ELSEIF) {
					{
					{
					setState(3304);
					elseIfClause();
					}
					}
					setState(3309);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(3311);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(3310);
					elseClause();
					}
				}

				setState(3313);
				match(END);
				setState(3314);
				match(IF);
				}
				break;
			case 6:
				_localctx = new IterateStatementContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(3316);
				match(ITERATE);
				setState(3317);
				identifier();
				}
				break;
			case 7:
				_localctx = new LeaveStatementContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(3318);
				match(LEAVE);
				setState(3319);
				identifier();
				}
				break;
			case 8:
				_localctx = new CompoundStatementContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(3320);
				match(BEGIN);
				setState(3326);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,435,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(3321);
						variableDeclaration();
						setState(3322);
						match(SEMICOLON);
						}
						} 
					}
					setState(3328);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,435,_ctx);
				}
				setState(3330);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 18)) & ~0x3f) == 0 && ((1L << (_la - 18)) & -2631368514845607073L) != 0) || ((((_la - 82)) & ~0x3f) == 0 && ((1L << (_la - 82)) & -4560291593682213703L) != 0) || ((((_la - 146)) & ~0x3f) == 0 && ((1L << (_la - 146)) & -3114057515152705L) != 0) || ((((_la - 210)) & ~0x3f) == 0 && ((1L << (_la - 210)) & 4574531046338459631L) != 0) || ((((_la - 274)) & ~0x3f) == 0 && ((1L << (_la - 274)) & 4323455915874252663L) != 0)) {
					{
					setState(3329);
					sqlStatementList();
					}
				}

				setState(3332);
				match(END);
				}
				break;
			case 9:
				_localctx = new LoopStatementContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(3336);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,437,_ctx) ) {
				case 1:
					{
					setState(3333);
					((LoopStatementContext)_localctx).label = identifier();
					setState(3334);
					match(T__9);
					}
					break;
				}
				setState(3338);
				match(LOOP);
				setState(3339);
				sqlStatementList();
				setState(3340);
				match(END);
				setState(3341);
				match(LOOP);
				}
				break;
			case 10:
				_localctx = new WhileStatementContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(3346);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,438,_ctx) ) {
				case 1:
					{
					setState(3343);
					((WhileStatementContext)_localctx).label = identifier();
					setState(3344);
					match(T__9);
					}
					break;
				}
				setState(3348);
				match(WHILE);
				setState(3349);
				expression();
				setState(3350);
				match(DO);
				setState(3351);
				sqlStatementList();
				setState(3352);
				match(END);
				setState(3353);
				match(WHILE);
				}
				break;
			case 11:
				_localctx = new RepeatStatementContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(3358);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,439,_ctx) ) {
				case 1:
					{
					setState(3355);
					((RepeatStatementContext)_localctx).label = identifier();
					setState(3356);
					match(T__9);
					}
					break;
				}
				setState(3360);
				match(REPEAT);
				setState(3361);
				sqlStatementList();
				setState(3362);
				match(UNTIL);
				setState(3363);
				expression();
				setState(3364);
				match(END);
				setState(3365);
				match(REPEAT);
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
	public static class CaseStatementWhenClauseContext extends ParserRuleContext {
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public CaseStatementWhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseStatementWhenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCaseStatementWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCaseStatementWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCaseStatementWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseStatementWhenClauseContext caseStatementWhenClause() throws RecognitionException {
		CaseStatementWhenClauseContext _localctx = new CaseStatementWhenClauseContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_caseStatementWhenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3369);
			match(WHEN);
			setState(3370);
			expression();
			setState(3371);
			match(THEN);
			setState(3372);
			sqlStatementList();
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
	public static class ElseIfClauseContext extends ParserRuleContext {
		public TerminalNode ELSEIF() { return getToken(SqlBaseParser.ELSEIF, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public ElseIfClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_elseIfClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterElseIfClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitElseIfClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitElseIfClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ElseIfClauseContext elseIfClause() throws RecognitionException {
		ElseIfClauseContext _localctx = new ElseIfClauseContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_elseIfClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3374);
			match(ELSEIF);
			setState(3375);
			expression();
			setState(3376);
			match(THEN);
			setState(3377);
			sqlStatementList();
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
	public static class ElseClauseContext extends ParserRuleContext {
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SqlStatementListContext sqlStatementList() {
			return getRuleContext(SqlStatementListContext.class,0);
		}
		public ElseClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_elseClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterElseClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitElseClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitElseClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ElseClauseContext elseClause() throws RecognitionException {
		ElseClauseContext _localctx = new ElseClauseContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_elseClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3379);
			match(ELSE);
			setState(3380);
			sqlStatementList();
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
	public static class VariableDeclarationContext extends ParserRuleContext {
		public TerminalNode DECLARE() { return getToken(SqlBaseParser.DECLARE, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public VariableDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variableDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterVariableDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitVariableDeclaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitVariableDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableDeclarationContext variableDeclaration() throws RecognitionException {
		VariableDeclarationContext _localctx = new VariableDeclarationContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_variableDeclaration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3382);
			match(DECLARE);
			setState(3383);
			identifier();
			setState(3388);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(3384);
				match(T__3);
				setState(3385);
				identifier();
				}
				}
				setState(3390);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(3391);
			type(0);
			setState(3394);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT) {
				{
				setState(3392);
				match(DEFAULT);
				setState(3393);
				valueExpression(0);
				}
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
	public static class SqlStatementListContext extends ParserRuleContext {
		public List<ControlStatementContext> controlStatement() {
			return getRuleContexts(ControlStatementContext.class);
		}
		public ControlStatementContext controlStatement(int i) {
			return getRuleContext(ControlStatementContext.class,i);
		}
		public List<TerminalNode> SEMICOLON() { return getTokens(SqlBaseParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(SqlBaseParser.SEMICOLON, i);
		}
		public SqlStatementListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sqlStatementList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSqlStatementList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSqlStatementList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSqlStatementList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SqlStatementListContext sqlStatementList() throws RecognitionException {
		SqlStatementListContext _localctx = new SqlStatementListContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_sqlStatementList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3399); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(3396);
					controlStatement();
					setState(3397);
					match(SEMICOLON);
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(3401); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,443,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
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
	public static class PrivilegeContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PrivilegeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilege; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrivilege(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrivilege(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrivilege(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrivilegeContext privilege() throws RecognitionException {
		PrivilegeContext _localctx = new PrivilegeContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_privilege);
		try {
			setState(3409);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,444,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3403);
				match(CREATE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3404);
				match(SELECT);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3405);
				match(DELETE);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(3406);
				match(INSERT);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(3407);
				match(UPDATE);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(3408);
				identifier();
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
	public static class EntityKindContext extends ParserRuleContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public EntityKindContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_entityKind; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterEntityKind(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitEntityKind(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitEntityKind(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EntityKindContext entityKind() throws RecognitionException {
		EntityKindContext _localctx = new EntityKindContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_entityKind);
		try {
			setState(3414);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,445,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3411);
				match(TABLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3412);
				match(SCHEMA);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3413);
				identifier();
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
	public static class GrantObjectContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public EntityKindContext entityKind() {
			return getRuleContext(EntityKindContext.class,0);
		}
		public GrantObjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantObject; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGrantObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGrantObject(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGrantObject(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GrantObjectContext grantObject() throws RecognitionException {
		GrantObjectContext _localctx = new GrantObjectContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_grantObject);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3417);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,446,_ctx) ) {
			case 1:
				{
				setState(3416);
				entityKind();
				}
				break;
			}
			setState(3419);
			qualifiedName();
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
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3421);
			identifier();
			setState(3426);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,447,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(3422);
					match(T__0);
					setState(3423);
					identifier();
					}
					} 
				}
				setState(3428);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,447,_ctx);
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
	public static class QueryPeriodContext extends ParserRuleContext {
		public ValueExpressionContext end;
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public QueryPeriodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPeriod; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPeriod(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPeriod(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPeriod(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPeriodContext queryPeriod() throws RecognitionException {
		QueryPeriodContext _localctx = new QueryPeriodContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_queryPeriod);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3429);
			match(FOR);
			setState(3430);
			rangeType();
			setState(3431);
			match(AS);
			setState(3432);
			match(OF);
			setState(3433);
			((QueryPeriodContext)_localctx).end = valueExpression(0);
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
	public static class RangeTypeContext extends ParserRuleContext {
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public RangeTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRangeType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRangeType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRangeType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeTypeContext rangeType() throws RecognitionException {
		RangeTypeContext _localctx = new RangeTypeContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_rangeType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3435);
			_la = _input.LA(1);
			if ( !(_la==TIMESTAMP || _la==VERSION) ) {
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
	public static class GrantorContext extends ParserRuleContext {
		public GrantorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantor; }
	 
		public GrantorContext() { }
		public void copyFrom(GrantorContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentUserGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentUserGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentUserGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentUserGrantor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentUserGrantor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SpecifiedPrincipalContext extends GrantorContext {
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public SpecifiedPrincipalContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpecifiedPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpecifiedPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentRoleGrantorContext extends GrantorContext {
		public TerminalNode CURRENT_ROLE() { return getToken(SqlBaseParser.CURRENT_ROLE, 0); }
		public CurrentRoleGrantorContext(GrantorContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRoleGrantor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRoleGrantor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRoleGrantor(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GrantorContext grantor() throws RecognitionException {
		GrantorContext _localctx = new GrantorContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_grantor);
		try {
			setState(3440);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new SpecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3437);
				principal();
				}
				break;
			case CURRENT_USER:
				_localctx = new CurrentUserGrantorContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3438);
				match(CURRENT_USER);
				}
				break;
			case CURRENT_ROLE:
				_localctx = new CurrentRoleGrantorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3439);
				match(CURRENT_ROLE);
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
	public static class PrincipalContext extends ParserRuleContext {
		public PrincipalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_principal; }
	 
		public PrincipalContext() { }
		public void copyFrom(PrincipalContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnspecifiedPrincipalContext extends PrincipalContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UnspecifiedPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnspecifiedPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnspecifiedPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnspecifiedPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UserPrincipalContext extends PrincipalContext {
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UserPrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUserPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUserPrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUserPrincipal(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RolePrincipalContext extends PrincipalContext {
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public RolePrincipalContext(PrincipalContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRolePrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRolePrincipal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRolePrincipal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrincipalContext principal() throws RecognitionException {
		PrincipalContext _localctx = new PrincipalContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_principal);
		try {
			setState(3447);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,449,_ctx) ) {
			case 1:
				_localctx = new UnspecifiedPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3442);
				identifier();
				}
				break;
			case 2:
				_localctx = new UserPrincipalContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3443);
				match(USER);
				setState(3444);
				identifier();
				}
				break;
			case 3:
				_localctx = new RolePrincipalContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3445);
				match(ROLE);
				setState(3446);
				identifier();
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
	public static class RolesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public RolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRoles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRoles(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRoles(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RolesContext roles() throws RecognitionException {
		RolesContext _localctx = new RolesContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_roles);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3449);
			identifier();
			setState(3454);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(3450);
				match(T__3);
				setState(3451);
				identifier();
				}
				}
				setState(3456);
				_errHandler.sync(this);
				_la = _input.LA(1);
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
	public static class PrivilegeOrRoleContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PrivilegeOrRoleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilegeOrRole; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrivilegeOrRole(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrivilegeOrRole(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrivilegeOrRole(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrivilegeOrRoleContext privilegeOrRole() throws RecognitionException {
		PrivilegeOrRoleContext _localctx = new PrivilegeOrRoleContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_privilegeOrRole);
		try {
			setState(3463);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,451,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3457);
				match(CREATE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3458);
				match(SELECT);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3459);
				match(DELETE);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(3460);
				match(INSERT);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(3461);
				match(UPDATE);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(3462);
				identifier();
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
	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BackQuotedIdentifierContext extends IdentifierContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public BackQuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBackQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBackQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBackQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends IdentifierContext {
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(SqlBaseParser.QUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DigitIdentifierContext extends IdentifierContext {
		public TerminalNode DIGIT_IDENTIFIER() { return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0); }
		public DigitIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDigitIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDigitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDigitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_identifier);
		try {
			setState(3470);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3465);
				match(IDENTIFIER);
				}
				break;
			case QUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3466);
				match(QUOTED_IDENTIFIER);
				}
				break;
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3467);
				nonReserved();
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new BackQuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3468);
				match(BACKQUOTED_IDENTIFIER);
				}
				break;
			case DIGIT_IDENTIFIER:
				_localctx = new DigitIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3469);
				match(DIGIT_IDENTIFIER);
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
	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_VALUE() { return getToken(SqlBaseParser.DOUBLE_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_number);
		int _la;
		try {
			setState(3484);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,456,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3473);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3472);
					match(MINUS);
					}
				}

				setState(3475);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3477);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3476);
					match(MINUS);
					}
				}

				setState(3479);
				match(DOUBLE_VALUE);
				}
				break;
			case 3:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3481);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3480);
					match(MINUS);
					}
				}

				setState(3483);
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
	public static class AuthorizationUserContext extends ParserRuleContext {
		public AuthorizationUserContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_authorizationUser; }
	 
		public AuthorizationUserContext() { }
		public void copyFrom(AuthorizationUserContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringUserContext extends AuthorizationUserContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public StringUserContext(AuthorizationUserContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringUser(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringUser(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierUserContext extends AuthorizationUserContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierUserContext(AuthorizationUserContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierUser(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierUser(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierUser(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AuthorizationUserContext authorizationUser() throws RecognitionException {
		AuthorizationUserContext _localctx = new AuthorizationUserContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_authorizationUser);
		try {
			setState(3488);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ABSENT:
			case ADD:
			case ADMIN:
			case AFTER:
			case ALL:
			case ANALYZE:
			case ANY:
			case ARRAY:
			case ASC:
			case AT:
			case AUTHORIZATION:
			case BEGIN:
			case BERNOULLI:
			case BOTH:
			case CALL:
			case CALLED:
			case CASCADE:
			case CATALOG:
			case CATALOGS:
			case COLUMN:
			case COLUMNS:
			case COMMENT:
			case COMMIT:
			case COMMITTED:
			case CONDITIONAL:
			case COUNT:
			case COPARTITION:
			case CURRENT:
			case DATA:
			case DATE:
			case DAY:
			case DECLARE:
			case DEFAULT:
			case DEFINE:
			case DEFINER:
			case DENY:
			case DESC:
			case DESCRIPTOR:
			case DETERMINISTIC:
			case DISTRIBUTED:
			case DO:
			case DOUBLE:
			case EMPTY:
			case ELSEIF:
			case ENCODING:
			case ERROR:
			case EXCLUDING:
			case EXPLAIN:
			case FETCH:
			case FILTER:
			case FINAL:
			case FIRST:
			case FOLLOWING:
			case FORMAT:
			case FUNCTION:
			case FUNCTIONS:
			case GRACE:
			case GRANT:
			case GRANTED:
			case GRANTS:
			case GRAPHVIZ:
			case GROUPS:
			case HOUR:
			case IF:
			case IGNORE:
			case IMMEDIATE:
			case INCLUDING:
			case INITIAL:
			case INPUT:
			case INTERVAL:
			case INVOKER:
			case IO:
			case ISOLATION:
			case ITERATE:
			case JSON:
			case KEEP:
			case KEY:
			case KEYS:
			case LANGUAGE:
			case LAST:
			case LATERAL:
			case LEADING:
			case LEAVE:
			case LEVEL:
			case LIMIT:
			case LOCAL:
			case LOGICAL:
			case LOOP:
			case MAP:
			case MATCH:
			case MATCHED:
			case MATCHES:
			case MATCH_RECOGNIZE:
			case MATERIALIZED:
			case MEASURES:
			case MERGE:
			case MINUTE:
			case MONTH:
			case NESTED:
			case NEXT:
			case NFC:
			case NFD:
			case NFKC:
			case NFKD:
			case NO:
			case NONE:
			case NULLIF:
			case NULLS:
			case OBJECT:
			case OF:
			case OFFSET:
			case OMIT:
			case ONE:
			case ONLY:
			case OPTION:
			case ORDINALITY:
			case OUTPUT:
			case OVER:
			case OVERFLOW:
			case PARTITION:
			case PARTITIONS:
			case PASSING:
			case PAST:
			case PATH:
			case PATTERN:
			case PER:
			case PERIOD:
			case PERMUTE:
			case PLAN:
			case POSITION:
			case PRECEDING:
			case PRECISION:
			case PRIVILEGES:
			case PROPERTIES:
			case PRUNE:
			case QUOTES:
			case RANGE:
			case READ:
			case REFRESH:
			case RENAME:
			case REPEAT:
			case REPEATABLE:
			case REPLACE:
			case RESET:
			case RESPECT:
			case RESTRICT:
			case RETURN:
			case RETURNING:
			case RETURNS:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROLLBACK:
			case ROW:
			case ROWS:
			case RUNNING:
			case SCALAR:
			case SCHEMA:
			case SCHEMAS:
			case SECOND:
			case SECURITY:
			case SEEK:
			case SERIALIZABLE:
			case SESSION:
			case SET:
			case SETS:
			case SHOW:
			case SOME:
			case START:
			case STATS:
			case SUBSET:
			case SUBSTRING:
			case SYSTEM:
			case TABLES:
			case TABLESAMPLE:
			case TEXT:
			case TEXT_STRING:
			case TIES:
			case TIME:
			case TIMESTAMP:
			case TO:
			case TRAILING:
			case TRANSACTION:
			case TRUNCATE:
			case TRY_CAST:
			case TYPE:
			case UNBOUNDED:
			case UNCOMMITTED:
			case UNCONDITIONAL:
			case UNIQUE:
			case UNKNOWN:
			case UNMATCHED:
			case UNTIL:
			case UPDATE:
			case USE:
			case USER:
			case UTF16:
			case UTF32:
			case UTF8:
			case VALIDATE:
			case VALUE:
			case VERBOSE:
			case VERSION:
			case VIEW:
			case WHILE:
			case WINDOW:
			case WITHIN:
			case WITHOUT:
			case WORK:
			case WRAPPER:
			case WRITE:
			case YEAR:
			case ZONE:
			case IDENTIFIER:
			case DIGIT_IDENTIFIER:
			case QUOTED_IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new IdentifierUserContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3486);
				identifier();
				}
				break;
			case STRING:
			case UNICODE_STRING:
				_localctx = new StringUserContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3487);
				string();
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
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ABSENT() { return getToken(SqlBaseParser.ABSENT, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode ADMIN() { return getToken(SqlBaseParser.ADMIN, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public TerminalNode BEGIN() { return getToken(SqlBaseParser.BEGIN, 0); }
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public TerminalNode CALLED() { return getToken(SqlBaseParser.CALLED, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public TerminalNode CONDITIONAL() { return getToken(SqlBaseParser.CONDITIONAL, 0); }
		public TerminalNode COPARTITION() { return getToken(SqlBaseParser.COPARTITION, 0); }
		public TerminalNode COUNT() { return getToken(SqlBaseParser.COUNT, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATE() { return getToken(SqlBaseParser.DATE, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DECLARE() { return getToken(SqlBaseParser.DECLARE, 0); }
		public TerminalNode DEFAULT() { return getToken(SqlBaseParser.DEFAULT, 0); }
		public TerminalNode DEFINE() { return getToken(SqlBaseParser.DEFINE, 0); }
		public TerminalNode DEFINER() { return getToken(SqlBaseParser.DEFINER, 0); }
		public TerminalNode DENY() { return getToken(SqlBaseParser.DENY, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIPTOR() { return getToken(SqlBaseParser.DESCRIPTOR, 0); }
		public TerminalNode DETERMINISTIC() { return getToken(SqlBaseParser.DETERMINISTIC, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode DO() { return getToken(SqlBaseParser.DO, 0); }
		public TerminalNode DOUBLE() { return getToken(SqlBaseParser.DOUBLE, 0); }
		public TerminalNode ELSEIF() { return getToken(SqlBaseParser.ELSEIF, 0); }
		public TerminalNode EMPTY() { return getToken(SqlBaseParser.EMPTY, 0); }
		public TerminalNode ENCODING() { return getToken(SqlBaseParser.ENCODING, 0); }
		public TerminalNode ERROR() { return getToken(SqlBaseParser.ERROR, 0); }
		public TerminalNode EXCLUDING() { return getToken(SqlBaseParser.EXCLUDING, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode FINAL() { return getToken(SqlBaseParser.FINAL, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GRACE() { return getToken(SqlBaseParser.GRACE, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GRANTED() { return getToken(SqlBaseParser.GRANTED, 0); }
		public TerminalNode GRANTS() { return getToken(SqlBaseParser.GRANTS, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode GROUPS() { return getToken(SqlBaseParser.GROUPS, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMMEDIATE() { return getToken(SqlBaseParser.IMMEDIATE, 0); }
		public TerminalNode INCLUDING() { return getToken(SqlBaseParser.INCLUDING, 0); }
		public TerminalNode INITIAL() { return getToken(SqlBaseParser.INITIAL, 0); }
		public TerminalNode INPUT() { return getToken(SqlBaseParser.INPUT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode INVOKER() { return getToken(SqlBaseParser.INVOKER, 0); }
		public TerminalNode IO() { return getToken(SqlBaseParser.IO, 0); }
		public TerminalNode ITERATE() { return getToken(SqlBaseParser.ITERATE, 0); }
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode JSON() { return getToken(SqlBaseParser.JSON, 0); }
		public TerminalNode KEEP() { return getToken(SqlBaseParser.KEEP, 0); }
		public TerminalNode KEY() { return getToken(SqlBaseParser.KEY, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LANGUAGE() { return getToken(SqlBaseParser.LANGUAGE, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode LEAVE() { return getToken(SqlBaseParser.LEAVE, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode LOOP() { return getToken(SqlBaseParser.LOOP, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATCH() { return getToken(SqlBaseParser.MATCH, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode MATCHES() { return getToken(SqlBaseParser.MATCHES, 0); }
		public TerminalNode MATCH_RECOGNIZE() { return getToken(SqlBaseParser.MATCH_RECOGNIZE, 0); }
		public TerminalNode MATERIALIZED() { return getToken(SqlBaseParser.MATERIALIZED, 0); }
		public TerminalNode MEASURES() { return getToken(SqlBaseParser.MEASURES, 0); }
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode NESTED() { return getToken(SqlBaseParser.NESTED, 0); }
		public TerminalNode NEXT() { return getToken(SqlBaseParser.NEXT, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NONE() { return getToken(SqlBaseParser.NONE, 0); }
		public TerminalNode NULLIF() { return getToken(SqlBaseParser.NULLIF, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OBJECT() { return getToken(SqlBaseParser.OBJECT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode OFFSET() { return getToken(SqlBaseParser.OFFSET, 0); }
		public TerminalNode OMIT() { return getToken(SqlBaseParser.OMIT, 0); }
		public TerminalNode ONE() { return getToken(SqlBaseParser.ONE, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public TerminalNode OUTPUT() { return getToken(SqlBaseParser.OUTPUT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERFLOW() { return getToken(SqlBaseParser.OVERFLOW, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PASSING() { return getToken(SqlBaseParser.PASSING, 0); }
		public TerminalNode PAST() { return getToken(SqlBaseParser.PAST, 0); }
		public TerminalNode PATH() { return getToken(SqlBaseParser.PATH, 0); }
		public TerminalNode PATTERN() { return getToken(SqlBaseParser.PATTERN, 0); }
		public TerminalNode PER() { return getToken(SqlBaseParser.PER, 0); }
		public TerminalNode PERIOD() { return getToken(SqlBaseParser.PERIOD, 0); }
		public TerminalNode PERMUTE() { return getToken(SqlBaseParser.PERMUTE, 0); }
		public TerminalNode PLAN() { return getToken(SqlBaseParser.PLAN, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRECISION() { return getToken(SqlBaseParser.PRECISION, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PRUNE() { return getToken(SqlBaseParser.PRUNE, 0); }
		public TerminalNode QUOTES() { return getToken(SqlBaseParser.QUOTES, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPEAT() { return getToken(SqlBaseParser.REPEAT, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode RETURN() { return getToken(SqlBaseParser.RETURN, 0); }
		public TerminalNode RETURNING() { return getToken(SqlBaseParser.RETURNING, 0); }
		public TerminalNode RETURNS() { return getToken(SqlBaseParser.RETURNS, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode RUNNING() { return getToken(SqlBaseParser.RUNNING, 0); }
		public TerminalNode SCALAR() { return getToken(SqlBaseParser.SCALAR, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SECURITY() { return getToken(SqlBaseParser.SECURITY, 0); }
		public TerminalNode SEEK() { return getToken(SqlBaseParser.SEEK, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATS() { return getToken(SqlBaseParser.STATS, 0); }
		public TerminalNode SUBSET() { return getToken(SqlBaseParser.SUBSET, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode TEXT_STRING() { return getToken(SqlBaseParser.TEXT_STRING, 0); }
		public TerminalNode TIES() { return getToken(SqlBaseParser.TIES, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public TerminalNode UNCONDITIONAL() { return getToken(SqlBaseParser.UNCONDITIONAL, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode UNMATCHED() { return getToken(SqlBaseParser.UNMATCHED, 0); }
		public TerminalNode UNTIL() { return getToken(SqlBaseParser.UNTIL, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public TerminalNode UTF16() { return getToken(SqlBaseParser.UTF16, 0); }
		public TerminalNode UTF32() { return getToken(SqlBaseParser.UTF32, 0); }
		public TerminalNode UTF8() { return getToken(SqlBaseParser.UTF8, 0); }
		public TerminalNode VALIDATE() { return getToken(SqlBaseParser.VALIDATE, 0); }
		public TerminalNode VALUE() { return getToken(SqlBaseParser.VALUE, 0); }
		public TerminalNode VERBOSE() { return getToken(SqlBaseParser.VERBOSE, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode WHILE() { return getToken(SqlBaseParser.WHILE, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode WITHIN() { return getToken(SqlBaseParser.WITHIN, 0); }
		public TerminalNode WITHOUT() { return getToken(SqlBaseParser.WITHOUT, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public TerminalNode WRAPPER() { return getToken(SqlBaseParser.WRAPPER, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3490);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 79935508640956416L) != 0) || ((((_la - 65)) & ~0x3f) == 0 && ((1L << (_la - 65)) & 3308453295486646007L) != 0) || ((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & -2337377004485115539L) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & -36066180684840983L) != 0) || ((((_la - 257)) & ~0x3f) == 0 && ((1L << (_la - 257)) & 35861112619433719L) != 0)) ) {
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
		case 21:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 33:
			return relation_sempred((RelationContext)_localctx, predIndex);
		case 64:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 66:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 67:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 87:
			return type_sempred((TypeContext)_localctx, predIndex);
		case 97:
			return rowPattern_sempred((RowPatternContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean relation_sempred(RelationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 2);
		case 4:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 5:
			return precpred(_ctx, 3);
		case 6:
			return precpred(_ctx, 2);
		case 7:
			return precpred(_ctx, 1);
		case 8:
			return precpred(_ctx, 5);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 9:
			return precpred(_ctx, 24);
		case 10:
			return precpred(_ctx, 22);
		}
		return true;
	}
	private boolean type_sempred(TypeContext _localctx, int predIndex) {
		switch (predIndex) {
		case 11:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean rowPattern_sempred(RowPatternContext _localctx, int predIndex) {
		switch (predIndex) {
		case 12:
			return precpred(_ctx, 2);
		case 13:
			return precpred(_ctx, 1);
		}
		return true;
	}

	private static final String _serializedATNSegment0 =
		"\u0004\u0001\u0154\u0da5\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007"+
		"r\u0002s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0002w\u0007"+
		"w\u0002x\u0007x\u0002y\u0007y\u0002z\u0007z\u0002{\u0007{\u0002|\u0007"+
		"|\u0002}\u0007}\u0002~\u0007~\u0002\u007f\u0007\u007f\u0002\u0080\u0007"+
		"\u0080\u0002\u0081\u0007\u0081\u0002\u0082\u0007\u0082\u0002\u0083\u0007"+
		"\u0083\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u0128\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u012f\b\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u0133\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0137"+
		"\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u013d"+
		"\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0141\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0148\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u014d\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0151\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0157\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u015b\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u016e\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u0174\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0178\b"+
		"\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u017c\b\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u0180\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0188\b\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u018c\b\u0006\u0001\u0006\u0003\u0006\u018f\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0194\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u019a\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u01a1\b\u0006"+
		"\n\u0006\f\u0006\u01a4\t\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003"+
		"\u0006\u01a9\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u01ad\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u01b3\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u01ba\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u01c3\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u01cf\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u01d8\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u01e1\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u01e7\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u01f2\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u01fa\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u0202\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u0209\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0213\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u021a\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0222\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0231\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0005\u0006\u0252\b\u0006\n\u0006\f\u0006\u0255\t\u0006\u0003"+
		"\u0006\u0257\b\u0006\u0001\u0006\u0003\u0006\u025a\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u025e\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0264\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u0269\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0270\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0276\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u027a\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u027e\b\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003"+
		"\u0006\u0286\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003"+
		"\u0006\u028c\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0290\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u029e\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u02a6\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u02b9\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0005\u0006\u02d0\b\u0006\n\u0006\f\u0006\u02d3"+
		"\t\u0006\u0003\u0006\u02d5\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u02dc\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u02e3\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u02ec\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u02f0\b"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003"+
		"\u0006\u02f7\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005"+
		"\u0006\u02fd\b\u0006\n\u0006\f\u0006\u0300\t\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0005\u0006\u0306\b\u0006\n\u0006\f\u0006\u0309"+
		"\t\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u030e\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0313\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0317\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0005\u0006\u031d\b\u0006\n\u0006\f\u0006\u0320\t\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u0324\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u032d"+
		"\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0333"+
		"\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u0338\b\u0006"+
		"\n\u0006\f\u0006\u033b\t\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0005\u0006\u0341\b\u0006\n\u0006\f\u0006\u0344\t\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u0349\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u034d\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u0353\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006"+
		"\u0358\b\u0006\n\u0006\f\u0006\u035b\t\u0006\u0001\u0006\u0001\u0006\u0003"+
		"\u0006\u035f\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u036a"+
		"\b\u0006\n\u0006\f\u0006\u036d\t\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u0371\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u037d\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0381\b\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0387\b\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u038e"+
		"\b\u0006\n\u0006\f\u0006\u0391\t\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u0395\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u039b\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u03b3\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u03b9\b\u0006\u0003\u0006\u03bb\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u03c1\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u03c7\b\u0006\u0003\u0006\u03c9\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u03d1\b\u0006\u0003\u0006\u03d3\b\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u03dd\b\u0006\u0003\u0006\u03df\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u03ee\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u03f3\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u03fa\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0404\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u040a\b\u0006"+
		"\u0003\u0006\u040c\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u0414\b\u0006\u0003\u0006\u0416\b"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u042d\b\u0006\n"+
		"\u0006\f\u0006\u0430\t\u0006\u0003\u0006\u0432\b\u0006\u0001\u0006\u0001"+
		"\u0006\u0003\u0006\u0436\b\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u043a"+
		"\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u044a\b\u0006\n\u0006\f\u0006"+
		"\u044d\t\u0006\u0003\u0006\u044f\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u0458"+
		"\b\u0006\n\u0006\f\u0006\u045b\t\u0006\u0003\u0006\u045d\b\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0003\u0006\u046d\b\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u0475\b\u0006\n"+
		"\u0006\f\u0006\u0478\t\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u047c"+
		"\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0482"+
		"\b\u0006\u0001\u0006\u0003\u0006\u0485\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0004\u0006\u048c\b\u0006\u000b\u0006"+
		"\f\u0006\u048d\u0003\u0006\u0490\b\u0006\u0001\u0007\u0003\u0007\u0493"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0005"+
		"\b\u049b\b\b\n\b\f\b\u049e\t\b\u0001\t\u0003\t\u04a1\b\t\u0001\t\u0001"+
		"\t\u0001\n\u0001\n\u0003\n\u04a7\b\n\u0001\n\u0001\n\u0001\n\u0005\n\u04ac"+
		"\b\n\n\n\f\n\u04af\t\n\u0001\u000b\u0001\u000b\u0003\u000b\u04b3\b\u000b"+
		"\u0001\f\u0001\f\u0001\f\u0001\f\u0003\f\u04b9\b\f\u0001\f\u0001\f\u0003"+
		"\f\u04bd\b\f\u0001\f\u0001\f\u0003\f\u04c1\b\f\u0001\r\u0001\r\u0001\r"+
		"\u0001\r\u0003\r\u04c7\b\r\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e"+
		"\u0001\u000f\u0001\u000f\u0001\u000f\u0005\u000f\u04d0\b\u000f\n\u000f"+
		"\f\u000f\u04d3\t\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010"+
		"\u0001\u0011\u0001\u0011\u0003\u0011\u04db\b\u0011\u0001\u0012\u0001\u0012"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0005\u0012\u04e3\b\u0012"+
		"\n\u0012\f\u0012\u04e6\t\u0012\u0003\u0012\u04e8\b\u0012\u0001\u0012\u0001"+
		"\u0012\u0001\u0012\u0003\u0012\u04ed\b\u0012\u0003\u0012\u04ef\b\u0012"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012"+
		"\u04f6\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012"+
		"\u04fc\b\u0012\u0003\u0012\u04fe\b\u0012\u0001\u0013\u0001\u0013\u0003"+
		"\u0013\u0502\b\u0013\u0001\u0014\u0001\u0014\u0001\u0015\u0001\u0015\u0001"+
		"\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u050c\b\u0015\u0001"+
		"\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u0512\b\u0015\u0001"+
		"\u0015\u0005\u0015\u0515\b\u0015\n\u0015\f\u0015\u0518\t\u0015\u0001\u0016"+
		"\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016"+
		"\u0005\u0016\u0521\b\u0016\n\u0016\f\u0016\u0524\t\u0016\u0001\u0016\u0001"+
		"\u0016\u0001\u0016\u0001\u0016\u0003\u0016\u052a\b\u0016\u0001\u0017\u0001"+
		"\u0017\u0003\u0017\u052e\b\u0017\u0001\u0017\u0001\u0017\u0003\u0017\u0532"+
		"\b\u0017\u0001\u0018\u0001\u0018\u0003\u0018\u0536\b\u0018\u0001\u0018"+
		"\u0001\u0018\u0001\u0018\u0005\u0018\u053b\b\u0018\n\u0018\f\u0018\u053e"+
		"\t\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0005\u0018\u0544"+
		"\b\u0018\n\u0018\f\u0018\u0547\t\u0018\u0003\u0018\u0549\b\u0018\u0001"+
		"\u0018\u0001\u0018\u0003\u0018\u054d\b\u0018\u0001\u0018\u0001\u0018\u0001"+
		"\u0018\u0003\u0018\u0552\b\u0018\u0001\u0018\u0001\u0018\u0003\u0018\u0556"+
		"\b\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0001\u0018\u0005\u0018\u055c"+
		"\b\u0018\n\u0018\f\u0018\u055f\t\u0018\u0003\u0018\u0561\b\u0018\u0001"+
		"\u0019\u0003\u0019\u0564\b\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0005"+
		"\u0019\u0569\b\u0019\n\u0019\f\u0019\u056c\t\u0019\u0001\u001a\u0001\u001a"+
		"\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0005\u001a\u0574\b\u001a"+
		"\n\u001a\f\u001a\u0577\t\u001a\u0003\u001a\u0579\b\u001a\u0001\u001a\u0001"+
		"\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0005\u001a\u0581"+
		"\b\u001a\n\u001a\f\u001a\u0584\t\u001a\u0003\u001a\u0586\b\u001a\u0001"+
		"\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0001"+
		"\u001a\u0005\u001a\u058f\b\u001a\n\u001a\f\u001a\u0592\t\u001a\u0001\u001a"+
		"\u0001\u001a\u0003\u001a\u0596\b\u001a\u0001\u001b\u0001\u001b\u0001\u001b"+
		"\u0001\u001b\u0005\u001b\u059c\b\u001b\n\u001b\f\u001b\u059f\t\u001b\u0003"+
		"\u001b\u05a1\b\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u05a5\b\u001b"+
		"\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c"+
		"\u0001\u001d\u0003\u001d\u05ae\b\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0005\u001d\u05b5\b\u001d\n\u001d\f\u001d\u05b8"+
		"\t\u001d\u0003\u001d\u05ba\b\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0005\u001d\u05c1\b\u001d\n\u001d\f\u001d\u05c4"+
		"\t\u001d\u0003\u001d\u05c6\b\u001d\u0001\u001d\u0003\u001d\u05c9\b\u001d"+
		"\u0001\u001e\u0001\u001e\u0003\u001e\u05cd\b\u001e\u0001\u001e\u0001\u001e"+
		"\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001 \u0001"+
		" \u0003 \u05d8\b \u0001 \u0003 \u05db\b \u0001 \u0001 \u0001 \u0001 \u0001"+
		" \u0003 \u05e2\b \u0001 \u0003 \u05e5\b \u0001!\u0001!\u0001!\u0001!\u0001"+
		"!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001!\u0001"+
		"!\u0001!\u0001!\u0003!\u05f8\b!\u0005!\u05fa\b!\n!\f!\u05fd\t!\u0001\""+
		"\u0003\"\u0600\b\"\u0001\"\u0001\"\u0003\"\u0604\b\"\u0001\"\u0001\"\u0003"+
		"\"\u0608\b\"\u0001\"\u0001\"\u0003\"\u060c\b\"\u0003\"\u060e\b\"\u0001"+
		"#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0005#\u0617\b#\n#\f#\u061a"+
		"\t#\u0001#\u0001#\u0003#\u061e\b#\u0001$\u0001$\u0001$\u0001$\u0001$\u0001"+
		"$\u0001$\u0003$\u0627\b$\u0001%\u0001%\u0001&\u0001&\u0001\'\u0001\'\u0001"+
		"\'\u0003\'\u0630\b\'\u0001\'\u0003\'\u0633\b\'\u0001(\u0001(\u0001(\u0001"+
		"(\u0003(\u0639\b(\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001"+
		")\u0005)\u0643\b)\n)\f)\u0646\t)\u0003)\u0648\b)\u0001)\u0001)\u0001)"+
		"\u0001)\u0001)\u0005)\u064f\b)\n)\f)\u0652\t)\u0003)\u0654\b)\u0001)\u0001"+
		")\u0001)\u0001)\u0005)\u065a\b)\n)\f)\u065d\t)\u0003)\u065f\b)\u0001)"+
		"\u0003)\u0662\b)\u0001)\u0001)\u0001)\u0003)\u0667\b)\u0001)\u0003)\u066a"+
		"\b)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0005)\u0674"+
		"\b)\n)\f)\u0677\t)\u0003)\u0679\b)\u0001)\u0001)\u0001)\u0001)\u0005)"+
		"\u067f\b)\n)\f)\u0682\t)\u0001)\u0001)\u0003)\u0686\b)\u0001)\u0001)\u0003"+
		")\u068a\b)\u0003)\u068c\b)\u0003)\u068e\b)\u0001*\u0001*\u0001*\u0001"+
		"*\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0003"+
		"+\u069d\b+\u0003+\u069f\b+\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001"+
		",\u0001,\u0001,\u0003,\u06aa\b,\u0001-\u0001-\u0001-\u0001-\u0001-\u0001"+
		"-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001-\u0001"+
		"-\u0001-\u0001-\u0001-\u0003-\u06bf\b-\u0001.\u0001.\u0001.\u0001.\u0001"+
		".\u0001.\u0005.\u06c7\b.\n.\f.\u06ca\t.\u0001.\u0001.\u0001/\u0001/\u0001"+
		"/\u0001/\u00010\u00010\u00030\u06d4\b0\u00010\u00010\u00030\u06d8\b0\u0003"+
		"0\u06da\b0\u00011\u00011\u00011\u00011\u00051\u06e0\b1\n1\f1\u06e3\t1"+
		"\u00011\u00011\u00012\u00012\u00032\u06e9\b2\u00012\u00012\u00012\u0001"+
		"2\u00012\u00012\u00012\u00012\u00012\u00052\u06f4\b2\n2\f2\u06f7\t2\u0001"+
		"2\u00012\u00012\u00032\u06fc\b2\u00012\u00012\u00012\u00012\u00012\u0001"+
		"2\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u0001"+
		"2\u00012\u00012\u00012\u00012\u00012\u00012\u00052\u0714\b2\n2\f2\u0717"+
		"\t2\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u00012\u0001"+
		"2\u00012\u00012\u00032\u0725\b2\u00012\u00012\u00012\u00032\u072a\b2\u0001"+
		"2\u00012\u00032\u072e\b2\u00013\u00013\u00013\u00013\u00013\u00013\u0001"+
		"3\u00013\u00033\u0738\b3\u00013\u00013\u00013\u00013\u00033\u073e\b3\u0001"+
		"3\u00013\u00013\u00013\u00033\u0744\b3\u00013\u00013\u00013\u00013\u0001"+
		"3\u00013\u00033\u074c\b3\u00013\u00013\u00013\u00033\u0751\b3\u00013\u0001"+
		"3\u00013\u00013\u00013\u00033\u0758\b3\u00033\u075a\b3\u00013\u00013\u0001"+
		"3\u00013\u00033\u0760\b3\u00013\u00013\u00013\u00013\u00033\u0766\b3\u0001"+
		"3\u00013\u00033\u076a\b3\u00013\u00013\u00013\u00033\u076f\b3\u00013\u0001"+
		"3\u00013\u00013\u00013\u00053\u0776\b3\n3\f3\u0779\t3\u00013\u00013\u0003"+
		"3\u077d\b3\u00014\u00014\u00014\u00014\u00014\u00014\u00014\u00014\u0001"+
		"4\u00014\u00054\u0789\b4\n4\f4\u078c\t4\u00014\u00014\u00014\u00014\u0001"+
		"4\u00054\u0793\b4\n4\f4\u0796\t4\u00034\u0798\b4\u00015\u00015\u00016"+
		"\u00016\u00016\u00016\u00016\u00036\u07a1\b6\u00017\u00017\u00017\u0003"+
		"7\u07a6\b7\u00017\u00017\u00017\u00037\u07ab\b7\u00037\u07ad\b7\u0001"+
		"8\u00018\u00018\u00018\u00018\u00058\u07b4\b8\n8\f8\u07b7\t8\u00038\u07b9"+
		"\b8\u00018\u00018\u00018\u00018\u00058\u07bf\b8\n8\f8\u07c2\t8\u00038"+
		"\u07c4\b8\u00018\u00018\u00019\u00019\u00019\u00039\u07cb\b9\u00019\u0001"+
		"9\u00019\u00039\u07d0\b9\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001"+
		":\u0005:\u07d9\b:\n:\f:\u07dc\t:\u0003:\u07de\b:\u0001:\u0001:\u0003:"+
		"\u07e2\b:\u0003:\u07e4\b:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0003"+
		":\u07ec\b:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0005:\u07f4\b:\n"+
		":\f:\u07f7\t:\u0001:\u0001:\u0001:\u0003:\u07fc\b:\u0003:\u07fe\b:\u0001"+
		";\u0001;\u0001;\u0001;\u0001;\u0003;\u0805\b;\u0001;\u0001;\u0003;\u0809"+
		"\b;\u0003;\u080b\b;\u0001;\u0001;\u0001;\u0001;\u0001;\u0003;\u0812\b"+
		";\u0001;\u0001;\u0003;\u0816\b;\u0003;\u0818\b;\u0003;\u081a\b;\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0005<\u0821\b<\n<\f<\u0824\t<\u0001<\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u082e\b<\u0001=\u0001"+
		"=\u0003=\u0832\b=\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u083a"+
		"\b>\n>\f>\u083d\t>\u0001>\u0001>\u0001?\u0001?\u0001@\u0001@\u0001@\u0003"+
		"@\u0846\b@\u0001@\u0001@\u0003@\u084a\b@\u0001@\u0001@\u0001@\u0001@\u0001"+
		"@\u0001@\u0005@\u0852\b@\n@\f@\u0855\t@\u0001A\u0001A\u0001A\u0001A\u0001"+
		"A\u0001A\u0001A\u0001A\u0001A\u0001A\u0003A\u0861\bA\u0001A\u0001A\u0001"+
		"A\u0001A\u0001A\u0001A\u0003A\u0869\bA\u0001A\u0001A\u0001A\u0001A\u0001"+
		"A\u0005A\u0870\bA\nA\fA\u0873\tA\u0001A\u0001A\u0001A\u0003A\u0878\bA"+
		"\u0001A\u0001A\u0001A\u0001A\u0001A\u0001A\u0003A\u0880\bA\u0001A\u0001"+
		"A\u0001A\u0001A\u0003A\u0886\bA\u0001A\u0001A\u0003A\u088a\bA\u0001A\u0001"+
		"A\u0001A\u0003A\u088f\bA\u0001A\u0001A\u0001A\u0003A\u0894\bA\u0001B\u0001"+
		"B\u0001B\u0001B\u0003B\u089a\bB\u0001B\u0001B\u0001B\u0001B\u0001B\u0001"+
		"B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0005B\u08a8\bB\nB\fB\u08ab"+
		"\tB\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0004C\u08c6\bC\u000bC\fC\u08c7\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0005C\u08d1\bC\nC\fC\u08d4"+
		"\tC\u0001C\u0001C\u0001C\u0001C\u0001C\u0003C\u08db\bC\u0001C\u0001C\u0001"+
		"C\u0003C\u08e0\bC\u0001C\u0001C\u0001C\u0003C\u08e5\bC\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0005C\u08f0\bC\nC\fC\u08f3"+
		"\tC\u0001C\u0001C\u0001C\u0003C\u08f8\bC\u0001C\u0003C\u08fb\bC\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0003C\u0902\bC\u0001C\u0001C\u0001C\u0003"+
		"C\u0907\bC\u0001C\u0003C\u090a\bC\u0001C\u0003C\u090d\bC\u0001C\u0001"+
		"C\u0001C\u0003C\u0912\bC\u0001C\u0001C\u0001C\u0005C\u0917\bC\nC\fC\u091a"+
		"\tC\u0003C\u091c\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0005C\u0923\b"+
		"C\nC\fC\u0926\tC\u0003C\u0928\bC\u0001C\u0001C\u0003C\u092c\bC\u0001C"+
		"\u0003C\u092f\bC\u0001C\u0003C\u0932\bC\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0005C\u093f\bC\nC\fC\u0942"+
		"\tC\u0003C\u0944\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0004C\u0955\bC\u000b"+
		"C\fC\u0956\u0001C\u0001C\u0003C\u095b\bC\u0001C\u0001C\u0001C\u0001C\u0004"+
		"C\u0961\bC\u000bC\fC\u0962\u0001C\u0001C\u0003C\u0967\bC\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0005"+
		"C\u097e\bC\nC\fC\u0981\tC\u0003C\u0983\bC\u0001C\u0001C\u0001C\u0001C"+
		"\u0001C\u0001C\u0001C\u0003C\u098c\bC\u0001C\u0001C\u0001C\u0001C\u0003"+
		"C\u0992\bC\u0001C\u0001C\u0001C\u0001C\u0003C\u0998\bC\u0001C\u0001C\u0001"+
		"C\u0001C\u0003C\u099e\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0003C\u09a7\bC\u0001C\u0003C\u09aa\bC\u0001C\u0003C\u09ad\bC\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0003C\u09c0\bC\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0003C\u09c9\bC\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0001C\u0005C\u09dd\bC\nC\fC\u09e0\tC\u0003"+
		"C\u09e2\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0003"+
		"C\u09ec\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0003C\u09f5"+
		"\bC\u0001C\u0001C\u0001C\u0001C\u0003C\u09fb\bC\u0001C\u0001C\u0001C\u0001"+
		"C\u0003C\u0a01\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0003C\u0a0c\bC\u0003C\u0a0e\bC\u0001C\u0001C\u0001C\u0003C\u0a13"+
		"\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0003C\u0a1a\bC\u0003C\u0a1c\b"+
		"C\u0001C\u0001C\u0001C\u0001C\u0003C\u0a22\bC\u0001C\u0001C\u0001C\u0001"+
		"C\u0003C\u0a28\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0005"+
		"C\u0a31\bC\nC\fC\u0a34\tC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001C\u0003"+
		"C\u0a3c\bC\u0001C\u0001C\u0001C\u0003C\u0a41\bC\u0001C\u0001C\u0001C\u0003"+
		"C\u0a46\bC\u0003C\u0a48\bC\u0003C\u0a4a\bC\u0001C\u0001C\u0001C\u0001"+
		"C\u0003C\u0a50\bC\u0003C\u0a52\bC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0005C\u0a5a\bC\nC\fC\u0a5d\tC\u0001C\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0003C\u0a65\bC\u0003C\u0a67\bC\u0001C\u0001C\u0001C\u0001C\u0003C\u0a6d"+
		"\bC\u0003C\u0a6f\bC\u0001C\u0003C\u0a72\bC\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0001C\u0005C\u0a7c\bC\nC\fC\u0a7f\tC\u0001D\u0001"+
		"D\u0001D\u0001D\u0001D\u0003D\u0a86\bD\u0001D\u0001D\u0001D\u0001D\u0005"+
		"D\u0a8c\bD\nD\fD\u0a8f\tD\u0003D\u0a91\bD\u0001E\u0001E\u0001E\u0003E"+
		"\u0a96\bE\u0001F\u0001F\u0001F\u0003F\u0a9b\bF\u0001G\u0001G\u0001G\u0001"+
		"G\u0001H\u0001H\u0001I\u0001I\u0001I\u0001I\u0003I\u0aa7\bI\u0001J\u0001"+
		"J\u0003J\u0aab\bJ\u0001J\u0001J\u0003J\u0aaf\bJ\u0001J\u0003J\u0ab2\b"+
		"J\u0003J\u0ab4\bJ\u0001K\u0001K\u0001K\u0001K\u0001K\u0001K\u0003K\u0abc"+
		"\bK\u0001L\u0003L\u0abf\bL\u0001L\u0001L\u0001L\u0001L\u0001L\u0001L\u0001"+
		"L\u0001L\u0003L\u0ac9\bL\u0001M\u0001M\u0001N\u0001N\u0001N\u0001N\u0003"+
		"N\u0ad1\bN\u0001O\u0001O\u0001O\u0001O\u0003O\u0ad7\bO\u0003O\u0ad9\b"+
		"O\u0001P\u0001P\u0001P\u0001P\u0001P\u0001P\u0003P\u0ae1\bP\u0001Q\u0001"+
		"Q\u0001R\u0001R\u0001S\u0001S\u0001T\u0001T\u0003T\u0aeb\bT\u0001T\u0001"+
		"T\u0001T\u0001T\u0003T\u0af1\bT\u0001U\u0001U\u0001V\u0001V\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0001W\u0005W\u0afd\bW\nW\fW\u0b00\tW\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0001W\u0003W\u0b08\bW\u0001W\u0001W\u0001W\u0001"+
		"W\u0001W\u0003W\u0b0f\bW\u0001W\u0001W\u0001W\u0003W\u0b14\bW\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0003W\u0b1b\bW\u0001W\u0001W\u0001W\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0003W\u0b25\bW\u0001W\u0001W\u0001W\u0003W\u0b2a"+
		"\bW\u0001W\u0001W\u0001W\u0001W\u0001W\u0003W\u0b31\bW\u0001W\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0005"+
		"W\u0b49\bW\nW\fW\u0b4c\tW\u0001W\u0001W\u0003W\u0b50\bW\u0003W\u0b52\b"+
		"W\u0001W\u0001W\u0001W\u0001W\u0001W\u0003W\u0b59\bW\u0005W\u0b5b\bW\n"+
		"W\fW\u0b5e\tW\u0001X\u0001X\u0001X\u0001X\u0003X\u0b64\bX\u0001Y\u0001"+
		"Y\u0003Y\u0b68\bY\u0001Z\u0001Z\u0001Z\u0001Z\u0001Z\u0001[\u0001[\u0001"+
		"[\u0001[\u0001[\u0001[\u0001\\\u0001\\\u0001\\\u0001\\\u0003\\\u0b79\b"+
		"\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001"+
		"\\\u0001\\\u0001\\\u0005\\\u0b86\b\\\n\\\f\\\u0b89\t\\\u0001\\\u0001\\"+
		"\u0001\\\u0001\\\u0003\\\u0b8f\b\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001"+
		"\\\u0001\\\u0001\\\u0003\\\u0b98\b\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001"+
		"\\\u0001\\\u0005\\\u0ba0\b\\\n\\\f\\\u0ba3\t\\\u0001\\\u0001\\\u0003\\"+
		"\u0ba7\b\\\u0001\\\u0001\\\u0001\\\u0001\\\u0001\\\u0005\\\u0bae\b\\\n"+
		"\\\f\\\u0bb1\t\\\u0001\\\u0001\\\u0003\\\u0bb5\b\\\u0001]\u0001]\u0001"+
		"]\u0001]\u0001]\u0001]\u0003]\u0bbd\b]\u0001^\u0001^\u0001^\u0001^\u0005"+
		"^\u0bc3\b^\n^\f^\u0bc6\t^\u0003^\u0bc8\b^\u0001^\u0001^\u0001^\u0001^"+
		"\u0003^\u0bce\b^\u0001^\u0003^\u0bd1\b^\u0001^\u0001^\u0001^\u0001^\u0001"+
		"^\u0003^\u0bd8\b^\u0001^\u0001^\u0001^\u0001^\u0005^\u0bde\b^\n^\f^\u0be1"+
		"\t^\u0003^\u0be3\b^\u0001^\u0001^\u0001^\u0001^\u0005^\u0be9\b^\n^\f^"+
		"\u0bec\t^\u0003^\u0bee\b^\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001"+
		"_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001"+
		"_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0001_\u0003_\u0c08\b_\u0001"+
		"`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0003`\u0c13"+
		"\b`\u0001a\u0001a\u0001a\u0003a\u0c18\ba\u0001a\u0001a\u0001a\u0001a\u0001"+
		"a\u0005a\u0c1f\ba\na\fa\u0c22\ta\u0001b\u0001b\u0001b\u0001b\u0001b\u0001"+
		"b\u0001b\u0001b\u0005b\u0c2c\bb\nb\fb\u0c2f\tb\u0001b\u0001b\u0001b\u0001"+
		"b\u0001b\u0001b\u0001b\u0001b\u0001b\u0001b\u0001b\u0001b\u0003b\u0c3d"+
		"\bb\u0001c\u0001c\u0003c\u0c41\bc\u0001c\u0001c\u0003c\u0c45\bc\u0001"+
		"c\u0001c\u0003c\u0c49\bc\u0001c\u0001c\u0001c\u0001c\u0003c\u0c4f\bc\u0001"+
		"c\u0001c\u0003c\u0c53\bc\u0001c\u0001c\u0003c\u0c57\bc\u0001c\u0001c\u0003"+
		"c\u0c5b\bc\u0003c\u0c5d\bc\u0001d\u0001d\u0001d\u0001d\u0001e\u0001e\u0001"+
		"e\u0001e\u0003e\u0c67\be\u0001f\u0001f\u0001f\u0001f\u0001f\u0003f\u0c6e"+
		"\bf\u0001g\u0001g\u0001g\u0001g\u0001g\u0001g\u0001g\u0003g\u0c77\bg\u0001"+
		"h\u0001h\u0001h\u0001h\u0001h\u0003h\u0c7e\bh\u0001i\u0001i\u0001i\u0001"+
		"i\u0001i\u0003i\u0c85\bi\u0001j\u0001j\u0001j\u0005j\u0c8a\bj\nj\fj\u0c8d"+
		"\tj\u0001k\u0001k\u0001k\u0001k\u0005k\u0c93\bk\nk\fk\u0c96\tk\u0001k"+
		"\u0001k\u0001l\u0001l\u0001l\u0001l\u0001l\u0005l\u0c9f\bl\nl\fl\u0ca2"+
		"\tl\u0003l\u0ca4\bl\u0001l\u0001l\u0001m\u0003m\u0ca9\bm\u0001m\u0001"+
		"m\u0001n\u0001n\u0001n\u0001o\u0001o\u0001o\u0003o\u0cb3\bo\u0001o\u0001"+
		"o\u0001o\u0001o\u0001o\u0001o\u0001o\u0001o\u0001o\u0001o\u0001o\u0001"+
		"o\u0001o\u0001o\u0003o\u0cc3\bo\u0001p\u0001p\u0001p\u0001p\u0001p\u0001"+
		"p\u0001p\u0001p\u0001p\u0001p\u0004p\u0ccf\bp\u000bp\fp\u0cd0\u0001p\u0003"+
		"p\u0cd4\bp\u0001p\u0001p\u0001p\u0001p\u0001p\u0004p\u0cdb\bp\u000bp\f"+
		"p\u0cdc\u0001p\u0003p\u0ce0\bp\u0001p\u0001p\u0001p\u0001p\u0001p\u0001"+
		"p\u0001p\u0001p\u0005p\u0cea\bp\np\fp\u0ced\tp\u0001p\u0003p\u0cf0\bp"+
		"\u0001p\u0001p\u0001p\u0001p\u0001p\u0001p\u0001p\u0001p\u0001p\u0001"+
		"p\u0001p\u0005p\u0cfd\bp\np\fp\u0d00\tp\u0001p\u0003p\u0d03\bp\u0001p"+
		"\u0001p\u0001p\u0001p\u0003p\u0d09\bp\u0001p\u0001p\u0001p\u0001p\u0001"+
		"p\u0001p\u0001p\u0001p\u0003p\u0d13\bp\u0001p\u0001p\u0001p\u0001p\u0001"+
		"p\u0001p\u0001p\u0001p\u0001p\u0001p\u0003p\u0d1f\bp\u0001p\u0001p\u0001"+
		"p\u0001p\u0001p\u0001p\u0001p\u0003p\u0d28\bp\u0001q\u0001q\u0001q\u0001"+
		"q\u0001q\u0001r\u0001r\u0001r\u0001r\u0001r\u0001s\u0001s\u0001s\u0001"+
		"t\u0001t\u0001t\u0001t\u0005t\u0d3b\bt\nt\ft\u0d3e\tt\u0001t\u0001t\u0001"+
		"t\u0003t\u0d43\bt\u0001u\u0001u\u0001u\u0004u\u0d48\bu\u000bu\fu\u0d49"+
		"\u0001v\u0001v\u0001v\u0001v\u0001v\u0001v\u0003v\u0d52\bv\u0001w\u0001"+
		"w\u0001w\u0003w\u0d57\bw\u0001x\u0003x\u0d5a\bx\u0001x\u0001x\u0001y\u0001"+
		"y\u0001y\u0005y\u0d61\by\ny\fy\u0d64\ty\u0001z\u0001z\u0001z\u0001z\u0001"+
		"z\u0001z\u0001{\u0001{\u0001|\u0001|\u0001|\u0003|\u0d71\b|\u0001}\u0001"+
		"}\u0001}\u0001}\u0001}\u0003}\u0d78\b}\u0001~\u0001~\u0001~\u0005~\u0d7d"+
		"\b~\n~\f~\u0d80\t~\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f\u0001"+
		"\u007f\u0001\u007f\u0003\u007f\u0d88\b\u007f\u0001\u0080\u0001\u0080\u0001"+
		"\u0080\u0001\u0080\u0001\u0080\u0003\u0080\u0d8f\b\u0080\u0001\u0081\u0003"+
		"\u0081\u0d92\b\u0081\u0001\u0081\u0001\u0081\u0003\u0081\u0d96\b\u0081"+
		"\u0001\u0081\u0001\u0081\u0003\u0081\u0d9a\b\u0081\u0001\u0081\u0003\u0081"+
		"\u0d9d\b\u0081\u0001\u0082\u0001\u0082\u0003\u0082\u0da1\b\u0082\u0001"+
		"\u0083\u0001\u0083\u0001\u0083\u0000\u0007*B\u0080\u0084\u0086\u00ae\u00c2"+
		"\u0084\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018"+
		"\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080"+
		"\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098"+
		"\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0"+
		"\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8"+
		"\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0"+
		"\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8"+
		"\u00fa\u00fc\u00fe\u0100\u0102\u0104\u0106\u0000#\u0002\u0000\'\'\u00e5"+
		"\u00e5\u0002\u0000HH\u0083\u0083\u0002\u0000iizz\u0002\u0000\\\\{{\u0001"+
		"\u0000\u00ef\u00f0\u0002\u0000ee\u00ae\u00ae\u0002\u0000\u0144\u0144\u0149"+
		"\u0149\u0002\u0000[[\u0119\u0119\u0002\u0000\u001d\u001dKK\u0002\u0000"+
		"ee\u0094\u0094\u0002\u0000\u0016\u0016OO\u0002\u0000!!\u0103\u0103\u0003"+
		"\u0000##\u0096\u0096\u010e\u010e\u0002\u0000||\u00f7\u00f7\u0002\u0000"+
		"UUYY\u0002\u0000\u0090\u0090\u00bd\u00bd\u0002\u0000}}\u00c5\u00c5\u0002"+
		"\u000066\u0119\u0119\u0001\u0000\u013e\u013f\u0001\u0000\u0140\u0142\u0001"+
		"\u0000\u0123\u0125\u0004\u0000YYaa\u0111\u0111\u011b\u011b\u0002\u0000"+
		"11\u0118\u0118\u0002\u0000dd\u00f1\u00f1\u0001\u0000\u0138\u013d\u0003"+
		"\u0000\u0016\u0016\u001a\u001a\u00fe\u00fe\u0002\u0000aa\u0111\u0111\u0005"+
		"\u0000CCvv\u00aa\u00ab\u00f5\u00f5\u0136\u0136\u0001\u0000\u00af\u00b2"+
		"\u0002\u0000ff\u00d4\u00d4\u0003\u0000qq\u0089\u0089\u0107\u0107\u0004"+
		"\u0000PP\u0084\u0084\u00a0\u00a0\u0126\u0126\u0002\u0000\u00c0\u00c0\u0135"+
		"\u0135\u0002\u0000\u010c\u010c\u012a\u012a6\u0000\u0012\u0016\u0018\u0018"+
		"\u001a\u001b\u001d!##%\'*13488ACEHJKMNPRUWYY\\\\__bfhhkqttvy{|~~\u0081"+
		"\u0081\u0083\u0084\u0086\u0087\u0089\u0089\u0090\u0097\u0099\u0099\u009b"+
		"\u009b\u009d\u009d\u00a0\u00ab\u00ad\u00b4\u00b8\u00bd\u00bf\u00c1\u00c4"+
		"\u00c4\u00c6\u00d5\u00d7\u00dc\u00de\u00e9\u00eb\u00ed\u00ef\u00f7\u00f9"+
		"\u0103\u0105\u0108\u010a\u010f\u0112\u0114\u0116\u0118\u011a\u011c\u011e"+
		"\u0121\u0123\u0127\u0129\u012b\u012e\u012f\u0131\u0137\u0fc6\u0000\u0108"+
		"\u0001\u0000\u0000\u0000\u0002\u010b\u0001\u0000\u0000\u0000\u0004\u010e"+
		"\u0001\u0000\u0000\u0000\u0006\u0111\u0001\u0000\u0000\u0000\b\u0114\u0001"+
		"\u0000\u0000\u0000\n\u0117\u0001\u0000\u0000\u0000\f\u048f\u0001\u0000"+
		"\u0000\u0000\u000e\u0492\u0001\u0000\u0000\u0000\u0010\u0496\u0001\u0000"+
		"\u0000\u0000\u0012\u04a0\u0001\u0000\u0000\u0000\u0014\u04a4\u0001\u0000"+
		"\u0000\u0000\u0016\u04b2\u0001\u0000\u0000\u0000\u0018\u04b4\u0001\u0000"+
		"\u0000\u0000\u001a\u04c2\u0001\u0000\u0000\u0000\u001c\u04c8\u0001\u0000"+
		"\u0000\u0000\u001e\u04cc\u0001\u0000\u0000\u0000 \u04d4\u0001\u0000\u0000"+
		"\u0000\"\u04da\u0001\u0000\u0000\u0000$\u04dc\u0001\u0000\u0000\u0000"+
		"&\u0501\u0001\u0000\u0000\u0000(\u0503\u0001\u0000\u0000\u0000*\u0505"+
		"\u0001\u0000\u0000\u0000,\u0529\u0001\u0000\u0000\u0000.\u052b\u0001\u0000"+
		"\u0000\u00000\u0533\u0001\u0000\u0000\u00002\u0563\u0001\u0000\u0000\u0000"+
		"4\u0595\u0001\u0000\u0000\u00006\u05a4\u0001\u0000\u0000\u00008\u05a6"+
		"\u0001\u0000\u0000\u0000:\u05ad\u0001\u0000\u0000\u0000<\u05ca\u0001\u0000"+
		"\u0000\u0000>\u05d3\u0001\u0000\u0000\u0000@\u05e4\u0001\u0000\u0000\u0000"+
		"B\u05e6\u0001\u0000\u0000\u0000D\u060d\u0001\u0000\u0000\u0000F\u061d"+
		"\u0001\u0000\u0000\u0000H\u061f\u0001\u0000\u0000\u0000J\u0628\u0001\u0000"+
		"\u0000\u0000L\u062a\u0001\u0000\u0000\u0000N\u0632\u0001\u0000\u0000\u0000"+
		"P\u0638\u0001\u0000\u0000\u0000R\u063a\u0001\u0000\u0000\u0000T\u068f"+
		"\u0001\u0000\u0000\u0000V\u069e\u0001\u0000\u0000\u0000X\u06a9\u0001\u0000"+
		"\u0000\u0000Z\u06be\u0001\u0000\u0000\u0000\\\u06c0\u0001\u0000\u0000"+
		"\u0000^\u06cd\u0001\u0000\u0000\u0000`\u06d1\u0001\u0000\u0000\u0000b"+
		"\u06db\u0001\u0000\u0000\u0000d\u072d\u0001\u0000\u0000\u0000f\u077c\u0001"+
		"\u0000\u0000\u0000h\u0797\u0001\u0000\u0000\u0000j\u0799\u0001\u0000\u0000"+
		"\u0000l\u07a0\u0001\u0000\u0000\u0000n\u07ac\u0001\u0000\u0000\u0000p"+
		"\u07ae\u0001\u0000\u0000\u0000r\u07ca\u0001\u0000\u0000\u0000t\u07d1\u0001"+
		"\u0000\u0000\u0000v\u0819\u0001\u0000\u0000\u0000x\u082d\u0001\u0000\u0000"+
		"\u0000z\u082f\u0001\u0000\u0000\u0000|\u0833\u0001\u0000\u0000\u0000~"+
		"\u0840\u0001\u0000\u0000\u0000\u0080\u0849\u0001\u0000\u0000\u0000\u0082"+
		"\u0893\u0001\u0000\u0000\u0000\u0084\u0899\u0001\u0000\u0000\u0000\u0086"+
		"\u0a71\u0001\u0000\u0000\u0000\u0088\u0a80\u0001\u0000\u0000\u0000\u008a"+
		"\u0a92\u0001\u0000\u0000\u0000\u008c\u0a97\u0001\u0000\u0000\u0000\u008e"+
		"\u0a9c\u0001\u0000\u0000\u0000\u0090\u0aa0\u0001\u0000\u0000\u0000\u0092"+
		"\u0aa6\u0001\u0000\u0000\u0000\u0094\u0ab3\u0001\u0000\u0000\u0000\u0096"+
		"\u0abb\u0001\u0000\u0000\u0000\u0098\u0ac8\u0001\u0000\u0000\u0000\u009a"+
		"\u0aca\u0001\u0000\u0000\u0000\u009c\u0ad0\u0001\u0000\u0000\u0000\u009e"+
		"\u0ad8\u0001\u0000\u0000\u0000\u00a0\u0ae0\u0001\u0000\u0000\u0000\u00a2"+
		"\u0ae2\u0001\u0000\u0000\u0000\u00a4\u0ae4\u0001\u0000\u0000\u0000\u00a6"+
		"\u0ae6\u0001\u0000\u0000\u0000\u00a8\u0ae8\u0001\u0000\u0000\u0000\u00aa"+
		"\u0af2\u0001\u0000\u0000\u0000\u00ac\u0af4\u0001\u0000\u0000\u0000\u00ae"+
		"\u0b51\u0001\u0000\u0000\u0000\u00b0\u0b63\u0001\u0000\u0000\u0000\u00b2"+
		"\u0b67\u0001\u0000\u0000\u0000\u00b4\u0b69\u0001\u0000\u0000\u0000\u00b6"+
		"\u0b6e\u0001\u0000\u0000\u0000\u00b8\u0bb4\u0001\u0000\u0000\u0000\u00ba"+
		"\u0bb6\u0001\u0000\u0000\u0000\u00bc\u0bc7\u0001\u0000\u0000\u0000\u00be"+
		"\u0c07\u0001\u0000\u0000\u0000\u00c0\u0c12\u0001\u0000\u0000\u0000\u00c2"+
		"\u0c14\u0001\u0000\u0000\u0000\u00c4\u0c3c\u0001\u0000\u0000\u0000\u00c6"+
		"\u0c5c\u0001\u0000\u0000\u0000\u00c8\u0c5e\u0001\u0000\u0000\u0000\u00ca"+
		"\u0c66\u0001\u0000\u0000\u0000\u00cc\u0c6d\u0001\u0000\u0000\u0000\u00ce"+
		"\u0c76\u0001\u0000\u0000\u0000\u00d0\u0c7d\u0001\u0000\u0000\u0000\u00d2"+
		"\u0c84\u0001\u0000\u0000\u0000\u00d4\u0c86\u0001\u0000\u0000\u0000\u00d6"+
		"\u0c8e\u0001\u0000\u0000\u0000\u00d8\u0c99\u0001\u0000\u0000\u0000\u00da"+
		"\u0ca8\u0001\u0000\u0000\u0000\u00dc\u0cac\u0001\u0000\u0000\u0000\u00de"+
		"\u0cc2\u0001\u0000\u0000\u0000\u00e0\u0d27\u0001\u0000\u0000\u0000\u00e2"+
		"\u0d29\u0001\u0000\u0000\u0000\u00e4\u0d2e\u0001\u0000\u0000\u0000\u00e6"+
		"\u0d33\u0001\u0000\u0000\u0000\u00e8\u0d36\u0001\u0000\u0000\u0000\u00ea"+
		"\u0d47\u0001\u0000\u0000\u0000\u00ec\u0d51\u0001\u0000\u0000\u0000\u00ee"+
		"\u0d56\u0001\u0000\u0000\u0000\u00f0\u0d59\u0001\u0000\u0000\u0000\u00f2"+
		"\u0d5d\u0001\u0000\u0000\u0000\u00f4\u0d65\u0001\u0000\u0000\u0000\u00f6"+
		"\u0d6b\u0001\u0000\u0000\u0000\u00f8\u0d70\u0001\u0000\u0000\u0000\u00fa"+
		"\u0d77\u0001\u0000\u0000\u0000\u00fc\u0d79\u0001\u0000\u0000\u0000\u00fe"+
		"\u0d87\u0001\u0000\u0000\u0000\u0100\u0d8e\u0001\u0000\u0000\u0000\u0102"+
		"\u0d9c\u0001\u0000\u0000\u0000\u0104\u0da0\u0001\u0000\u0000\u0000\u0106"+
		"\u0da2\u0001\u0000\u0000\u0000\u0108\u0109\u0003\f\u0006\u0000\u0109\u010a"+
		"\u0005\u0000\u0000\u0001\u010a\u0001\u0001\u0000\u0000\u0000\u010b\u010c"+
		"\u0003~?\u0000\u010c\u010d\u0005\u0000\u0000\u0001\u010d\u0003\u0001\u0000"+
		"\u0000\u0000\u010e\u010f\u0003\u00d4j\u0000\u010f\u0110\u0005\u0000\u0000"+
		"\u0001\u0110\u0005\u0001\u0000\u0000\u0000\u0111\u0112\u0003\u00aeW\u0000"+
		"\u0112\u0113\u0005\u0000\u0000\u0001\u0113\u0007\u0001\u0000\u0000\u0000"+
		"\u0114\u0115\u0003\u00c2a\u0000\u0115\u0116\u0005\u0000\u0000\u0001\u0116"+
		"\t\u0001\u0000\u0000\u0000\u0117\u0118\u0003\u00d6k\u0000\u0118\u0119"+
		"\u0005\u0000\u0000\u0001\u0119\u000b\u0001\u0000\u0000\u0000\u011a\u0490"+
		"\u0003\u000e\u0007\u0000\u011b\u011c\u0005\u0120\u0000\u0000\u011c\u0490"+
		"\u0003\u0100\u0080\u0000\u011d\u011e\u0005\u0120\u0000\u0000\u011e\u011f"+
		"\u0003\u0100\u0080\u0000\u011f\u0120\u0005\u0001\u0000\u0000\u0120\u0121"+
		"\u0003\u0100\u0080\u0000\u0121\u0490\u0001\u0000\u0000\u0000\u0122\u0123"+
		"\u00055\u0000\u0000\u0123\u0127\u0005*\u0000\u0000\u0124\u0125\u0005w"+
		"\u0000\u0000\u0125\u0126\u0005\u00b6\u0000\u0000\u0126\u0128\u0005^\u0000"+
		"\u0000\u0127\u0124\u0001\u0000\u0000\u0000\u0127\u0128\u0001\u0000\u0000"+
		"\u0000\u0128\u0129\u0001\u0000\u0000\u0000\u0129\u012a\u0003\u0100\u0080"+
		"\u0000\u012a\u012b\u0005\u0122\u0000\u0000\u012b\u012e\u0003\u0100\u0080"+
		"\u0000\u012c\u012d\u0005.\u0000\u0000\u012d\u012f\u0003\u009eO\u0000\u012e"+
		"\u012c\u0001\u0000\u0000\u0000\u012e\u012f\u0001\u0000\u0000\u0000\u012f"+
		"\u0132\u0001\u0000\u0000\u0000\u0130\u0131\u0005\u001f\u0000\u0000\u0131"+
		"\u0133\u0003\u00fa}\u0000\u0132\u0130\u0001\u0000\u0000\u0000\u0132\u0133"+
		"\u0001\u0000\u0000\u0000\u0133\u0136\u0001\u0000\u0000\u0000\u0134\u0135"+
		"\u0005\u0130\u0000\u0000\u0135\u0137\u0003\u001c\u000e\u0000\u0136\u0134"+
		"\u0001\u0000\u0000\u0000\u0136\u0137\u0001\u0000\u0000\u0000\u0137\u0490"+
		"\u0001\u0000\u0000\u0000\u0138\u0139\u0005S\u0000\u0000\u0139\u013c\u0005"+
		"*\u0000\u0000\u013a\u013b\u0005w\u0000\u0000\u013b\u013d\u0005^\u0000"+
		"\u0000\u013c\u013a\u0001\u0000\u0000\u0000\u013c\u013d\u0001\u0000\u0000"+
		"\u0000\u013d\u013e\u0001\u0000\u0000\u0000\u013e\u0140\u0003\u0100\u0080"+
		"\u0000\u013f\u0141\u0007\u0000\u0000\u0000\u0140\u013f\u0001\u0000\u0000"+
		"\u0000\u0140\u0141\u0001\u0000\u0000\u0000\u0141\u0490\u0001\u0000\u0000"+
		"\u0000\u0142\u0143\u00055\u0000\u0000\u0143\u0147\u0005\u00f3\u0000\u0000"+
		"\u0144\u0145\u0005w\u0000\u0000\u0145\u0146\u0005\u00b6\u0000\u0000\u0146"+
		"\u0148\u0005^\u0000\u0000\u0147\u0144\u0001\u0000\u0000\u0000\u0147\u0148"+
		"\u0001\u0000\u0000\u0000\u0148\u0149\u0001\u0000\u0000\u0000\u0149\u014c"+
		"\u0003\u00f2y\u0000\u014a\u014b\u0005\u001f\u0000\u0000\u014b\u014d\u0003"+
		"\u00fa}\u0000\u014c\u014a\u0001\u0000\u0000\u0000\u014c\u014d\u0001\u0000"+
		"\u0000\u0000\u014d\u0150\u0001\u0000\u0000\u0000\u014e\u014f\u0005\u0130"+
		"\u0000\u0000\u014f\u0151\u0003\u001c\u000e\u0000\u0150\u014e\u0001\u0000"+
		"\u0000\u0000\u0150\u0151\u0001\u0000\u0000\u0000\u0151\u0490\u0001\u0000"+
		"\u0000\u0000\u0152\u0153\u0005S\u0000\u0000\u0153\u0156\u0005\u00f3\u0000"+
		"\u0000\u0154\u0155\u0005w\u0000\u0000\u0155\u0157\u0005^\u0000\u0000\u0156"+
		"\u0154\u0001\u0000\u0000\u0000\u0156\u0157\u0001\u0000\u0000\u0000\u0157"+
		"\u0158\u0001\u0000\u0000\u0000\u0158\u015a\u0003\u00f2y\u0000\u0159\u015b"+
		"\u0007\u0000\u0000\u0000\u015a\u0159\u0001\u0000\u0000\u0000\u015a\u015b"+
		"\u0001\u0000\u0000\u0000\u015b\u0490\u0001\u0000\u0000\u0000\u015c\u015d"+
		"\u0005\u0017\u0000\u0000\u015d\u015e\u0005\u00f3\u0000\u0000\u015e\u015f"+
		"\u0003\u00f2y\u0000\u015f\u0160\u0005\u00df\u0000\u0000\u0160\u0161\u0005"+
		"\u010d\u0000\u0000\u0161\u0162\u0003\u0100\u0080\u0000\u0162\u0490\u0001"+
		"\u0000\u0000\u0000\u0163\u0164\u0005\u0017\u0000\u0000\u0164\u0165\u0005"+
		"\u00f3\u0000\u0000\u0165\u0166\u0003\u00f2y\u0000\u0166\u0167\u0005\u00fb"+
		"\u0000\u0000\u0167\u0168\u0005\u001f\u0000\u0000\u0168\u0169\u0003\u00fa"+
		"}\u0000\u0169\u0490\u0001\u0000\u0000\u0000\u016a\u016d\u00055\u0000\u0000"+
		"\u016b\u016c\u0005\u00c2\u0000\u0000\u016c\u016e\u0005\u00e2\u0000\u0000"+
		"\u016d\u016b\u0001\u0000\u0000\u0000\u016d\u016e\u0001\u0000\u0000\u0000"+
		"\u016e\u016f\u0001\u0000\u0000\u0000\u016f\u0173\u0005\u0104\u0000\u0000"+
		"\u0170\u0171\u0005w\u0000\u0000\u0171\u0172\u0005\u00b6\u0000\u0000\u0172"+
		"\u0174\u0005^\u0000\u0000\u0173\u0170\u0001\u0000\u0000\u0000\u0173\u0174"+
		"\u0001\u0000\u0000\u0000\u0174\u0175\u0001\u0000\u0000\u0000\u0175\u0177"+
		"\u0003\u00f2y\u0000\u0176\u0178\u0003b1\u0000\u0177\u0176\u0001\u0000"+
		"\u0000\u0000\u0177\u0178\u0001\u0000\u0000\u0000\u0178\u017b\u0001\u0000"+
		"\u0000\u0000\u0179\u017a\u0005.\u0000\u0000\u017a\u017c\u0003\u009eO\u0000"+
		"\u017b\u0179\u0001\u0000\u0000\u0000\u017b\u017c\u0001\u0000\u0000\u0000"+
		"\u017c\u017f\u0001\u0000\u0000\u0000\u017d\u017e\u0005\u0130\u0000\u0000"+
		"\u017e\u0180\u0003\u001c\u000e\u0000\u017f\u017d\u0001\u0000\u0000\u0000"+
		"\u017f\u0180\u0001\u0000\u0000\u0000\u0180\u0181\u0001\u0000\u0000\u0000"+
		"\u0181\u0187\u0005\u001c\u0000\u0000\u0182\u0188\u0003\u000e\u0007\u0000"+
		"\u0183\u0184\u0005\u0002\u0000\u0000\u0184\u0185\u0003\u000e\u0007\u0000"+
		"\u0185\u0186\u0005\u0003\u0000\u0000\u0186\u0188\u0001\u0000\u0000\u0000"+
		"\u0187\u0182\u0001\u0000\u0000\u0000\u0187\u0183\u0001\u0000\u0000\u0000"+
		"\u0188\u018e\u0001\u0000\u0000\u0000\u0189\u018b\u0005\u0130\u0000\u0000"+
		"\u018a\u018c\u0005\u00b3\u0000\u0000\u018b\u018a\u0001\u0000\u0000\u0000"+
		"\u018b\u018c\u0001\u0000\u0000\u0000\u018c\u018d\u0001\u0000\u0000\u0000"+
		"\u018d\u018f\u0005A\u0000\u0000\u018e\u0189\u0001\u0000\u0000\u0000\u018e"+
		"\u018f\u0001\u0000\u0000\u0000\u018f\u0490\u0001\u0000\u0000\u0000\u0190"+
		"\u0193\u00055\u0000\u0000\u0191\u0192\u0005\u00c2\u0000\u0000\u0192\u0194"+
		"\u0005\u00e2\u0000\u0000\u0193\u0191\u0001\u0000\u0000\u0000\u0193\u0194"+
		"\u0001\u0000\u0000\u0000\u0194\u0195\u0001\u0000\u0000\u0000\u0195\u0199"+
		"\u0005\u0104\u0000\u0000\u0196\u0197\u0005w\u0000\u0000\u0197\u0198\u0005"+
		"\u00b6\u0000\u0000\u0198\u019a\u0005^\u0000\u0000\u0199\u0196\u0001\u0000"+
		"\u0000\u0000\u0199\u019a\u0001\u0000\u0000\u0000\u019a\u019b\u0001\u0000"+
		"\u0000\u0000\u019b\u019c\u0003\u00f2y\u0000\u019c\u019d\u0005\u0002\u0000"+
		"\u0000\u019d\u01a2\u0003\u0016\u000b\u0000\u019e\u019f\u0005\u0004\u0000"+
		"\u0000\u019f\u01a1\u0003\u0016\u000b\u0000\u01a0\u019e\u0001\u0000\u0000"+
		"\u0000\u01a1\u01a4\u0001\u0000\u0000\u0000\u01a2\u01a0\u0001\u0000\u0000"+
		"\u0000\u01a2\u01a3\u0001\u0000\u0000\u0000\u01a3\u01a5\u0001\u0000\u0000"+
		"\u0000\u01a4\u01a2\u0001\u0000\u0000\u0000\u01a5\u01a8\u0005\u0003\u0000"+
		"\u0000\u01a6\u01a7\u0005.\u0000\u0000\u01a7\u01a9\u0003\u009eO\u0000\u01a8"+
		"\u01a6\u0001\u0000\u0000\u0000\u01a8\u01a9\u0001\u0000\u0000\u0000\u01a9"+
		"\u01ac\u0001\u0000\u0000\u0000\u01aa\u01ab\u0005\u0130\u0000\u0000\u01ab"+
		"\u01ad\u0003\u001c\u000e\u0000\u01ac\u01aa\u0001\u0000\u0000\u0000\u01ac"+
		"\u01ad\u0001\u0000\u0000\u0000\u01ad\u0490\u0001\u0000\u0000\u0000\u01ae"+
		"\u01af\u0005S\u0000\u0000\u01af\u01b2\u0005\u0104\u0000\u0000\u01b0\u01b1"+
		"\u0005w\u0000\u0000\u01b1\u01b3\u0005^\u0000\u0000\u01b2\u01b0\u0001\u0000"+
		"\u0000\u0000\u01b2\u01b3\u0001\u0000\u0000\u0000\u01b3\u01b4\u0001\u0000"+
		"\u0000\u0000\u01b4\u0490\u0003\u00f2y\u0000\u01b5\u01b6\u0005\u007f\u0000"+
		"\u0000\u01b6\u01b7\u0005\u0082\u0000\u0000\u01b7\u01b9\u0003\u00f2y\u0000"+
		"\u01b8\u01ba\u0003b1\u0000\u01b9\u01b8\u0001\u0000\u0000\u0000\u01b9\u01ba"+
		"\u0001\u0000\u0000\u0000\u01ba\u01bb\u0001\u0000\u0000\u0000\u01bb\u01bc"+
		"\u0003\u000e\u0007\u0000\u01bc\u0490\u0001\u0000\u0000\u0000\u01bd\u01be"+
		"\u0005I\u0000\u0000\u01be\u01bf\u0005i\u0000\u0000\u01bf\u01c2\u0003\u00f2"+
		"y\u0000\u01c0\u01c1\u0005\u012d\u0000\u0000\u01c1\u01c3\u0003\u0080@\u0000"+
		"\u01c2\u01c0\u0001\u0000\u0000\u0000\u01c2\u01c3\u0001\u0000\u0000\u0000"+
		"\u01c3\u0490\u0001\u0000\u0000\u0000\u01c4\u01c5\u0005\u0112\u0000\u0000"+
		"\u01c5\u01c6\u0005\u0104\u0000\u0000\u01c6\u0490\u0003\u00f2y\u0000\u01c7"+
		"\u01c8\u0005.\u0000\u0000\u01c8\u01c9\u0005\u00be\u0000\u0000\u01c9\u01ca"+
		"\u0005\u0104\u0000\u0000\u01ca\u01cb\u0003\u00f2y\u0000\u01cb\u01ce\u0005"+
		"\u0085\u0000\u0000\u01cc\u01cf\u0003\u009eO\u0000\u01cd\u01cf\u0005\u00b7"+
		"\u0000\u0000\u01ce\u01cc\u0001\u0000\u0000\u0000\u01ce\u01cd\u0001\u0000"+
		"\u0000\u0000\u01cf\u0490\u0001\u0000\u0000\u0000\u01d0\u01d1\u0005.\u0000"+
		"\u0000\u01d1\u01d2\u0005\u00be\u0000\u0000\u01d2\u01d3\u0005\u012b\u0000"+
		"\u0000\u01d3\u01d4\u0003\u00f2y\u0000\u01d4\u01d7\u0005\u0085\u0000\u0000"+
		"\u01d5\u01d8\u0003\u009eO\u0000\u01d6\u01d8\u0005\u00b7\u0000\u0000\u01d7"+
		"\u01d5\u0001\u0000\u0000\u0000\u01d7\u01d6\u0001\u0000\u0000\u0000\u01d8"+
		"\u0490\u0001\u0000\u0000\u0000\u01d9\u01da\u0005.\u0000\u0000\u01da\u01db"+
		"\u0005\u00be\u0000\u0000\u01db\u01dc\u0005,\u0000\u0000\u01dc\u01dd\u0003"+
		"\u00f2y\u0000\u01dd\u01e0\u0005\u0085\u0000\u0000\u01de\u01e1\u0003\u009e"+
		"O\u0000\u01df\u01e1\u0005\u00b7\u0000\u0000\u01e0\u01de\u0001\u0000\u0000"+
		"\u0000\u01e0\u01df\u0001\u0000\u0000\u0000\u01e1\u0490\u0001\u0000\u0000"+
		"\u0000\u01e2\u01e3\u0005\u0017\u0000\u0000\u01e3\u01e6\u0005\u0104\u0000"+
		"\u0000\u01e4\u01e5\u0005w\u0000\u0000\u01e5\u01e7\u0005^\u0000\u0000\u01e6"+
		"\u01e4\u0001\u0000\u0000\u0000\u01e6\u01e7\u0001\u0000\u0000\u0000\u01e7"+
		"\u01e8\u0001\u0000\u0000\u0000\u01e8\u01e9\u0003\u00f2y\u0000\u01e9\u01ea"+
		"\u0005\u00df\u0000\u0000\u01ea\u01eb\u0005\u010d\u0000\u0000\u01eb\u01ec"+
		"\u0003\u00f2y\u0000\u01ec\u0490\u0001\u0000\u0000\u0000\u01ed\u01ee\u0005"+
		"\u0017\u0000\u0000\u01ee\u01f1\u0005\u0104\u0000\u0000\u01ef\u01f0\u0005"+
		"w\u0000\u0000\u01f0\u01f2\u0005^\u0000\u0000\u01f1\u01ef\u0001\u0000\u0000"+
		"\u0000\u01f1\u01f2\u0001\u0000\u0000\u0000\u01f2\u01f3\u0001\u0000\u0000"+
		"\u0000\u01f3\u01f4\u0003\u00f2y\u0000\u01f4\u01f5\u0005\u0013\u0000\u0000"+
		"\u01f5\u01f9\u0005,\u0000\u0000\u01f6\u01f7\u0005w\u0000\u0000\u01f7\u01f8"+
		"\u0005\u00b6\u0000\u0000\u01f8\u01fa\u0005^\u0000\u0000\u01f9\u01f6\u0001"+
		"\u0000\u0000\u0000\u01f9\u01fa\u0001\u0000\u0000\u0000\u01fa\u01fb\u0001"+
		"\u0000\u0000\u0000\u01fb\u01fc\u0003\u0018\f\u0000\u01fc\u0490\u0001\u0000"+
		"\u0000\u0000\u01fd\u01fe\u0005\u0017\u0000\u0000\u01fe\u0201\u0005\u0104"+
		"\u0000\u0000\u01ff\u0200\u0005w\u0000\u0000\u0200\u0202\u0005^\u0000\u0000"+
		"\u0201\u01ff\u0001\u0000\u0000\u0000\u0201\u0202\u0001\u0000\u0000\u0000"+
		"\u0202\u0203\u0001\u0000\u0000\u0000\u0203\u0204\u0003\u00f2y\u0000\u0204"+
		"\u0205\u0005\u00df\u0000\u0000\u0205\u0208\u0005,\u0000\u0000\u0206\u0207"+
		"\u0005w\u0000\u0000\u0207\u0209\u0005^\u0000\u0000\u0208\u0206\u0001\u0000"+
		"\u0000\u0000\u0208\u0209\u0001\u0000\u0000\u0000\u0209\u020a\u0001\u0000"+
		"\u0000\u0000\u020a\u020b\u0003\u00f2y\u0000\u020b\u020c\u0005\u010d\u0000"+
		"\u0000\u020c\u020d\u0003\u0100\u0080\u0000\u020d\u0490\u0001\u0000\u0000"+
		"\u0000\u020e\u020f\u0005\u0017\u0000\u0000\u020f\u0212\u0005\u0104\u0000"+
		"\u0000\u0210\u0211\u0005w\u0000\u0000\u0211\u0213\u0005^\u0000\u0000\u0212"+
		"\u0210\u0001\u0000\u0000\u0000\u0212\u0213\u0001\u0000\u0000\u0000\u0213"+
		"\u0214\u0001\u0000\u0000\u0000\u0214\u0215\u0003\u00f2y\u0000\u0215\u0216"+
		"\u0005S\u0000\u0000\u0216\u0219\u0005,\u0000\u0000\u0217\u0218\u0005w"+
		"\u0000\u0000\u0218\u021a\u0005^\u0000\u0000\u0219\u0217\u0001\u0000\u0000"+
		"\u0000\u0219\u021a\u0001\u0000\u0000\u0000\u021a\u021b\u0001\u0000\u0000"+
		"\u0000\u021b\u021c\u0003\u00f2y\u0000\u021c\u0490\u0001\u0000\u0000\u0000"+
		"\u021d\u021e\u0005\u0017\u0000\u0000\u021e\u0221\u0005\u0104\u0000\u0000"+
		"\u021f\u0220\u0005w\u0000\u0000\u0220\u0222\u0005^\u0000\u0000\u0221\u021f"+
		"\u0001\u0000\u0000\u0000\u0221\u0222\u0001\u0000\u0000\u0000\u0222\u0223"+
		"\u0001\u0000\u0000\u0000\u0223\u0224\u0003\u00f2y\u0000\u0224\u0225\u0005"+
		"\u0017\u0000\u0000\u0225\u0226\u0005,\u0000\u0000\u0226\u0227\u0003\u00f2"+
		"y\u0000\u0227\u0228\u0005\u00fb\u0000\u0000\u0228\u0229\u0005A\u0000\u0000"+
		"\u0229\u022a\u0005\u0114\u0000\u0000\u022a\u022b\u0003\u00aeW\u0000\u022b"+
		"\u0490\u0001\u0000\u0000\u0000\u022c\u022d\u0005\u0017\u0000\u0000\u022d"+
		"\u0230\u0005\u0104\u0000\u0000\u022e\u022f\u0005w\u0000\u0000\u022f\u0231"+
		"\u0005^\u0000\u0000\u0230\u022e\u0001\u0000\u0000\u0000\u0230\u0231\u0001"+
		"\u0000\u0000\u0000\u0231\u0232\u0001\u0000\u0000\u0000\u0232\u0233\u0003"+
		"\u00f2y\u0000\u0233\u0234\u0005\u0017\u0000\u0000\u0234\u0235\u0005,\u0000"+
		"\u0000\u0235\u0236\u0003\u0100\u0080\u0000\u0236\u0237\u0005S\u0000\u0000"+
		"\u0237\u0238\u0005\u00b6\u0000\u0000\u0238\u0239\u0005\u00b7\u0000\u0000"+
		"\u0239\u0490\u0001\u0000\u0000\u0000\u023a\u023b\u0005\u0017\u0000\u0000"+
		"\u023b\u023c\u0005\u0104\u0000\u0000\u023c\u023d\u0003\u00f2y\u0000\u023d"+
		"\u023e\u0005\u00fb\u0000\u0000\u023e\u023f\u0005\u001f\u0000\u0000\u023f"+
		"\u0240\u0003\u00fa}\u0000\u0240\u0490\u0001\u0000\u0000\u0000\u0241\u0242"+
		"\u0005\u0017\u0000\u0000\u0242\u0243\u0005\u0104\u0000\u0000\u0243\u0244"+
		"\u0003\u00f2y\u0000\u0244\u0245\u0005\u00fb\u0000\u0000\u0245\u0246\u0005"+
		"\u00d8\u0000\u0000\u0246\u0247\u0003\u001e\u000f\u0000\u0247\u0490\u0001"+
		"\u0000\u0000\u0000\u0248\u0249\u0005\u0017\u0000\u0000\u0249\u024a\u0005"+
		"\u0104\u0000\u0000\u024a\u024b\u0003\u00f2y\u0000\u024b\u024c\u0005]\u0000"+
		"\u0000\u024c\u0259\u0003\u0100\u0080\u0000\u024d\u0256\u0005\u0002\u0000"+
		"\u0000\u024e\u0253\u0003\u00d0h\u0000\u024f\u0250\u0005\u0004\u0000\u0000"+
		"\u0250\u0252\u0003\u00d0h\u0000\u0251\u024f\u0001\u0000\u0000\u0000\u0252"+
		"\u0255\u0001\u0000\u0000\u0000\u0253\u0251\u0001\u0000\u0000\u0000\u0253"+
		"\u0254\u0001\u0000\u0000\u0000\u0254\u0257\u0001\u0000\u0000\u0000\u0255"+
		"\u0253\u0001\u0000\u0000\u0000\u0256\u024e\u0001\u0000\u0000\u0000\u0256"+
		"\u0257\u0001\u0000\u0000\u0000\u0257\u0258\u0001\u0000\u0000\u0000\u0258"+
		"\u025a\u0005\u0003\u0000\u0000\u0259\u024d\u0001\u0000\u0000\u0000\u0259"+
		"\u025a\u0001\u0000\u0000\u0000\u025a\u025d\u0001\u0000\u0000\u0000\u025b"+
		"\u025c\u0005\u012d\u0000\u0000\u025c\u025e\u0003\u0080@\u0000\u025d\u025b"+
		"\u0001\u0000\u0000\u0000\u025d\u025e\u0001\u0000\u0000\u0000\u025e\u0490"+
		"\u0001\u0000\u0000\u0000\u025f\u0260\u0005\u0018\u0000\u0000\u0260\u0263"+
		"\u0003\u00f2y\u0000\u0261\u0262\u0005\u0130\u0000\u0000\u0262\u0264\u0003"+
		"\u001c\u000e\u0000\u0263\u0261\u0001\u0000\u0000\u0000\u0263\u0264\u0001"+
		"\u0000\u0000\u0000\u0264\u0490\u0001\u0000\u0000\u0000\u0265\u0268\u0005"+
		"5\u0000\u0000\u0266\u0267\u0005\u00c2\u0000\u0000\u0267\u0269\u0005\u00e2"+
		"\u0000\u0000\u0268\u0266\u0001\u0000\u0000\u0000\u0268\u0269\u0001\u0000"+
		"\u0000\u0000\u0269\u026a\u0001\u0000\u0000\u0000\u026a\u026b\u0005\u00a7"+
		"\u0000\u0000\u026b\u026f\u0005\u012b\u0000\u0000\u026c\u026d\u0005w\u0000"+
		"\u0000\u026d\u026e\u0005\u00b6\u0000\u0000\u026e\u0270\u0005^\u0000\u0000"+
		"\u026f\u026c\u0001\u0000\u0000\u0000\u026f\u0270\u0001\u0000\u0000\u0000"+
		"\u0270\u0271\u0001\u0000\u0000\u0000\u0271\u0275\u0003\u00f2y\u0000\u0272"+
		"\u0273\u0005m\u0000\u0000\u0273\u0274\u0005\u00d0\u0000\u0000\u0274\u0276"+
		"\u0003\u00a8T\u0000\u0275\u0272\u0001\u0000\u0000\u0000\u0275\u0276\u0001"+
		"\u0000\u0000\u0000\u0276\u0279\u0001\u0000\u0000\u0000\u0277\u0278\u0005"+
		".\u0000\u0000\u0278\u027a\u0003\u009eO\u0000\u0279\u0277\u0001\u0000\u0000"+
		"\u0000\u0279\u027a\u0001\u0000\u0000\u0000\u027a\u027d\u0001\u0000\u0000"+
		"\u0000\u027b\u027c\u0005\u0130\u0000\u0000\u027c\u027e\u0003\u001c\u000e"+
		"\u0000\u027d\u027b\u0001\u0000\u0000\u0000\u027d\u027e\u0001\u0000\u0000"+
		"\u0000\u027e\u027f\u0001\u0000\u0000\u0000\u027f\u0280\u0005\u001c\u0000"+
		"\u0000\u0280\u0281\u0003\u000e\u0007\u0000\u0281\u0490\u0001\u0000\u0000"+
		"\u0000\u0282\u0285\u00055\u0000\u0000\u0283\u0284\u0005\u00c2\u0000\u0000"+
		"\u0284\u0286\u0005\u00e2\u0000\u0000\u0285\u0283\u0001\u0000\u0000\u0000"+
		"\u0285\u0286\u0001\u0000\u0000\u0000\u0286\u0287\u0001\u0000\u0000\u0000"+
		"\u0287\u0288\u0005\u012b\u0000\u0000\u0288\u028b\u0003\u00f2y\u0000\u0289"+
		"\u028a\u0005.\u0000\u0000\u028a\u028c\u0003\u009eO\u0000\u028b\u0289\u0001"+
		"\u0000\u0000\u0000\u028b\u028c\u0001\u0000\u0000\u0000\u028c\u028f\u0001"+
		"\u0000\u0000\u0000\u028d\u028e\u0005\u00f6\u0000\u0000\u028e\u0290\u0007"+
		"\u0001\u0000\u0000\u028f\u028d\u0001\u0000\u0000\u0000\u028f\u0290\u0001"+
		"\u0000\u0000\u0000\u0290\u0291\u0001\u0000\u0000\u0000\u0291\u0292\u0005"+
		"\u001c\u0000\u0000\u0292\u0293\u0003\u000e\u0007\u0000\u0293\u0490\u0001"+
		"\u0000\u0000\u0000\u0294\u0295\u0005\u00de\u0000\u0000\u0295\u0296\u0005"+
		"\u00a7\u0000\u0000\u0296\u0297\u0005\u012b\u0000\u0000\u0297\u0490\u0003"+
		"\u00f2y\u0000\u0298\u0299\u0005S\u0000\u0000\u0299\u029a\u0005\u00a7\u0000"+
		"\u0000\u029a\u029d\u0005\u012b\u0000\u0000\u029b\u029c\u0005w\u0000\u0000"+
		"\u029c\u029e\u0005^\u0000\u0000\u029d\u029b\u0001\u0000\u0000\u0000\u029d"+
		"\u029e\u0001\u0000\u0000\u0000\u029e\u029f\u0001\u0000\u0000\u0000\u029f"+
		"\u0490\u0003\u00f2y\u0000\u02a0\u02a1\u0005\u0017\u0000\u0000\u02a1\u02a2"+
		"\u0005\u00a7\u0000\u0000\u02a2\u02a5\u0005\u012b\u0000\u0000\u02a3\u02a4"+
		"\u0005w\u0000\u0000\u02a4\u02a6\u0005^\u0000\u0000\u02a5\u02a3\u0001\u0000"+
		"\u0000\u0000\u02a5\u02a6\u0001\u0000\u0000\u0000\u02a6\u02a7\u0001\u0000"+
		"\u0000\u0000\u02a7\u02a8\u0003\u00f2y\u0000\u02a8\u02a9\u0005\u00df\u0000"+
		"\u0000\u02a9\u02aa\u0005\u010d\u0000\u0000\u02aa\u02ab\u0003\u00f2y\u0000"+
		"\u02ab\u0490\u0001\u0000\u0000\u0000\u02ac\u02ad\u0005\u0017\u0000\u0000"+
		"\u02ad\u02ae\u0005\u00a7\u0000\u0000\u02ae\u02af\u0005\u012b\u0000\u0000"+
		"\u02af\u02b0\u0003\u00f2y\u0000\u02b0\u02b1\u0005\u00fb\u0000\u0000\u02b1"+
		"\u02b2\u0005\u00d8\u0000\u0000\u02b2\u02b3\u0003\u001e\u000f\u0000\u02b3"+
		"\u0490\u0001\u0000\u0000\u0000\u02b4\u02b5\u0005S\u0000\u0000\u02b5\u02b8"+
		"\u0005\u012b\u0000\u0000\u02b6\u02b7\u0005w\u0000\u0000\u02b7\u02b9\u0005"+
		"^\u0000\u0000\u02b8\u02b6\u0001\u0000\u0000\u0000\u02b8\u02b9\u0001\u0000"+
		"\u0000\u0000\u02b9\u02ba\u0001\u0000\u0000\u0000\u02ba\u0490\u0003\u00f2"+
		"y\u0000\u02bb\u02bc\u0005\u0017\u0000\u0000\u02bc\u02bd\u0005\u012b\u0000"+
		"\u0000\u02bd\u02be\u0003\u00f2y\u0000\u02be\u02bf\u0005\u00df\u0000\u0000"+
		"\u02bf\u02c0\u0005\u010d\u0000\u0000\u02c0\u02c1\u0003\u00f2y\u0000\u02c1"+
		"\u0490\u0001\u0000\u0000\u0000\u02c2\u02c3\u0005\u0017\u0000\u0000\u02c3"+
		"\u02c4\u0005\u012b\u0000\u0000\u02c4\u02c5\u0003\u00f2y\u0000\u02c5\u02c6"+
		"\u0005\u00fb\u0000\u0000\u02c6\u02c7\u0005\u001f\u0000\u0000\u02c7\u02c8"+
		"\u0003\u00fa}\u0000\u02c8\u0490\u0001\u0000\u0000\u0000\u02c9\u02ca\u0005"+
		"%\u0000\u0000\u02ca\u02cb\u0003\u00f2y\u0000\u02cb\u02d4\u0005\u0002\u0000"+
		"\u0000\u02cc\u02d1\u0003\u00d0h\u0000\u02cd\u02ce\u0005\u0004\u0000\u0000"+
		"\u02ce\u02d0\u0003\u00d0h\u0000\u02cf\u02cd\u0001\u0000\u0000\u0000\u02d0"+
		"\u02d3\u0001\u0000\u0000\u0000\u02d1\u02cf\u0001\u0000\u0000\u0000\u02d1"+
		"\u02d2\u0001\u0000\u0000\u0000\u02d2\u02d5\u0001\u0000\u0000\u0000\u02d3"+
		"\u02d1\u0001\u0000\u0000\u0000\u02d4\u02cc\u0001\u0000\u0000\u0000\u02d4"+
		"\u02d5\u0001\u0000\u0000\u0000\u02d5\u02d6\u0001\u0000\u0000\u0000\u02d6"+
		"\u02d7\u0005\u0003\u0000\u0000\u02d7\u0490\u0001\u0000\u0000\u0000\u02d8"+
		"\u02db\u00055\u0000\u0000\u02d9\u02da\u0005\u00c2\u0000\u0000\u02da\u02dc"+
		"\u0005\u00e2\u0000\u0000\u02db\u02d9\u0001\u0000\u0000\u0000\u02db\u02dc"+
		"\u0001\u0000\u0000\u0000\u02dc\u02dd\u0001\u0000\u0000\u0000\u02dd\u0490"+
		"\u0003\u00d6k\u0000\u02de\u02df\u0005S\u0000\u0000\u02df\u02e2\u0005k"+
		"\u0000\u0000\u02e0\u02e1\u0005w\u0000\u0000\u02e1\u02e3\u0005^\u0000\u0000"+
		"\u02e2\u02e0\u0001\u0000\u0000\u0000\u02e2\u02e3\u0001\u0000\u0000\u0000"+
		"\u02e3\u02e4\u0001\u0000\u0000\u0000\u02e4\u0490\u0003\u00d8l\u0000\u02e5"+
		"\u02e6\u00055\u0000\u0000\u02e6\u02e7\u0005\u00eb\u0000\u0000\u02e7\u02eb"+
		"\u0003\u0100\u0080\u0000\u02e8\u02e9\u0005\u0130\u0000\u0000\u02e9\u02ea"+
		"\u0005\u0014\u0000\u0000\u02ea\u02ec\u0003\u00f8|\u0000\u02eb\u02e8\u0001"+
		"\u0000\u0000\u0000\u02eb\u02ec\u0001\u0000\u0000\u0000\u02ec\u02ef\u0001"+
		"\u0000\u0000\u0000\u02ed\u02ee\u0005z\u0000\u0000\u02ee\u02f0\u0003\u0100"+
		"\u0080\u0000\u02ef\u02ed\u0001\u0000\u0000\u0000\u02ef\u02f0\u0001\u0000"+
		"\u0000\u0000\u02f0\u0490\u0001\u0000\u0000\u0000\u02f1\u02f2\u0005S\u0000"+
		"\u0000\u02f2\u02f3\u0005\u00eb\u0000\u0000\u02f3\u02f6\u0003\u0100\u0080"+
		"\u0000\u02f4\u02f5\u0005z\u0000\u0000\u02f5\u02f7\u0003\u0100\u0080\u0000"+
		"\u02f6\u02f4\u0001\u0000\u0000\u0000\u02f6\u02f7\u0001\u0000\u0000\u0000"+
		"\u02f7\u0490\u0001\u0000\u0000\u0000\u02f8\u02f9\u0005n\u0000\u0000\u02f9"+
		"\u02fe\u0003\u00fe\u007f\u0000\u02fa\u02fb\u0005\u0004\u0000\u0000\u02fb"+
		"\u02fd\u0003\u00fe\u007f\u0000\u02fc\u02fa\u0001\u0000\u0000\u0000\u02fd"+
		"\u0300\u0001\u0000\u0000\u0000\u02fe\u02fc\u0001\u0000\u0000\u0000\u02fe"+
		"\u02ff\u0001\u0000\u0000\u0000\u02ff\u0301\u0001\u0000\u0000\u0000\u0300"+
		"\u02fe\u0001\u0000\u0000\u0000\u0301\u0302\u0005\u010d\u0000\u0000\u0302"+
		"\u0307\u0003\u00fa}\u0000\u0303\u0304\u0005\u0004\u0000\u0000\u0304\u0306"+
		"\u0003\u00fa}\u0000\u0305\u0303\u0001\u0000\u0000\u0000\u0306\u0309\u0001"+
		"\u0000\u0000\u0000\u0307\u0305\u0001\u0000\u0000\u0000\u0307\u0308\u0001"+
		"\u0000\u0000\u0000\u0308\u030d\u0001\u0000\u0000\u0000\u0309\u0307\u0001"+
		"\u0000\u0000\u0000\u030a\u030b\u0005\u0130\u0000\u0000\u030b\u030c\u0005"+
		"\u0014\u0000\u0000\u030c\u030e\u0005\u00c1\u0000\u0000\u030d\u030a\u0001"+
		"\u0000\u0000\u0000\u030d\u030e\u0001\u0000\u0000\u0000\u030e\u0312\u0001"+
		"\u0000\u0000\u0000\u030f\u0310\u0005o\u0000\u0000\u0310\u0311\u0005$\u0000"+
		"\u0000\u0311\u0313\u0003\u00f8|\u0000\u0312\u030f\u0001\u0000\u0000\u0000"+
		"\u0312\u0313\u0001\u0000\u0000\u0000\u0313\u0316\u0001\u0000\u0000\u0000"+
		"\u0314\u0315\u0005z\u0000\u0000\u0315\u0317\u0003\u0100\u0080\u0000\u0316"+
		"\u0314\u0001\u0000\u0000\u0000\u0316\u0317\u0001\u0000\u0000\u0000\u0317"+
		"\u0490\u0001\u0000\u0000\u0000\u0318\u0323\u0005n\u0000\u0000\u0319\u031e"+
		"\u0003\u00fe\u007f\u0000\u031a\u031b\u0005\u0004\u0000\u0000\u031b\u031d"+
		"\u0003\u00fe\u007f\u0000\u031c\u031a\u0001\u0000\u0000\u0000\u031d\u0320"+
		"\u0001\u0000\u0000\u0000\u031e\u031c\u0001\u0000\u0000\u0000\u031e\u031f"+
		"\u0001\u0000\u0000\u0000\u031f\u0324\u0001\u0000\u0000\u0000\u0320\u031e"+
		"\u0001\u0000\u0000\u0000\u0321\u0322\u0005\u0016\u0000\u0000\u0322\u0324"+
		"\u0005\u00d7\u0000\u0000\u0323\u0319\u0001\u0000\u0000\u0000\u0323\u0321"+
		"\u0001\u0000\u0000\u0000\u0324\u0325\u0001\u0000\u0000\u0000\u0325\u0326"+
		"\u0005\u00be\u0000\u0000\u0326\u0327\u0003\u00f0x\u0000\u0327\u0328\u0005"+
		"\u010d\u0000\u0000\u0328\u032c\u0003\u00fa}\u0000\u0329\u032a\u0005\u0130"+
		"\u0000\u0000\u032a\u032b\u0005n\u0000\u0000\u032b\u032d\u0005\u00c1\u0000"+
		"\u0000\u032c\u0329\u0001\u0000\u0000\u0000\u032c\u032d\u0001\u0000\u0000"+
		"\u0000\u032d\u0490\u0001\u0000\u0000\u0000\u032e\u0332\u0005\u00e9\u0000"+
		"\u0000\u032f\u0330\u0005\u0014\u0000\u0000\u0330\u0331\u0005\u00c1\u0000"+
		"\u0000\u0331\u0333\u0005g\u0000\u0000\u0332\u032f\u0001\u0000\u0000\u0000"+
		"\u0332\u0333\u0001\u0000\u0000\u0000\u0333\u0334\u0001\u0000\u0000\u0000"+
		"\u0334\u0339\u0003\u00fe\u007f\u0000\u0335\u0336\u0005\u0004\u0000\u0000"+
		"\u0336\u0338\u0003\u00fe\u007f\u0000\u0337\u0335\u0001\u0000\u0000\u0000"+
		"\u0338\u033b\u0001\u0000\u0000\u0000\u0339\u0337\u0001\u0000\u0000\u0000"+
		"\u0339\u033a\u0001\u0000\u0000\u0000\u033a\u033c\u0001\u0000\u0000\u0000"+
		"\u033b\u0339\u0001\u0000\u0000\u0000\u033c\u033d\u0005i\u0000\u0000\u033d"+
		"\u0342\u0003\u00fa}\u0000\u033e\u033f\u0005\u0004\u0000\u0000\u033f\u0341"+
		"\u0003\u00fa}\u0000\u0340\u033e\u0001\u0000\u0000\u0000\u0341\u0344\u0001"+
		"\u0000\u0000\u0000\u0342\u0340\u0001\u0000\u0000\u0000\u0342\u0343\u0001"+
		"\u0000\u0000\u0000\u0343\u0348\u0001\u0000\u0000\u0000\u0344\u0342\u0001"+
		"\u0000\u0000\u0000\u0345\u0346\u0005o\u0000\u0000\u0346\u0347\u0005$\u0000"+
		"\u0000\u0347\u0349\u0003\u00f8|\u0000\u0348\u0345\u0001\u0000\u0000\u0000"+
		"\u0348\u0349\u0001\u0000\u0000\u0000\u0349\u034c\u0001\u0000\u0000\u0000"+
		"\u034a\u034b\u0005z\u0000\u0000\u034b\u034d\u0003\u0100\u0080\u0000\u034c"+
		"\u034a\u0001\u0000\u0000\u0000\u034c\u034d\u0001\u0000\u0000\u0000\u034d"+
		"\u0490\u0001\u0000\u0000\u0000\u034e\u0352\u0005\u00e9\u0000\u0000\u034f"+
		"\u0350\u0005n\u0000\u0000\u0350\u0351\u0005\u00c1\u0000\u0000\u0351\u0353"+
		"\u0005g\u0000\u0000\u0352\u034f\u0001\u0000\u0000\u0000\u0352\u0353\u0001"+
		"\u0000\u0000\u0000\u0353\u035e\u0001\u0000\u0000\u0000\u0354\u0359\u0003"+
		"\u00fe\u007f\u0000\u0355\u0356\u0005\u0004\u0000\u0000\u0356\u0358\u0003"+
		"\u00fe\u007f\u0000\u0357\u0355\u0001\u0000\u0000\u0000\u0358\u035b\u0001"+
		"\u0000\u0000\u0000\u0359\u0357\u0001\u0000\u0000\u0000\u0359\u035a\u0001"+
		"\u0000\u0000\u0000\u035a\u035f\u0001\u0000\u0000\u0000\u035b\u0359\u0001"+
		"\u0000\u0000\u0000\u035c\u035d\u0005\u0016\u0000\u0000\u035d\u035f\u0005"+
		"\u00d7\u0000\u0000\u035e\u0354\u0001\u0000\u0000\u0000\u035e\u035c\u0001"+
		"\u0000\u0000\u0000\u035f\u0360\u0001\u0000\u0000\u0000\u0360\u0361\u0005"+
		"\u00be\u0000\u0000\u0361\u0362\u0003\u00f0x\u0000\u0362\u0363\u0005i\u0000"+
		"\u0000\u0363\u0364\u0003\u00fa}\u0000\u0364\u0490\u0001\u0000\u0000\u0000"+
		"\u0365\u0370\u0005J\u0000\u0000\u0366\u036b\u0003\u00ecv\u0000\u0367\u0368"+
		"\u0005\u0004\u0000\u0000\u0368\u036a\u0003\u00ecv\u0000\u0369\u0367\u0001"+
		"\u0000\u0000\u0000\u036a\u036d\u0001\u0000\u0000\u0000\u036b\u0369\u0001"+
		"\u0000\u0000\u0000\u036b\u036c\u0001\u0000\u0000\u0000\u036c\u0371\u0001"+
		"\u0000\u0000\u0000\u036d\u036b\u0001\u0000\u0000\u0000\u036e\u036f\u0005"+
		"\u0016\u0000\u0000\u036f\u0371\u0005\u00d7\u0000\u0000\u0370\u0366\u0001"+
		"\u0000\u0000\u0000\u0370\u036e\u0001\u0000\u0000\u0000\u0371\u0372\u0001"+
		"\u0000\u0000\u0000\u0372\u0373\u0005\u00be\u0000\u0000\u0373\u0374\u0003"+
		"\u00f0x\u0000\u0374\u0375\u0005\u010d\u0000\u0000\u0375\u0376\u0003\u00fa"+
		"}\u0000\u0376\u0490\u0001\u0000\u0000\u0000\u0377\u0378\u0005\u00fb\u0000"+
		"\u0000\u0378\u037c\u0005\u00eb\u0000\u0000\u0379\u037d\u0005\u0016\u0000"+
		"\u0000\u037a\u037d\u0005\u00b4\u0000\u0000\u037b\u037d\u0003\u0100\u0080"+
		"\u0000\u037c\u0379\u0001\u0000\u0000\u0000\u037c\u037a\u0001\u0000\u0000"+
		"\u0000\u037c\u037b\u0001\u0000\u0000\u0000\u037d\u0380\u0001\u0000\u0000"+
		"\u0000\u037e\u037f\u0005z\u0000\u0000\u037f\u0381\u0003\u0100\u0080\u0000"+
		"\u0380\u037e\u0001\u0000\u0000\u0000\u0380\u0381\u0001\u0000\u0000\u0000"+
		"\u0381\u0490\u0001\u0000\u0000\u0000\u0382\u0383\u0005\u00fd\u0000\u0000"+
		"\u0383\u0386\u0005p\u0000\u0000\u0384\u0385\u0005\u00be\u0000\u0000\u0385"+
		"\u0387\u0003\u00f0x\u0000\u0386\u0384\u0001\u0000\u0000\u0000\u0386\u0387"+
		"\u0001\u0000\u0000\u0000\u0387\u0490\u0001\u0000\u0000\u0000\u0388\u0394"+
		"\u0005_\u0000\u0000\u0389\u038a\u0005\u0002\u0000\u0000\u038a\u038f\u0003"+
		"\u00cae\u0000\u038b\u038c\u0005\u0004\u0000\u0000\u038c\u038e\u0003\u00ca"+
		"e\u0000\u038d\u038b\u0001\u0000\u0000\u0000\u038e\u0391\u0001\u0000\u0000"+
		"\u0000\u038f\u038d\u0001\u0000\u0000\u0000\u038f\u0390\u0001\u0000\u0000"+
		"\u0000\u0390\u0392\u0001\u0000\u0000\u0000\u0391\u038f\u0001\u0000\u0000"+
		"\u0000\u0392\u0393\u0005\u0003\u0000\u0000\u0393\u0395\u0001\u0000\u0000"+
		"\u0000\u0394\u0389\u0001\u0000\u0000\u0000\u0394\u0395\u0001\u0000\u0000"+
		"\u0000\u0395\u0396\u0001\u0000\u0000\u0000\u0396\u0490\u0003\f\u0006\u0000"+
		"\u0397\u0398\u0005_\u0000\u0000\u0398\u039a\u0005\u0018\u0000\u0000\u0399"+
		"\u039b\u0005\u0129\u0000\u0000\u039a\u0399\u0001\u0000\u0000\u0000\u039a"+
		"\u039b\u0001\u0000\u0000\u0000\u039b\u039c\u0001\u0000\u0000\u0000\u039c"+
		"\u0490\u0003\f\u0006\u0000\u039d\u039e\u0005\u00fd\u0000\u0000\u039e\u039f"+
		"\u00055\u0000\u0000\u039f\u03a0\u0005\u0104\u0000\u0000\u03a0\u0490\u0003"+
		"\u00f2y\u0000\u03a1\u03a2\u0005\u00fd\u0000\u0000\u03a2\u03a3\u00055\u0000"+
		"\u0000\u03a3\u03a4\u0005\u00f3\u0000\u0000\u03a4\u0490\u0003\u00f2y\u0000"+
		"\u03a5\u03a6\u0005\u00fd\u0000\u0000\u03a6\u03a7\u00055\u0000\u0000\u03a7"+
		"\u03a8\u0005\u012b\u0000\u0000\u03a8\u0490\u0003\u00f2y\u0000\u03a9\u03aa"+
		"\u0005\u00fd\u0000\u0000\u03aa\u03ab\u00055\u0000\u0000\u03ab\u03ac\u0005"+
		"\u00a7\u0000\u0000\u03ac\u03ad\u0005\u012b\u0000\u0000\u03ad\u0490\u0003"+
		"\u00f2y\u0000\u03ae\u03af\u0005\u00fd\u0000\u0000\u03af\u03b2\u0005\u0105"+
		"\u0000\u0000\u03b0\u03b1\u0007\u0002\u0000\u0000\u03b1\u03b3\u0003\u00f2"+
		"y\u0000\u03b2\u03b0\u0001\u0000\u0000\u0000\u03b2\u03b3\u0001\u0000\u0000"+
		"\u0000\u03b3\u03ba\u0001\u0000\u0000\u0000\u03b4\u03b5\u0005\u009a\u0000"+
		"\u0000\u03b5\u03b8\u0003\u009eO\u0000\u03b6\u03b7\u0005Z\u0000\u0000\u03b7"+
		"\u03b9\u0003\u009eO\u0000\u03b8\u03b6\u0001\u0000\u0000\u0000\u03b8\u03b9"+
		"\u0001\u0000\u0000\u0000\u03b9\u03bb\u0001\u0000\u0000\u0000\u03ba\u03b4"+
		"\u0001\u0000\u0000\u0000\u03ba\u03bb\u0001\u0000\u0000\u0000\u03bb\u0490"+
		"\u0001\u0000\u0000\u0000\u03bc\u03bd\u0005\u00fd\u0000\u0000\u03bd\u03c0"+
		"\u0005\u00f4\u0000\u0000\u03be\u03bf\u0007\u0002\u0000\u0000\u03bf\u03c1"+
		"\u0003\u0100\u0080\u0000\u03c0\u03be\u0001\u0000\u0000\u0000\u03c0\u03c1"+
		"\u0001\u0000\u0000\u0000\u03c1\u03c8\u0001\u0000\u0000\u0000\u03c2\u03c3"+
		"\u0005\u009a\u0000\u0000\u03c3\u03c6\u0003\u009eO\u0000\u03c4\u03c5\u0005"+
		"Z\u0000\u0000\u03c5\u03c7\u0003\u009eO\u0000\u03c6\u03c4\u0001\u0000\u0000"+
		"\u0000\u03c6\u03c7\u0001\u0000\u0000\u0000\u03c7\u03c9\u0001\u0000\u0000"+
		"\u0000\u03c8\u03c2\u0001\u0000\u0000\u0000\u03c8\u03c9\u0001\u0000\u0000"+
		"\u0000\u03c9\u0490\u0001\u0000\u0000\u0000\u03ca\u03cb\u0005\u00fd\u0000"+
		"\u0000\u03cb\u03d2\u0005+\u0000\u0000\u03cc\u03cd\u0005\u009a\u0000\u0000"+
		"\u03cd\u03d0\u0003\u009eO\u0000\u03ce\u03cf\u0005Z\u0000\u0000\u03cf\u03d1"+
		"\u0003\u009eO\u0000\u03d0\u03ce\u0001\u0000\u0000\u0000\u03d0\u03d1\u0001"+
		"\u0000\u0000\u0000\u03d1\u03d3\u0001\u0000\u0000\u0000\u03d2\u03cc\u0001"+
		"\u0000\u0000\u0000\u03d2\u03d3\u0001\u0000\u0000\u0000\u03d3\u0490\u0001"+
		"\u0000\u0000\u0000\u03d4\u03d5\u0005\u00fd\u0000\u0000\u03d5\u03d6\u0005"+
		"-\u0000\u0000\u03d6\u03d7\u0007\u0002\u0000\u0000\u03d7\u03de\u0003\u00f2"+
		"y\u0000\u03d8\u03d9\u0005\u009a\u0000\u0000\u03d9\u03dc\u0003\u009eO\u0000"+
		"\u03da\u03db\u0005Z\u0000\u0000\u03db\u03dd\u0003\u009eO\u0000\u03dc\u03da"+
		"\u0001\u0000\u0000\u0000\u03dc\u03dd\u0001\u0000\u0000\u0000\u03dd\u03df"+
		"\u0001\u0000\u0000\u0000\u03de\u03d8\u0001\u0000\u0000\u0000\u03de\u03df"+
		"\u0001\u0000\u0000\u0000\u03df\u0490\u0001\u0000\u0000\u0000\u03e0\u03e1"+
		"\u0005\u00fd\u0000\u0000\u03e1\u03e2\u0005\u0100\u0000\u0000\u03e2\u03e3"+
		"\u0005g\u0000\u0000\u03e3\u0490\u0003\u00f2y\u0000\u03e4\u03e5\u0005\u00fd"+
		"\u0000\u0000\u03e5\u03e6\u0005\u0100\u0000\u0000\u03e6\u03e7\u0005g\u0000"+
		"\u0000\u03e7\u03e8\u0005\u0002\u0000\u0000\u03e8\u03e9\u0003\u000e\u0007"+
		"\u0000\u03e9\u03ea\u0005\u0003\u0000\u0000\u03ea\u0490\u0001\u0000\u0000"+
		"\u0000\u03eb\u03ed\u0005\u00fd\u0000\u0000\u03ec\u03ee\u00058\u0000\u0000"+
		"\u03ed\u03ec\u0001\u0000\u0000\u0000\u03ed\u03ee\u0001\u0000\u0000\u0000"+
		"\u03ee\u03ef\u0001\u0000\u0000\u0000\u03ef\u03f2\u0005\u00ec\u0000\u0000"+
		"\u03f0\u03f1\u0007\u0002\u0000\u0000\u03f1\u03f3\u0003\u0100\u0080\u0000"+
		"\u03f2\u03f0\u0001\u0000\u0000\u0000\u03f2\u03f3\u0001\u0000\u0000\u0000"+
		"\u03f3\u0490\u0001\u0000\u0000\u0000\u03f4\u03f5\u0005\u00fd\u0000\u0000"+
		"\u03f5\u03f6\u0005\u00eb\u0000\u0000\u03f6\u03f9\u0005p\u0000\u0000\u03f7"+
		"\u03f8\u0007\u0002\u0000\u0000\u03f8\u03fa\u0003\u0100\u0080\u0000\u03f9"+
		"\u03f7\u0001\u0000\u0000\u0000\u03f9\u03fa\u0001\u0000\u0000\u0000\u03fa"+
		"\u0490\u0001\u0000\u0000\u0000\u03fb\u03fc\u0005L\u0000\u0000\u03fc\u0490"+
		"\u0003\u00f2y\u0000\u03fd\u03fe\u0005K\u0000\u0000\u03fe\u0490\u0003\u00f2"+
		"y\u0000\u03ff\u0400\u0005\u00fd\u0000\u0000\u0400\u0403\u0005l\u0000\u0000"+
		"\u0401\u0402\u0007\u0002\u0000\u0000\u0402\u0404\u0003\u00f2y\u0000\u0403"+
		"\u0401\u0001\u0000\u0000\u0000\u0403\u0404\u0001\u0000\u0000\u0000\u0404"+
		"\u040b\u0001\u0000\u0000\u0000\u0405\u0406\u0005\u009a\u0000\u0000\u0406"+
		"\u0409\u0003\u009eO\u0000\u0407\u0408\u0005Z\u0000\u0000\u0408\u040a\u0003"+
		"\u009eO\u0000\u0409\u0407\u0001\u0000\u0000\u0000\u0409\u040a\u0001\u0000"+
		"\u0000\u0000\u040a\u040c\u0001\u0000\u0000\u0000\u040b\u0405\u0001\u0000"+
		"\u0000\u0000\u040b\u040c\u0001\u0000\u0000\u0000\u040c\u0490\u0001\u0000"+
		"\u0000\u0000\u040d\u040e\u0005\u00fd\u0000\u0000\u040e\u0415\u0005\u00fa"+
		"\u0000\u0000\u040f\u0410\u0005\u009a\u0000\u0000\u0410\u0413\u0003\u009e"+
		"O\u0000\u0411\u0412\u0005Z\u0000\u0000\u0412\u0414\u0003\u009eO\u0000"+
		"\u0413\u0411\u0001\u0000\u0000\u0000\u0413\u0414\u0001\u0000\u0000\u0000"+
		"\u0414\u0416\u0001\u0000\u0000\u0000\u0415\u040f\u0001\u0000\u0000\u0000"+
		"\u0415\u0416\u0001\u0000\u0000\u0000\u0416\u0490\u0001\u0000\u0000\u0000"+
		"\u0417\u0418\u0005\u00fb\u0000\u0000\u0418\u0419\u0005\u00fa\u0000\u0000"+
		"\u0419\u041a\u0005\u001f\u0000\u0000\u041a\u0490\u0003\u0104\u0082\u0000"+
		"\u041b\u041c\u0005\u00e3\u0000\u0000\u041c\u041d\u0005\u00fa\u0000\u0000"+
		"\u041d\u0490\u0005\u001f\u0000\u0000\u041e\u041f\u0005\u00fb\u0000\u0000"+
		"\u041f\u0420\u0005\u00fa\u0000\u0000\u0420\u0421\u0003\u00f2y\u0000\u0421"+
		"\u0422\u0005\u0138\u0000\u0000\u0422\u0423\u0003~?\u0000\u0423\u0490\u0001"+
		"\u0000\u0000\u0000\u0424\u0425\u0005\u00e3\u0000\u0000\u0425\u0426\u0005"+
		"\u00fa\u0000\u0000\u0426\u0490\u0003\u00f2y\u0000\u0427\u0428\u0005\u00ff"+
		"\u0000\u0000\u0428\u0431\u0005\u010f\u0000\u0000\u0429\u042e\u0003\u00cc"+
		"f\u0000\u042a\u042b\u0005\u0004\u0000\u0000\u042b\u042d\u0003\u00ccf\u0000"+
		"\u042c\u042a\u0001\u0000\u0000\u0000\u042d\u0430\u0001\u0000\u0000\u0000"+
		"\u042e\u042c\u0001\u0000\u0000\u0000\u042e\u042f\u0001\u0000\u0000\u0000"+
		"\u042f\u0432\u0001\u0000\u0000\u0000\u0430\u042e\u0001\u0000\u0000\u0000"+
		"\u0431\u0429\u0001\u0000\u0000\u0000\u0431\u0432\u0001\u0000\u0000\u0000"+
		"\u0432\u0490\u0001\u0000\u0000\u0000\u0433\u0435\u0005/\u0000\u0000\u0434"+
		"\u0436\u0005\u0133\u0000\u0000\u0435\u0434\u0001\u0000\u0000\u0000\u0435"+
		"\u0436\u0001\u0000\u0000\u0000\u0436\u0490\u0001\u0000\u0000\u0000\u0437"+
		"\u0439\u0005\u00ed\u0000\u0000\u0438\u043a\u0005\u0133\u0000\u0000\u0439"+
		"\u0438\u0001\u0000\u0000\u0000\u0439\u043a\u0001\u0000\u0000\u0000\u043a"+
		"\u0490\u0001\u0000\u0000\u0000\u043b\u043c\u0005\u00d6\u0000\u0000\u043c"+
		"\u043d\u0003\u0100\u0080\u0000\u043d\u043e\u0005i\u0000\u0000\u043e\u043f"+
		"\u0003\f\u0006\u0000\u043f\u0490\u0001\u0000\u0000\u0000\u0440\u0441\u0005"+
		"D\u0000\u0000\u0441\u0442\u0005\u00d6\u0000\u0000\u0442\u0490\u0003\u0100"+
		"\u0080\u0000\u0443\u0444\u0005]\u0000\u0000\u0444\u044e\u0003\u0100\u0080"+
		"\u0000\u0445\u0446\u0005\u0122\u0000\u0000\u0446\u044b\u0003~?\u0000\u0447"+
		"\u0448\u0005\u0004\u0000\u0000\u0448\u044a\u0003~?\u0000\u0449\u0447\u0001"+
		"\u0000\u0000\u0000\u044a\u044d\u0001\u0000\u0000\u0000\u044b\u0449\u0001"+
		"\u0000\u0000\u0000\u044b\u044c\u0001\u0000\u0000\u0000\u044c\u044f\u0001"+
		"\u0000\u0000\u0000\u044d\u044b\u0001\u0000\u0000\u0000\u044e\u0445\u0001"+
		"\u0000\u0000\u0000\u044e\u044f\u0001\u0000\u0000\u0000\u044f\u0490\u0001"+
		"\u0000\u0000\u0000\u0450\u0451\u0005]\u0000\u0000\u0451\u0452\u0005y\u0000"+
		"\u0000\u0452\u045c\u0003\u009eO\u0000\u0453\u0454\u0005\u0122\u0000\u0000"+
		"\u0454\u0459\u0003~?\u0000\u0455\u0456\u0005\u0004\u0000\u0000\u0456\u0458"+
		"\u0003~?\u0000\u0457\u0455\u0001\u0000\u0000\u0000\u0458\u045b\u0001\u0000"+
		"\u0000\u0000\u0459\u0457\u0001\u0000\u0000\u0000\u0459\u045a\u0001\u0000"+
		"\u0000\u0000\u045a\u045d\u0001\u0000\u0000\u0000\u045b\u0459\u0001\u0000"+
		"\u0000\u0000\u045c\u0453\u0001\u0000\u0000\u0000\u045c\u045d\u0001\u0000"+
		"\u0000\u0000\u045d\u0490\u0001\u0000\u0000\u0000\u045e\u045f\u0005L\u0000"+
		"\u0000\u045f\u0460\u0005~\u0000\u0000\u0460\u0490\u0003\u0100\u0080\u0000"+
		"\u0461\u0462\u0005L\u0000\u0000\u0462\u0463\u0005\u00c6\u0000\u0000\u0463"+
		"\u0490\u0003\u0100\u0080\u0000\u0464\u0465\u0005\u00fb\u0000\u0000\u0465"+
		"\u0466\u0005\u00cd\u0000\u0000\u0466\u0490\u0003\u00d4j\u0000\u0467\u0468"+
		"\u0005\u00fb\u0000\u0000\u0468\u0469\u0005\u010b\u0000\u0000\u0469\u046c"+
		"\u0005\u0137\u0000\u0000\u046a\u046d\u0005\u009d\u0000\u0000\u046b\u046d"+
		"\u0003~?\u0000\u046c\u046a\u0001\u0000\u0000\u0000\u046c\u046b\u0001\u0000"+
		"\u0000\u0000\u046d\u0490\u0001\u0000\u0000\u0000\u046e\u046f\u0005\u011f"+
		"\u0000\u0000\u046f\u0470\u0003\u00f2y\u0000\u0470\u0471\u0005\u00fb\u0000"+
		"\u0000\u0471\u0476\u0003\u00c8d\u0000\u0472\u0473\u0005\u0004\u0000\u0000"+
		"\u0473\u0475\u0003\u00c8d\u0000\u0474\u0472\u0001\u0000\u0000\u0000\u0475"+
		"\u0478\u0001\u0000\u0000\u0000\u0476\u0474\u0001\u0000\u0000\u0000\u0476"+
		"\u0477\u0001\u0000\u0000\u0000\u0477\u047b\u0001\u0000\u0000\u0000\u0478"+
		"\u0476\u0001\u0000\u0000\u0000\u0479\u047a\u0005\u012d\u0000\u0000\u047a"+
		"\u047c\u0003\u0080@\u0000\u047b\u0479\u0001\u0000\u0000\u0000\u047b\u047c"+
		"\u0001\u0000\u0000\u0000\u047c\u0490\u0001\u0000\u0000\u0000\u047d\u047e"+
		"\u0005\u00a9\u0000\u0000\u047e\u047f\u0005\u0082\u0000\u0000\u047f\u0484"+
		"\u0003\u00f2y\u0000\u0480\u0482\u0005\u001c\u0000\u0000\u0481\u0480\u0001"+
		"\u0000\u0000\u0000\u0481\u0482\u0001\u0000\u0000\u0000\u0482\u0483\u0001"+
		"\u0000\u0000\u0000\u0483\u0485\u0003\u0100\u0080\u0000\u0484\u0481\u0001"+
		"\u0000\u0000\u0000\u0484\u0485\u0001\u0000\u0000\u0000\u0485\u0486\u0001"+
		"\u0000\u0000\u0000\u0486\u0487\u0005\u0122\u0000\u0000\u0487\u0488\u0003"+
		"B!\u0000\u0488\u0489\u0005\u00be\u0000\u0000\u0489\u048b\u0003~?\u0000"+
		"\u048a\u048c\u0003\u00b8\\\u0000\u048b\u048a\u0001\u0000\u0000\u0000\u048c"+
		"\u048d\u0001\u0000\u0000\u0000\u048d\u048b\u0001\u0000\u0000\u0000\u048d"+
		"\u048e\u0001\u0000\u0000\u0000\u048e\u0490\u0001\u0000\u0000\u0000\u048f"+
		"\u011a\u0001\u0000\u0000\u0000\u048f\u011b\u0001\u0000\u0000\u0000\u048f"+
		"\u011d\u0001\u0000\u0000\u0000\u048f\u0122\u0001\u0000\u0000\u0000\u048f"+
		"\u0138\u0001\u0000\u0000\u0000\u048f\u0142\u0001\u0000\u0000\u0000\u048f"+
		"\u0152\u0001\u0000\u0000\u0000\u048f\u015c\u0001\u0000\u0000\u0000\u048f"+
		"\u0163\u0001\u0000\u0000\u0000\u048f\u016a\u0001\u0000\u0000\u0000\u048f"+
		"\u0190\u0001\u0000\u0000\u0000\u048f\u01ae\u0001\u0000\u0000\u0000\u048f"+
		"\u01b5\u0001\u0000\u0000\u0000\u048f\u01bd\u0001\u0000\u0000\u0000\u048f"+
		"\u01c4\u0001\u0000\u0000\u0000\u048f\u01c7\u0001\u0000\u0000\u0000\u048f"+
		"\u01d0\u0001\u0000\u0000\u0000\u048f\u01d9\u0001\u0000\u0000\u0000\u048f"+
		"\u01e2\u0001\u0000\u0000\u0000\u048f\u01ed\u0001\u0000\u0000\u0000\u048f"+
		"\u01fd\u0001\u0000\u0000\u0000\u048f\u020e\u0001\u0000\u0000\u0000\u048f"+
		"\u021d\u0001\u0000\u0000\u0000\u048f\u022c\u0001\u0000\u0000\u0000\u048f"+
		"\u023a\u0001\u0000\u0000\u0000\u048f\u0241\u0001\u0000\u0000\u0000\u048f"+
		"\u0248\u0001\u0000\u0000\u0000\u048f\u025f\u0001\u0000\u0000\u0000\u048f"+
		"\u0265\u0001\u0000\u0000\u0000\u048f\u0282\u0001\u0000\u0000\u0000\u048f"+
		"\u0294\u0001\u0000\u0000\u0000\u048f\u0298\u0001\u0000\u0000\u0000\u048f"+
		"\u02a0\u0001\u0000\u0000\u0000\u048f\u02ac\u0001\u0000\u0000\u0000\u048f"+
		"\u02b4\u0001\u0000\u0000\u0000\u048f\u02bb\u0001\u0000\u0000\u0000\u048f"+
		"\u02c2\u0001\u0000\u0000\u0000\u048f\u02c9\u0001\u0000\u0000\u0000\u048f"+
		"\u02d8\u0001\u0000\u0000\u0000\u048f\u02de\u0001\u0000\u0000\u0000\u048f"+
		"\u02e5\u0001\u0000\u0000\u0000\u048f\u02f1\u0001\u0000\u0000\u0000\u048f"+
		"\u02f8\u0001\u0000\u0000\u0000\u048f\u0318\u0001\u0000\u0000\u0000\u048f"+
		"\u032e\u0001\u0000\u0000\u0000\u048f\u034e\u0001\u0000\u0000\u0000\u048f"+
		"\u0365\u0001\u0000\u0000\u0000\u048f\u0377\u0001\u0000\u0000\u0000\u048f"+
		"\u0382\u0001\u0000\u0000\u0000\u048f\u0388\u0001\u0000\u0000\u0000\u048f"+
		"\u0397\u0001\u0000\u0000\u0000\u048f\u039d\u0001\u0000\u0000\u0000\u048f"+
		"\u03a1\u0001\u0000\u0000\u0000\u048f\u03a5\u0001\u0000\u0000\u0000\u048f"+
		"\u03a9\u0001\u0000\u0000\u0000\u048f\u03ae\u0001\u0000\u0000\u0000\u048f"+
		"\u03bc\u0001\u0000\u0000\u0000\u048f\u03ca\u0001\u0000\u0000\u0000\u048f"+
		"\u03d4\u0001\u0000\u0000\u0000\u048f\u03e0\u0001\u0000\u0000\u0000\u048f"+
		"\u03e4\u0001\u0000\u0000\u0000\u048f\u03eb\u0001\u0000\u0000\u0000\u048f"+
		"\u03f4\u0001\u0000\u0000\u0000\u048f\u03fb\u0001\u0000\u0000\u0000\u048f"+
		"\u03fd\u0001\u0000\u0000\u0000\u048f\u03ff\u0001\u0000\u0000\u0000\u048f"+
		"\u040d\u0001\u0000\u0000\u0000\u048f\u0417\u0001\u0000\u0000\u0000\u048f"+
		"\u041b\u0001\u0000\u0000\u0000\u048f\u041e\u0001\u0000\u0000\u0000\u048f"+
		"\u0424\u0001\u0000\u0000\u0000\u048f\u0427\u0001\u0000\u0000\u0000\u048f"+
		"\u0433\u0001\u0000\u0000\u0000\u048f\u0437\u0001\u0000\u0000\u0000\u048f"+
		"\u043b\u0001\u0000\u0000\u0000\u048f\u0440\u0001\u0000\u0000\u0000\u048f"+
		"\u0443\u0001\u0000\u0000\u0000\u048f\u0450\u0001\u0000\u0000\u0000\u048f"+
		"\u045e\u0001\u0000\u0000\u0000\u048f\u0461\u0001\u0000\u0000\u0000\u048f"+
		"\u0464\u0001\u0000\u0000\u0000\u048f\u0467\u0001\u0000\u0000\u0000\u048f"+
		"\u046e\u0001\u0000\u0000\u0000\u048f\u047d\u0001\u0000\u0000\u0000\u0490"+
		"\r\u0001\u0000\u0000\u0000\u0491\u0493\u0003\u0010\b\u0000\u0492\u0491"+
		"\u0001\u0000\u0000\u0000\u0492\u0493\u0001\u0000\u0000\u0000\u0493\u0494"+
		"\u0001\u0000\u0000\u0000\u0494\u0495\u0003\u0012\t\u0000\u0495\u000f\u0001"+
		"\u0000\u0000\u0000\u0496\u0497\u0005\u0130\u0000\u0000\u0497\u049c\u0003"+
		"\u00d6k\u0000\u0498\u0499\u0005\u0004\u0000\u0000\u0499\u049b\u0003\u00d6"+
		"k\u0000\u049a\u0498\u0001\u0000\u0000\u0000\u049b\u049e\u0001\u0000\u0000"+
		"\u0000\u049c\u049a\u0001\u0000\u0000\u0000\u049c\u049d\u0001\u0000\u0000"+
		"\u0000\u049d\u0011\u0001\u0000\u0000\u0000\u049e\u049c\u0001\u0000\u0000"+
		"\u0000\u049f\u04a1\u0003\u0014\n\u0000\u04a0\u049f\u0001\u0000\u0000\u0000"+
		"\u04a0\u04a1\u0001\u0000\u0000\u0000\u04a1\u04a2\u0001\u0000\u0000\u0000"+
		"\u04a2\u04a3\u0003$\u0012\u0000\u04a3\u0013\u0001\u0000\u0000\u0000\u04a4"+
		"\u04a6\u0005\u0130\u0000\u0000\u04a5\u04a7\u0005\u00dd\u0000\u0000\u04a6"+
		"\u04a5\u0001\u0000\u0000\u0000\u04a6\u04a7\u0001\u0000\u0000\u0000\u04a7"+
		"\u04a8\u0001\u0000\u0000\u0000\u04a8\u04ad\u0003<\u001e\u0000\u04a9\u04aa"+
		"\u0005\u0004\u0000\u0000\u04aa\u04ac\u0003<\u001e\u0000\u04ab\u04a9\u0001"+
		"\u0000\u0000\u0000\u04ac\u04af\u0001\u0000\u0000\u0000\u04ad\u04ab\u0001"+
		"\u0000\u0000\u0000\u04ad\u04ae\u0001\u0000\u0000\u0000\u04ae\u0015\u0001"+
		"\u0000\u0000\u0000\u04af\u04ad\u0001\u0000\u0000\u0000\u04b0\u04b3\u0003"+
		"\u0018\f\u0000\u04b1\u04b3\u0003\u001a\r\u0000\u04b2\u04b0\u0001\u0000"+
		"\u0000\u0000\u04b2\u04b1\u0001\u0000\u0000\u0000\u04b3\u0017\u0001\u0000"+
		"\u0000\u0000\u04b4\u04b5\u0003\u00f2y\u0000\u04b5\u04b8\u0003\u00aeW\u0000"+
		"\u04b6\u04b7\u0005\u00b6\u0000\u0000\u04b7\u04b9\u0005\u00b7\u0000\u0000"+
		"\u04b8\u04b6\u0001\u0000\u0000\u0000\u04b8\u04b9\u0001\u0000\u0000\u0000"+
		"\u04b9\u04bc\u0001\u0000\u0000\u0000\u04ba\u04bb\u0005.\u0000\u0000\u04bb"+
		"\u04bd\u0003\u009eO\u0000\u04bc\u04ba\u0001\u0000\u0000\u0000\u04bc\u04bd"+
		"\u0001\u0000\u0000\u0000\u04bd\u04c0\u0001\u0000\u0000\u0000\u04be\u04bf"+
		"\u0005\u0130\u0000\u0000\u04bf\u04c1\u0003\u001c\u000e\u0000\u04c0\u04be"+
		"\u0001\u0000\u0000\u0000\u04c0\u04c1\u0001\u0000\u0000\u0000\u04c1\u0019"+
		"\u0001\u0000\u0000\u0000\u04c2\u04c3\u0005\u009a\u0000\u0000\u04c3\u04c6"+
		"\u0003\u00f2y\u0000\u04c4\u04c5\u0007\u0003\u0000\u0000\u04c5\u04c7\u0005"+
		"\u00d8\u0000\u0000\u04c6\u04c4\u0001\u0000\u0000\u0000\u04c6\u04c7\u0001"+
		"\u0000\u0000\u0000\u04c7\u001b\u0001\u0000\u0000\u0000\u04c8\u04c9\u0005"+
		"\u0002\u0000\u0000\u04c9\u04ca\u0003\u001e\u000f\u0000\u04ca\u04cb\u0005"+
		"\u0003\u0000\u0000\u04cb\u001d\u0001\u0000\u0000\u0000\u04cc\u04d1\u0003"+
		" \u0010\u0000\u04cd\u04ce\u0005\u0004\u0000\u0000\u04ce\u04d0\u0003 \u0010"+
		"\u0000\u04cf\u04cd\u0001\u0000\u0000\u0000\u04d0\u04d3\u0001\u0000\u0000"+
		"\u0000\u04d1\u04cf\u0001\u0000\u0000\u0000\u04d1\u04d2\u0001\u0000\u0000"+
		"\u0000\u04d2\u001f\u0001\u0000\u0000\u0000\u04d3\u04d1\u0001\u0000\u0000"+
		"\u0000\u04d4\u04d5\u0003\u0100\u0080\u0000\u04d5\u04d6\u0005\u0138\u0000"+
		"\u0000\u04d6\u04d7\u0003\"\u0011\u0000\u04d7!\u0001\u0000\u0000\u0000"+
		"\u04d8\u04db\u0005F\u0000\u0000\u04d9\u04db\u0003~?\u0000\u04da\u04d8"+
		"\u0001\u0000\u0000\u0000\u04da\u04d9\u0001\u0000\u0000\u0000\u04db#\u0001"+
		"\u0000\u0000\u0000\u04dc\u04e7\u0003*\u0015\u0000\u04dd\u04de\u0005\u00c3"+
		"\u0000\u0000\u04de\u04df\u0005$\u0000\u0000\u04df\u04e4\u0003.\u0017\u0000"+
		"\u04e0\u04e1\u0005\u0004\u0000\u0000\u04e1\u04e3\u0003.\u0017\u0000\u04e2"+
		"\u04e0\u0001\u0000\u0000\u0000\u04e3\u04e6\u0001\u0000\u0000\u0000\u04e4"+
		"\u04e2\u0001\u0000\u0000\u0000\u04e4\u04e5\u0001\u0000\u0000\u0000\u04e5"+
		"\u04e8\u0001\u0000\u0000\u0000\u04e6\u04e4\u0001\u0000\u0000\u0000\u04e7"+
		"\u04dd\u0001\u0000\u0000\u0000\u04e7\u04e8\u0001\u0000\u0000\u0000\u04e8"+
		"\u04ee\u0001\u0000\u0000\u0000\u04e9\u04ea\u0005\u00bc\u0000\u0000\u04ea"+
		"\u04ec\u0003(\u0014\u0000\u04eb\u04ed\u0007\u0004\u0000\u0000\u04ec\u04eb"+
		"\u0001\u0000\u0000\u0000\u04ec\u04ed\u0001\u0000\u0000\u0000\u04ed\u04ef"+
		"\u0001\u0000\u0000\u0000\u04ee\u04e9\u0001\u0000\u0000\u0000\u04ee\u04ef"+
		"\u0001\u0000\u0000\u0000\u04ef\u04fd\u0001\u0000\u0000\u0000\u04f0\u04f1"+
		"\u0005\u009b\u0000\u0000\u04f1\u04fe\u0003&\u0013\u0000\u04f2\u04f3\u0005"+
		"b\u0000\u0000\u04f3\u04f5\u0007\u0005\u0000\u0000\u04f4\u04f6\u0003(\u0014"+
		"\u0000\u04f5\u04f4\u0001\u0000\u0000\u0000\u04f5\u04f6\u0001\u0000\u0000"+
		"\u0000\u04f6\u04f7\u0001\u0000\u0000\u0000\u04f7\u04fb\u0007\u0004\u0000"+
		"\u0000\u04f8\u04fc\u0005\u00c0\u0000\u0000\u04f9\u04fa\u0005\u0130\u0000"+
		"\u0000\u04fa\u04fc\u0005\u010a\u0000\u0000\u04fb\u04f8\u0001\u0000\u0000"+
		"\u0000\u04fb\u04f9\u0001\u0000\u0000\u0000\u04fc\u04fe\u0001\u0000\u0000"+
		"\u0000\u04fd\u04f0\u0001\u0000\u0000\u0000\u04fd\u04f2\u0001\u0000\u0000"+
		"\u0000\u04fd\u04fe\u0001\u0000\u0000\u0000\u04fe%\u0001\u0000\u0000\u0000"+
		"\u04ff\u0502\u0005\u0016\u0000\u0000\u0500\u0502\u0003(\u0014\u0000\u0501"+
		"\u04ff\u0001\u0000\u0000\u0000\u0501\u0500\u0001\u0000\u0000\u0000\u0502"+
		"\'\u0001\u0000\u0000\u0000\u0503\u0504\u0007\u0006\u0000\u0000\u0504)"+
		"\u0001\u0000\u0000\u0000\u0505\u0506\u0006\u0015\uffff\uffff\u0000\u0506"+
		"\u0507\u0003,\u0016\u0000\u0507\u0516\u0001\u0000\u0000\u0000\u0508\u0509"+
		"\n\u0002\u0000\u0000\u0509\u050b\u0005\u0080\u0000\u0000\u050a\u050c\u0003"+
		">\u001f\u0000\u050b\u050a\u0001\u0000\u0000\u0000\u050b\u050c\u0001\u0000"+
		"\u0000\u0000\u050c\u050d\u0001\u0000\u0000\u0000\u050d\u0515\u0003*\u0015"+
		"\u0003\u050e\u050f\n\u0001\u0000\u0000\u050f\u0511\u0007\u0007\u0000\u0000"+
		"\u0510\u0512\u0003>\u001f\u0000\u0511\u0510\u0001\u0000\u0000\u0000\u0511"+
		"\u0512\u0001\u0000\u0000\u0000\u0512\u0513\u0001\u0000\u0000\u0000\u0513"+
		"\u0515\u0003*\u0015\u0002\u0514\u0508\u0001\u0000\u0000\u0000\u0514\u050e"+
		"\u0001\u0000\u0000\u0000\u0515\u0518\u0001\u0000\u0000\u0000\u0516\u0514"+
		"\u0001\u0000\u0000\u0000\u0516\u0517\u0001\u0000\u0000\u0000\u0517+\u0001"+
		"\u0000\u0000\u0000\u0518\u0516\u0001\u0000\u0000\u0000\u0519\u052a\u0003"+
		"0\u0018\u0000\u051a\u051b\u0005\u0104\u0000\u0000\u051b\u052a\u0003\u00f2"+
		"y\u0000\u051c\u051d\u0005\u0128\u0000\u0000\u051d\u0522\u0003~?\u0000"+
		"\u051e\u051f\u0005\u0004\u0000\u0000\u051f\u0521\u0003~?\u0000\u0520\u051e"+
		"\u0001\u0000\u0000\u0000\u0521\u0524\u0001\u0000\u0000\u0000\u0522\u0520"+
		"\u0001\u0000\u0000\u0000\u0522\u0523\u0001\u0000\u0000\u0000\u0523\u052a"+
		"\u0001\u0000\u0000\u0000\u0524\u0522\u0001\u0000\u0000\u0000\u0525\u0526"+
		"\u0005\u0002\u0000\u0000\u0526\u0527\u0003$\u0012\u0000\u0527\u0528\u0005"+
		"\u0003\u0000\u0000\u0528\u052a\u0001\u0000\u0000\u0000\u0529\u0519\u0001"+
		"\u0000\u0000\u0000\u0529\u051a\u0001\u0000\u0000\u0000\u0529\u051c\u0001"+
		"\u0000\u0000\u0000\u0529\u0525\u0001\u0000\u0000\u0000\u052a-\u0001\u0000"+
		"\u0000\u0000\u052b\u052d\u0003~?\u0000\u052c\u052e\u0007\b\u0000\u0000"+
		"\u052d\u052c\u0001\u0000\u0000\u0000\u052d\u052e\u0001\u0000\u0000\u0000"+
		"\u052e\u0531\u0001\u0000\u0000\u0000\u052f\u0530\u0005\u00b9\u0000\u0000"+
		"\u0530\u0532\u0007\t\u0000\u0000\u0531\u052f\u0001\u0000\u0000\u0000\u0531"+
		"\u0532\u0001\u0000\u0000\u0000\u0532/\u0001\u0000\u0000\u0000\u0533\u0535"+
		"\u0005\u00f8\u0000\u0000\u0534\u0536\u0003>\u001f\u0000\u0535\u0534\u0001"+
		"\u0000\u0000\u0000\u0535\u0536\u0001\u0000\u0000\u0000\u0536\u0537\u0001"+
		"\u0000\u0000\u0000\u0537\u053c\u0003@ \u0000\u0538\u0539\u0005\u0004\u0000"+
		"\u0000\u0539\u053b\u0003@ \u0000\u053a\u0538\u0001\u0000\u0000\u0000\u053b"+
		"\u053e\u0001\u0000\u0000\u0000\u053c\u053a\u0001\u0000\u0000\u0000\u053c"+
		"\u053d\u0001\u0000\u0000\u0000\u053d\u0548\u0001\u0000\u0000\u0000\u053e"+
		"\u053c\u0001\u0000\u0000\u0000\u053f\u0540\u0005i\u0000\u0000\u0540\u0545"+
		"\u0003B!\u0000\u0541\u0542\u0005\u0004\u0000\u0000\u0542\u0544\u0003B"+
		"!\u0000\u0543\u0541\u0001\u0000\u0000\u0000\u0544\u0547\u0001\u0000\u0000"+
		"\u0000\u0545\u0543\u0001\u0000\u0000\u0000\u0545\u0546\u0001\u0000\u0000"+
		"\u0000\u0546\u0549\u0001\u0000\u0000\u0000\u0547\u0545\u0001\u0000\u0000"+
		"\u0000\u0548\u053f\u0001\u0000\u0000\u0000\u0548\u0549\u0001\u0000\u0000"+
		"\u0000\u0549\u054c\u0001\u0000\u0000\u0000\u054a\u054b\u0005\u012d\u0000"+
		"\u0000\u054b\u054d\u0003\u0080@\u0000\u054c\u054a\u0001\u0000\u0000\u0000"+
		"\u054c\u054d\u0001\u0000\u0000\u0000\u054d\u0551\u0001\u0000\u0000\u0000"+
		"\u054e\u054f\u0005r\u0000\u0000\u054f\u0550\u0005$\u0000\u0000\u0550\u0552"+
		"\u00032\u0019\u0000\u0551\u054e\u0001\u0000\u0000\u0000\u0551\u0552\u0001"+
		"\u0000\u0000\u0000\u0552\u0555\u0001\u0000\u0000\u0000\u0553\u0554\u0005"+
		"u\u0000\u0000\u0554\u0556\u0003\u0080@\u0000\u0555\u0553\u0001\u0000\u0000"+
		"\u0000\u0555\u0556\u0001\u0000\u0000\u0000\u0556\u0560\u0001\u0000\u0000"+
		"\u0000\u0557\u0558\u0005\u012f\u0000\u0000\u0558\u055d\u00038\u001c\u0000"+
		"\u0559\u055a\u0005\u0004\u0000\u0000\u055a\u055c\u00038\u001c\u0000\u055b"+
		"\u0559\u0001\u0000\u0000\u0000\u055c\u055f\u0001\u0000\u0000\u0000\u055d"+
		"\u055b\u0001\u0000\u0000\u0000\u055d\u055e\u0001\u0000\u0000\u0000\u055e"+
		"\u0561\u0001\u0000\u0000\u0000\u055f\u055d\u0001\u0000\u0000\u0000\u0560"+
		"\u0557\u0001\u0000\u0000\u0000\u0560\u0561\u0001\u0000\u0000\u0000\u0561"+
		"1\u0001\u0000\u0000\u0000\u0562\u0564\u0003>\u001f\u0000\u0563\u0562\u0001"+
		"\u0000\u0000\u0000\u0563\u0564\u0001\u0000\u0000\u0000\u0564\u0565\u0001"+
		"\u0000\u0000\u0000\u0565\u056a\u00034\u001a\u0000\u0566\u0567\u0005\u0004"+
		"\u0000\u0000\u0567\u0569\u00034\u001a\u0000\u0568\u0566\u0001\u0000\u0000"+
		"\u0000\u0569\u056c\u0001\u0000\u0000\u0000\u056a\u0568\u0001\u0000\u0000"+
		"\u0000\u056a\u056b\u0001\u0000\u0000\u0000\u056b3\u0001\u0000\u0000\u0000"+
		"\u056c\u056a\u0001\u0000\u0000\u0000\u056d\u0596\u00036\u001b\u0000\u056e"+
		"\u056f\u0005\u00ee\u0000\u0000\u056f\u0578\u0005\u0002\u0000\u0000\u0570"+
		"\u0575\u00036\u001b\u0000\u0571\u0572\u0005\u0004\u0000\u0000\u0572\u0574"+
		"\u00036\u001b\u0000\u0573\u0571\u0001\u0000\u0000\u0000\u0574\u0577\u0001"+
		"\u0000\u0000\u0000\u0575\u0573\u0001\u0000\u0000\u0000\u0575\u0576\u0001"+
		"\u0000\u0000\u0000\u0576\u0579\u0001\u0000\u0000\u0000\u0577\u0575\u0001"+
		"\u0000\u0000\u0000\u0578\u0570\u0001\u0000\u0000\u0000\u0578\u0579\u0001"+
		"\u0000\u0000\u0000\u0579\u057a\u0001\u0000\u0000\u0000\u057a\u0596\u0005"+
		"\u0003\u0000\u0000\u057b\u057c\u00057\u0000\u0000\u057c\u0585\u0005\u0002"+
		"\u0000\u0000\u057d\u0582\u00036\u001b\u0000\u057e\u057f\u0005\u0004\u0000"+
		"\u0000\u057f\u0581\u00036\u001b\u0000\u0580\u057e\u0001\u0000\u0000\u0000"+
		"\u0581\u0584\u0001\u0000\u0000\u0000\u0582\u0580\u0001\u0000\u0000\u0000"+
		"\u0582\u0583\u0001\u0000\u0000\u0000\u0583\u0586\u0001\u0000\u0000\u0000"+
		"\u0584\u0582\u0001\u0000\u0000\u0000\u0585\u057d\u0001\u0000\u0000\u0000"+
		"\u0585\u0586\u0001\u0000\u0000\u0000\u0586\u0587\u0001\u0000\u0000\u0000"+
		"\u0587\u0596\u0005\u0003\u0000\u0000\u0588\u0589\u0005s\u0000\u0000\u0589"+
		"\u058a\u0005\u00fc\u0000\u0000\u058a\u058b\u0005\u0002\u0000\u0000\u058b"+
		"\u0590\u00036\u001b\u0000\u058c\u058d\u0005\u0004\u0000\u0000\u058d\u058f"+
		"\u00036\u001b\u0000\u058e\u058c\u0001\u0000\u0000\u0000\u058f\u0592\u0001"+
		"\u0000\u0000\u0000\u0590\u058e\u0001\u0000\u0000\u0000\u0590\u0591\u0001"+
		"\u0000\u0000\u0000\u0591\u0593\u0001\u0000\u0000\u0000\u0592\u0590\u0001"+
		"\u0000\u0000\u0000\u0593\u0594\u0005\u0003\u0000\u0000\u0594\u0596\u0001"+
		"\u0000\u0000\u0000\u0595\u056d\u0001\u0000\u0000\u0000\u0595\u056e\u0001"+
		"\u0000\u0000\u0000\u0595\u057b\u0001\u0000\u0000\u0000\u0595\u0588\u0001"+
		"\u0000\u0000\u0000\u05965\u0001\u0000\u0000\u0000\u0597\u05a0\u0005\u0002"+
		"\u0000\u0000\u0598\u059d\u0003~?\u0000\u0599\u059a\u0005\u0004\u0000\u0000"+
		"\u059a\u059c\u0003~?\u0000\u059b\u0599\u0001\u0000\u0000\u0000\u059c\u059f"+
		"\u0001\u0000\u0000\u0000\u059d\u059b\u0001\u0000\u0000\u0000\u059d\u059e"+
		"\u0001\u0000\u0000\u0000\u059e\u05a1\u0001\u0000\u0000\u0000\u059f\u059d"+
		"\u0001\u0000\u0000\u0000\u05a0\u0598\u0001\u0000\u0000\u0000\u05a0\u05a1"+
		"\u0001\u0000\u0000\u0000\u05a1\u05a2\u0001\u0000\u0000\u0000\u05a2\u05a5"+
		"\u0005\u0003\u0000\u0000\u05a3\u05a5\u0003~?\u0000\u05a4\u0597\u0001\u0000"+
		"\u0000\u0000\u05a4\u05a3\u0001\u0000\u0000\u0000\u05a57\u0001\u0000\u0000"+
		"\u0000\u05a6\u05a7\u0003\u0100\u0080\u0000\u05a7\u05a8\u0005\u001c\u0000"+
		"\u0000\u05a8\u05a9\u0005\u0002\u0000\u0000\u05a9\u05aa\u0003:\u001d\u0000"+
		"\u05aa\u05ab\u0005\u0003\u0000\u0000\u05ab9\u0001\u0000\u0000\u0000\u05ac"+
		"\u05ae\u0003\u0100\u0080\u0000\u05ad\u05ac\u0001\u0000\u0000\u0000\u05ad"+
		"\u05ae\u0001\u0000\u0000\u0000\u05ae\u05b9\u0001\u0000\u0000\u0000\u05af"+
		"\u05b0\u0005\u00c9\u0000\u0000\u05b0\u05b1\u0005$\u0000\u0000\u05b1\u05b6"+
		"\u0003~?\u0000\u05b2\u05b3\u0005\u0004\u0000\u0000\u05b3\u05b5\u0003~"+
		"?\u0000\u05b4\u05b2\u0001\u0000\u0000\u0000\u05b5\u05b8\u0001\u0000\u0000"+
		"\u0000\u05b6\u05b4\u0001\u0000\u0000\u0000\u05b6\u05b7\u0001\u0000\u0000"+
		"\u0000\u05b7\u05ba\u0001\u0000\u0000\u0000\u05b8\u05b6\u0001\u0000\u0000"+
		"\u0000\u05b9\u05af\u0001\u0000\u0000\u0000\u05b9\u05ba\u0001\u0000\u0000"+
		"\u0000\u05ba\u05c5\u0001\u0000\u0000\u0000\u05bb\u05bc\u0005\u00c3\u0000"+
		"\u0000\u05bc\u05bd\u0005$\u0000\u0000\u05bd\u05c2\u0003.\u0017\u0000\u05be"+
		"\u05bf\u0005\u0004\u0000\u0000\u05bf\u05c1\u0003.\u0017\u0000\u05c0\u05be"+
		"\u0001\u0000\u0000\u0000\u05c1\u05c4\u0001\u0000\u0000\u0000\u05c2\u05c0"+
		"\u0001\u0000\u0000\u0000\u05c2\u05c3\u0001\u0000\u0000\u0000\u05c3\u05c6"+
		"\u0001\u0000\u0000\u0000\u05c4\u05c2\u0001\u0000\u0000\u0000\u05c5\u05bb"+
		"\u0001\u0000\u0000\u0000\u05c5\u05c6\u0001\u0000\u0000\u0000\u05c6\u05c8"+
		"\u0001\u0000\u0000\u0000\u05c7\u05c9\u0003\u00bc^\u0000\u05c8\u05c7\u0001"+
		"\u0000\u0000\u0000\u05c8\u05c9\u0001\u0000\u0000\u0000\u05c9;\u0001\u0000"+
		"\u0000\u0000\u05ca\u05cc\u0003\u0100\u0080\u0000\u05cb\u05cd\u0003b1\u0000"+
		"\u05cc\u05cb\u0001\u0000\u0000\u0000\u05cc\u05cd\u0001\u0000\u0000\u0000"+
		"\u05cd\u05ce\u0001\u0000\u0000\u0000\u05ce\u05cf\u0005\u001c\u0000\u0000"+
		"\u05cf\u05d0\u0005\u0002\u0000\u0000\u05d0\u05d1\u0003\u0012\t\u0000\u05d1"+
		"\u05d2\u0005\u0003\u0000\u0000\u05d2=\u0001\u0000\u0000\u0000\u05d3\u05d4"+
		"\u0007\n\u0000\u0000\u05d4?\u0001\u0000\u0000\u0000\u05d5\u05da\u0003"+
		"~?\u0000\u05d6\u05d8\u0005\u001c\u0000\u0000\u05d7\u05d6\u0001\u0000\u0000"+
		"\u0000\u05d7\u05d8\u0001\u0000\u0000\u0000\u05d8\u05d9\u0001\u0000\u0000"+
		"\u0000\u05d9\u05db\u0003\u0100\u0080\u0000\u05da\u05d7\u0001\u0000\u0000"+
		"\u0000\u05da\u05db\u0001\u0000\u0000\u0000\u05db\u05e5\u0001\u0000\u0000"+
		"\u0000\u05dc\u05dd\u0003\u0086C\u0000\u05dd\u05de\u0005\u0001\u0000\u0000"+
		"\u05de\u05e1\u0005\u0140\u0000\u0000\u05df\u05e0\u0005\u001c\u0000\u0000"+
		"\u05e0\u05e2\u0003b1\u0000\u05e1\u05df\u0001\u0000\u0000\u0000\u05e1\u05e2"+
		"\u0001\u0000\u0000\u0000\u05e2\u05e5\u0001\u0000\u0000\u0000\u05e3\u05e5"+
		"\u0005\u0140\u0000\u0000\u05e4\u05d5\u0001\u0000\u0000\u0000\u05e4\u05dc"+
		"\u0001\u0000\u0000\u0000\u05e4\u05e3\u0001\u0000\u0000\u0000\u05e5A\u0001"+
		"\u0000\u0000\u0000\u05e6\u05e7\u0006!\uffff\uffff\u0000\u05e7\u05e8\u0003"+
		"H$\u0000\u05e8\u05fb\u0001\u0000\u0000\u0000\u05e9\u05f7\n\u0002\u0000"+
		"\u0000\u05ea\u05eb\u00056\u0000\u0000\u05eb\u05ec\u0005\u0088\u0000\u0000"+
		"\u05ec\u05f8\u0003H$\u0000\u05ed\u05ee\u0003D\"\u0000\u05ee\u05ef\u0005"+
		"\u0088\u0000\u0000\u05ef\u05f0\u0003B!\u0000\u05f0\u05f1\u0003F#\u0000"+
		"\u05f1\u05f8\u0001\u0000\u0000\u0000\u05f2\u05f3\u0005\u00ac\u0000\u0000"+
		"\u05f3\u05f4\u0003D\"\u0000\u05f4\u05f5\u0005\u0088\u0000\u0000\u05f5"+
		"\u05f6\u0003H$\u0000\u05f6\u05f8\u0001\u0000\u0000\u0000\u05f7\u05ea\u0001"+
		"\u0000\u0000\u0000\u05f7\u05ed\u0001\u0000\u0000\u0000\u05f7\u05f2\u0001"+
		"\u0000\u0000\u0000\u05f8\u05fa\u0001\u0000\u0000\u0000\u05f9\u05e9\u0001"+
		"\u0000\u0000\u0000\u05fa\u05fd\u0001\u0000\u0000\u0000\u05fb\u05f9\u0001"+
		"\u0000\u0000\u0000\u05fb\u05fc\u0001\u0000\u0000\u0000\u05fcC\u0001\u0000"+
		"\u0000\u0000\u05fd\u05fb\u0001\u0000\u0000\u0000\u05fe\u0600\u0005}\u0000"+
		"\u0000\u05ff\u05fe\u0001\u0000\u0000\u0000\u05ff\u0600\u0001\u0000\u0000"+
		"\u0000\u0600\u060e\u0001\u0000\u0000\u0000\u0601\u0603\u0005\u0098\u0000"+
		"\u0000\u0602\u0604\u0005\u00c5\u0000\u0000\u0603\u0602\u0001\u0000\u0000"+
		"\u0000\u0603\u0604\u0001\u0000\u0000\u0000\u0604\u060e\u0001\u0000\u0000"+
		"\u0000\u0605\u0607\u0005\u00ea\u0000\u0000\u0606\u0608\u0005\u00c5\u0000"+
		"\u0000\u0607\u0606\u0001\u0000\u0000\u0000\u0607\u0608\u0001\u0000\u0000"+
		"\u0000\u0608\u060e\u0001\u0000\u0000\u0000\u0609\u060b\u0005j\u0000\u0000"+
		"\u060a\u060c\u0005\u00c5\u0000\u0000\u060b\u060a\u0001\u0000\u0000\u0000"+
		"\u060b\u060c\u0001\u0000\u0000\u0000\u060c\u060e\u0001\u0000\u0000\u0000"+
		"\u060d\u05ff\u0001\u0000\u0000\u0000\u060d\u0601\u0001\u0000\u0000\u0000"+
		"\u060d\u0605\u0001\u0000\u0000\u0000\u060d\u0609\u0001\u0000\u0000\u0000"+
		"\u060eE\u0001\u0000\u0000\u0000\u060f\u0610\u0005\u00be\u0000\u0000\u0610"+
		"\u061e\u0003\u0080@\u0000\u0611\u0612\u0005\u0122\u0000\u0000\u0612\u0613"+
		"\u0005\u0002\u0000\u0000\u0613\u0618\u0003\u0100\u0080\u0000\u0614\u0615"+
		"\u0005\u0004\u0000\u0000\u0615\u0617\u0003\u0100\u0080\u0000\u0616\u0614"+
		"\u0001\u0000\u0000\u0000\u0617\u061a\u0001\u0000\u0000\u0000\u0618\u0616"+
		"\u0001\u0000\u0000\u0000\u0618\u0619\u0001\u0000\u0000\u0000\u0619\u061b"+
		"\u0001\u0000\u0000\u0000\u061a\u0618\u0001\u0000\u0000\u0000\u061b\u061c"+
		"\u0005\u0003\u0000\u0000\u061c\u061e\u0001\u0000\u0000\u0000\u061d\u060f"+
		"\u0001\u0000\u0000\u0000\u061d\u0611\u0001\u0000\u0000\u0000\u061eG\u0001"+
		"\u0000\u0000\u0000\u061f\u0626\u0003R)\u0000\u0620\u0621\u0005\u0106\u0000"+
		"\u0000\u0621\u0622\u0003J%\u0000\u0622\u0623\u0005\u0002\u0000\u0000\u0623"+
		"\u0624\u0003~?\u0000\u0624\u0625\u0005\u0003\u0000\u0000\u0625\u0627\u0001"+
		"\u0000\u0000\u0000\u0626\u0620\u0001\u0000\u0000\u0000\u0626\u0627\u0001"+
		"\u0000\u0000\u0000\u0627I\u0001\u0000\u0000\u0000\u0628\u0629\u0007\u000b"+
		"\u0000\u0000\u0629K\u0001\u0000\u0000\u0000\u062a\u062b\u0007\f\u0000"+
		"\u0000\u062bM\u0001\u0000\u0000\u0000\u062c\u0633\u0005Y\u0000\u0000\u062d"+
		"\u062f\u0005\u0112\u0000\u0000\u062e\u0630\u0003\u009eO\u0000\u062f\u062e"+
		"\u0001\u0000\u0000\u0000\u062f\u0630\u0001\u0000\u0000\u0000\u0630\u0631"+
		"\u0001\u0000\u0000\u0000\u0631\u0633\u0003P(\u0000\u0632\u062c\u0001\u0000"+
		"\u0000\u0000\u0632\u062d\u0001\u0000\u0000\u0000\u0633O\u0001\u0000\u0000"+
		"\u0000\u0634\u0635\u0005\u0130\u0000\u0000\u0635\u0639\u00053\u0000\u0000"+
		"\u0636\u0637\u0005\u0132\u0000\u0000\u0637\u0639\u00053\u0000\u0000\u0638"+
		"\u0634\u0001\u0000\u0000\u0000\u0638\u0636\u0001\u0000\u0000\u0000\u0639"+
		"Q\u0001\u0000\u0000\u0000\u063a\u068d\u0003`0\u0000\u063b\u063c\u0005"+
		"\u00a6\u0000\u0000\u063c\u0647\u0005\u0002\u0000\u0000\u063d\u063e\u0005"+
		"\u00c9\u0000\u0000\u063e\u063f\u0005$\u0000\u0000\u063f\u0644\u0003~?"+
		"\u0000\u0640\u0641\u0005\u0004\u0000\u0000\u0641\u0643\u0003~?\u0000\u0642"+
		"\u0640\u0001\u0000\u0000\u0000\u0643\u0646\u0001\u0000\u0000\u0000\u0644"+
		"\u0642\u0001\u0000\u0000\u0000\u0644\u0645\u0001\u0000\u0000\u0000\u0645"+
		"\u0648\u0001\u0000\u0000\u0000\u0646\u0644\u0001\u0000\u0000\u0000\u0647"+
		"\u063d\u0001\u0000\u0000\u0000\u0647\u0648\u0001\u0000\u0000\u0000\u0648"+
		"\u0653\u0001\u0000\u0000\u0000\u0649\u064a\u0005\u00c3\u0000\u0000\u064a"+
		"\u064b\u0005$\u0000\u0000\u064b\u0650\u0003.\u0017\u0000\u064c\u064d\u0005"+
		"\u0004\u0000\u0000\u064d\u064f\u0003.\u0017\u0000\u064e\u064c\u0001\u0000"+
		"\u0000\u0000\u064f\u0652\u0001\u0000\u0000\u0000\u0650\u064e\u0001\u0000"+
		"\u0000\u0000\u0650\u0651\u0001\u0000\u0000\u0000\u0651\u0654\u0001\u0000"+
		"\u0000\u0000\u0652\u0650\u0001\u0000\u0000\u0000\u0653\u0649\u0001\u0000"+
		"\u0000\u0000\u0653\u0654\u0001\u0000\u0000\u0000\u0654\u065e\u0001\u0000"+
		"\u0000\u0000\u0655\u0656\u0005\u00a8\u0000\u0000\u0656\u065b\u0003T*\u0000"+
		"\u0657\u0658\u0005\u0004\u0000\u0000\u0658\u065a\u0003T*\u0000\u0659\u0657"+
		"\u0001\u0000\u0000\u0000\u065a\u065d\u0001\u0000\u0000\u0000\u065b\u0659"+
		"\u0001\u0000\u0000\u0000\u065b\u065c\u0001\u0000\u0000\u0000\u065c\u065f"+
		"\u0001\u0000\u0000\u0000\u065d\u065b\u0001\u0000\u0000\u0000\u065e\u0655"+
		"\u0001\u0000\u0000\u0000\u065e\u065f\u0001\u0000\u0000\u0000\u065f\u0661"+
		"\u0001\u0000\u0000\u0000\u0660\u0662\u0003V+\u0000\u0661\u0660\u0001\u0000"+
		"\u0000\u0000\u0661\u0662\u0001\u0000\u0000\u0000\u0662\u0666\u0001\u0000"+
		"\u0000\u0000\u0663\u0664\u0005\u0015\u0000\u0000\u0664\u0665\u0005\u00a3"+
		"\u0000\u0000\u0665\u0667\u0003Z-\u0000\u0666\u0663\u0001\u0000\u0000\u0000"+
		"\u0666\u0667\u0001\u0000\u0000\u0000\u0667\u0669\u0001\u0000\u0000\u0000"+
		"\u0668\u066a\u0007\r\u0000\u0000\u0669\u0668\u0001\u0000\u0000\u0000\u0669"+
		"\u066a\u0001\u0000\u0000\u0000\u066a\u066b\u0001\u0000\u0000\u0000\u066b"+
		"\u066c\u0005\u00ce\u0000\u0000\u066c\u066d\u0005\u0002\u0000\u0000\u066d"+
		"\u066e\u0003\u00c2a\u0000\u066e\u0678\u0005\u0003\u0000\u0000\u066f\u0670"+
		"\u0005\u0101\u0000\u0000\u0670\u0675\u0003\\.\u0000\u0671\u0672\u0005"+
		"\u0004\u0000\u0000\u0672\u0674\u0003\\.\u0000\u0673\u0671\u0001\u0000"+
		"\u0000\u0000\u0674\u0677\u0001\u0000\u0000\u0000\u0675\u0673\u0001\u0000"+
		"\u0000\u0000\u0675\u0676\u0001\u0000\u0000\u0000\u0676\u0679\u0001\u0000"+
		"\u0000\u0000\u0677\u0675\u0001\u0000\u0000\u0000\u0678\u066f\u0001\u0000"+
		"\u0000\u0000\u0678\u0679\u0001\u0000\u0000\u0000\u0679\u067a\u0001\u0000"+
		"\u0000\u0000\u067a\u067b\u0005G\u0000\u0000\u067b\u0680\u0003^/\u0000"+
		"\u067c\u067d\u0005\u0004\u0000\u0000\u067d\u067f\u0003^/\u0000\u067e\u067c"+
		"\u0001\u0000\u0000\u0000\u067f\u0682\u0001\u0000\u0000\u0000\u0680\u067e"+
		"\u0001\u0000\u0000\u0000\u0680\u0681\u0001\u0000\u0000\u0000\u0681\u0683"+
		"\u0001\u0000\u0000\u0000\u0682\u0680\u0001\u0000\u0000\u0000\u0683\u068b"+
		"\u0005\u0003\u0000\u0000\u0684\u0686\u0005\u001c\u0000\u0000\u0685\u0684"+
		"\u0001\u0000\u0000\u0000\u0685\u0686\u0001\u0000\u0000\u0000\u0686\u0687"+
		"\u0001\u0000\u0000\u0000\u0687\u0689\u0003\u0100\u0080\u0000\u0688\u068a"+
		"\u0003b1\u0000\u0689\u0688\u0001\u0000\u0000\u0000\u0689\u068a\u0001\u0000"+
		"\u0000\u0000\u068a\u068c\u0001\u0000\u0000\u0000\u068b\u0685\u0001\u0000"+
		"\u0000\u0000\u068b\u068c\u0001\u0000\u0000\u0000\u068c\u068e\u0001\u0000"+
		"\u0000\u0000\u068d\u063b\u0001\u0000\u0000\u0000\u068d\u068e\u0001\u0000"+
		"\u0000\u0000\u068eS\u0001\u0000\u0000\u0000\u068f\u0690\u0003~?\u0000"+
		"\u0690\u0691\u0005\u001c\u0000\u0000\u0691\u0692\u0003\u0100\u0080\u0000"+
		"\u0692U\u0001\u0000\u0000\u0000\u0693\u0694\u0005\u00bf\u0000\u0000\u0694"+
		"\u0695\u0005\u00ef\u0000\u0000\u0695\u0696\u0005\u00cf\u0000\u0000\u0696"+
		"\u069f\u0005\u00a3\u0000\u0000\u0697\u0698\u0005\u0016\u0000\u0000\u0698"+
		"\u0699\u0005\u00f0\u0000\u0000\u0699\u069a\u0005\u00cf\u0000\u0000\u069a"+
		"\u069c\u0005\u00a3\u0000\u0000\u069b\u069d\u0003X,\u0000\u069c\u069b\u0001"+
		"\u0000\u0000\u0000\u069c\u069d\u0001\u0000\u0000\u0000\u069d\u069f\u0001"+
		"\u0000\u0000\u0000\u069e\u0693\u0001\u0000\u0000\u0000\u069e\u0697\u0001"+
		"\u0000\u0000\u0000\u069fW\u0001\u0000\u0000\u0000\u06a0\u06a1\u0005\u00fd"+
		"\u0000\u0000\u06a1\u06a2\u0005U\u0000\u0000\u06a2\u06aa\u0005\u00a5\u0000"+
		"\u0000\u06a3\u06a4\u0005\u00bd\u0000\u0000\u06a4\u06a5\u0005U\u0000\u0000"+
		"\u06a5\u06aa\u0005\u00a5\u0000\u0000\u06a6\u06a7\u0005\u0130\u0000\u0000"+
		"\u06a7\u06a8\u0005\u011c\u0000\u0000\u06a8\u06aa\u0005\u00f0\u0000\u0000"+
		"\u06a9\u06a0\u0001\u0000\u0000\u0000\u06a9\u06a3\u0001\u0000\u0000\u0000"+
		"\u06a9\u06a6\u0001\u0000\u0000\u0000\u06aaY\u0001\u0000\u0000\u0000\u06ab"+
		"\u06ac\u0005\u0005\u0000\u0000\u06ac\u06ad\u0005\u010d\u0000\u0000\u06ad"+
		"\u06ae\u0005\u00ae\u0000\u0000\u06ae\u06bf\u0005\u00ef\u0000\u0000\u06af"+
		"\u06b0\u0005\u0005\u0000\u0000\u06b0\u06b1\u0005\u00cc\u0000\u0000\u06b1"+
		"\u06b2\u0005\u0094\u0000\u0000\u06b2\u06bf\u0005\u00ef\u0000\u0000\u06b3"+
		"\u06b4\u0005\u0005\u0000\u0000\u06b4\u06b5\u0005\u010d\u0000\u0000\u06b5"+
		"\u06b6\u0005e\u0000\u0000\u06b6\u06bf\u0003\u0100\u0080\u0000\u06b7\u06b8"+
		"\u0005\u0005\u0000\u0000\u06b8\u06b9\u0005\u010d\u0000\u0000\u06b9\u06ba"+
		"\u0005\u0094\u0000\u0000\u06ba\u06bf\u0003\u0100\u0080\u0000\u06bb\u06bc"+
		"\u0005\u0005\u0000\u0000\u06bc\u06bd\u0005\u010d\u0000\u0000\u06bd\u06bf"+
		"\u0003\u0100\u0080\u0000\u06be\u06ab\u0001\u0000\u0000\u0000\u06be\u06af"+
		"\u0001\u0000\u0000\u0000\u06be\u06b3\u0001\u0000\u0000\u0000\u06be\u06b7"+
		"\u0001\u0000\u0000\u0000\u06be\u06bb\u0001\u0000\u0000\u0000\u06bf[\u0001"+
		"\u0000\u0000\u0000\u06c0\u06c1\u0003\u0100\u0080\u0000\u06c1\u06c2\u0005"+
		"\u0138\u0000\u0000\u06c2\u06c3\u0005\u0002\u0000\u0000\u06c3\u06c8\u0003"+
		"\u0100\u0080\u0000\u06c4\u06c5\u0005\u0004\u0000\u0000\u06c5\u06c7\u0003"+
		"\u0100\u0080\u0000\u06c6\u06c4\u0001\u0000\u0000\u0000\u06c7\u06ca\u0001"+
		"\u0000\u0000\u0000\u06c8\u06c6\u0001\u0000\u0000\u0000\u06c8\u06c9\u0001"+
		"\u0000\u0000\u0000\u06c9\u06cb\u0001\u0000\u0000\u0000\u06ca\u06c8\u0001"+
		"\u0000\u0000\u0000\u06cb\u06cc\u0005\u0003\u0000\u0000\u06cc]\u0001\u0000"+
		"\u0000\u0000\u06cd\u06ce\u0003\u0100\u0080\u0000\u06ce\u06cf\u0005\u001c"+
		"\u0000\u0000\u06cf\u06d0\u0003~?\u0000\u06d0_\u0001\u0000\u0000\u0000"+
		"\u06d1\u06d9\u0003d2\u0000\u06d2\u06d4\u0005\u001c\u0000\u0000\u06d3\u06d2"+
		"\u0001\u0000\u0000\u0000\u06d3\u06d4\u0001\u0000\u0000\u0000\u06d4\u06d5"+
		"\u0001\u0000\u0000\u0000\u06d5\u06d7\u0003\u0100\u0080\u0000\u06d6\u06d8"+
		"\u0003b1\u0000\u06d7\u06d6\u0001\u0000\u0000\u0000\u06d7\u06d8\u0001\u0000"+
		"\u0000\u0000\u06d8\u06da\u0001\u0000\u0000\u0000\u06d9\u06d3\u0001\u0000"+
		"\u0000\u0000\u06d9\u06da\u0001\u0000\u0000\u0000\u06daa\u0001\u0000\u0000"+
		"\u0000\u06db\u06dc\u0005\u0002\u0000\u0000\u06dc\u06e1\u0003\u0100\u0080"+
		"\u0000\u06dd\u06de\u0005\u0004\u0000\u0000\u06de\u06e0\u0003\u0100\u0080"+
		"\u0000\u06df\u06dd\u0001\u0000\u0000\u0000\u06e0\u06e3\u0001\u0000\u0000"+
		"\u0000\u06e1\u06df\u0001\u0000\u0000\u0000\u06e1\u06e2\u0001\u0000\u0000"+
		"\u0000\u06e2\u06e4\u0001\u0000\u0000\u0000\u06e3\u06e1\u0001\u0000\u0000"+
		"\u0000\u06e4\u06e5\u0005\u0003\u0000\u0000\u06e5c\u0001\u0000\u0000\u0000"+
		"\u06e6\u06e8\u0003\u00f2y\u0000\u06e7\u06e9\u0003\u00f4z\u0000\u06e8\u06e7"+
		"\u0001\u0000\u0000\u0000\u06e8\u06e9\u0001\u0000\u0000\u0000\u06e9\u072e"+
		"\u0001\u0000\u0000\u0000\u06ea\u06eb\u0005\u0002\u0000\u0000\u06eb\u06ec"+
		"\u0003\u0012\t\u0000\u06ec\u06ed\u0005\u0003\u0000\u0000\u06ed\u072e\u0001"+
		"\u0000\u0000\u0000\u06ee\u06ef\u0005\u011d\u0000\u0000\u06ef\u06f0\u0005"+
		"\u0002\u0000\u0000\u06f0\u06f5\u0003~?\u0000\u06f1\u06f2\u0005\u0004\u0000"+
		"\u0000\u06f2\u06f4\u0003~?\u0000\u06f3\u06f1\u0001\u0000\u0000\u0000\u06f4"+
		"\u06f7\u0001\u0000\u0000\u0000\u06f5\u06f3\u0001\u0000\u0000\u0000\u06f5"+
		"\u06f6\u0001\u0000\u0000\u0000\u06f6\u06f8\u0001\u0000\u0000\u0000\u06f7"+
		"\u06f5\u0001\u0000\u0000\u0000\u06f8\u06fb\u0005\u0003\u0000\u0000\u06f9"+
		"\u06fa\u0005\u0130\u0000\u0000\u06fa\u06fc\u0005\u00c4\u0000\u0000\u06fb"+
		"\u06f9\u0001\u0000\u0000\u0000\u06fb\u06fc\u0001\u0000\u0000\u0000\u06fc"+
		"\u072e\u0001\u0000\u0000\u0000\u06fd\u06fe\u0005\u0095\u0000\u0000\u06fe"+
		"\u06ff\u0005\u0002\u0000\u0000\u06ff\u0700\u0003\u0012\t\u0000\u0700\u0701"+
		"\u0005\u0003\u0000\u0000\u0701\u072e\u0001\u0000\u0000\u0000\u0702\u0703"+
		"\u0005\u0104\u0000\u0000\u0703\u0704\u0005\u0002\u0000\u0000\u0704\u0705"+
		"\u0003p8\u0000\u0705\u0706\u0005\u0003\u0000\u0000\u0706\u072e\u0001\u0000"+
		"\u0000\u0000\u0707\u0708\u0005\u0002\u0000\u0000\u0708\u0709\u0003B!\u0000"+
		"\u0709\u070a\u0005\u0003\u0000\u0000\u070a\u072e\u0001\u0000\u0000\u0000"+
		"\u070b\u070c\u0005\u008e\u0000\u0000\u070c\u070d\u0005\u0002\u0000\u0000"+
		"\u070d\u070e\u0003\u0088D\u0000\u070e\u070f\u0005-\u0000\u0000\u070f\u0710"+
		"\u0005\u0002\u0000\u0000\u0710\u0715\u0003f3\u0000\u0711\u0712\u0005\u0004"+
		"\u0000\u0000\u0712\u0714\u0003f3\u0000\u0713\u0711\u0001\u0000\u0000\u0000"+
		"\u0714\u0717\u0001\u0000\u0000\u0000\u0715\u0713\u0001\u0000\u0000\u0000"+
		"\u0715\u0716\u0001\u0000\u0000\u0000\u0716\u0718\u0001\u0000\u0000\u0000"+
		"\u0717\u0715\u0001\u0000\u0000\u0000\u0718\u0724\u0005\u0003\u0000\u0000"+
		"\u0719\u071a\u0005\u00d2\u0000\u0000\u071a\u071b\u0005\u0002\u0000\u0000"+
		"\u071b\u071c\u0003h4\u0000\u071c\u071d\u0005\u0003\u0000\u0000\u071d\u0725"+
		"\u0001\u0000\u0000\u0000\u071e\u071f\u0005\u00d2\u0000\u0000\u071f\u0720"+
		"\u0005F\u0000\u0000\u0720\u0721\u0005\u0002\u0000\u0000\u0721\u0722\u0003"+
		"n7\u0000\u0722\u0723\u0005\u0003\u0000\u0000\u0723\u0725\u0001\u0000\u0000"+
		"\u0000\u0724\u0719\u0001\u0000\u0000\u0000\u0724\u071e\u0001\u0000\u0000"+
		"\u0000\u0724\u0725\u0001\u0000\u0000\u0000\u0725\u0729\u0001\u0000\u0000"+
		"\u0000\u0726\u0727\u0007\u000e\u0000\u0000\u0727\u0728\u0005\u00be\u0000"+
		"\u0000\u0728\u072a\u0005Y\u0000\u0000\u0729\u0726\u0001\u0000\u0000\u0000"+
		"\u0729\u072a\u0001\u0000\u0000\u0000\u072a\u072b\u0001\u0000\u0000\u0000"+
		"\u072b\u072c\u0005\u0003\u0000\u0000\u072c\u072e\u0001\u0000\u0000\u0000"+
		"\u072d\u06e6\u0001\u0000\u0000\u0000\u072d\u06ea\u0001\u0000\u0000\u0000"+
		"\u072d\u06ee\u0001\u0000\u0000\u0000\u072d\u06fd\u0001\u0000\u0000\u0000"+
		"\u072d\u0702\u0001\u0000\u0000\u0000\u072d\u0707\u0001\u0000\u0000\u0000"+
		"\u072d\u070b\u0001\u0000\u0000\u0000\u072ee\u0001\u0000\u0000\u0000\u072f"+
		"\u0730\u0003\u0100\u0080\u0000\u0730\u0731\u0005g\u0000\u0000\u0731\u0732"+
		"\u0005\u00c4\u0000\u0000\u0732\u077d\u0001\u0000\u0000\u0000\u0733\u0734"+
		"\u0003\u0100\u0080\u0000\u0734\u0737\u0003\u00aeW\u0000\u0735\u0736\u0005"+
		"\u00cd\u0000\u0000\u0736\u0738\u0003\u009eO\u0000\u0737\u0735\u0001\u0000"+
		"\u0000\u0000\u0737\u0738\u0001\u0000\u0000\u0000\u0738\u073d\u0001\u0000"+
		"\u0000\u0000\u0739\u073a\u0003\u0092I\u0000\u073a\u073b\u0005\u00be\u0000"+
		"\u0000\u073b\u073c\u0005U\u0000\u0000\u073c\u073e\u0001\u0000\u0000\u0000"+
		"\u073d\u0739\u0001\u0000\u0000\u0000\u073d\u073e\u0001\u0000\u0000\u0000"+
		"\u073e\u0743\u0001\u0000\u0000\u0000\u073f\u0740\u0003\u0092I\u0000\u0740"+
		"\u0741\u0005\u00be\u0000\u0000\u0741\u0742\u0005Y\u0000\u0000\u0742\u0744"+
		"\u0001\u0000\u0000\u0000\u0743\u073f\u0001\u0000\u0000\u0000\u0743\u0744"+
		"\u0001\u0000\u0000\u0000\u0744\u077d\u0001\u0000\u0000\u0000\u0745\u0746"+
		"\u0003\u0100\u0080\u0000\u0746\u0747\u0003\u00aeW\u0000\u0747\u0748\u0005"+
		"h\u0000\u0000\u0748\u074b\u0003\u008cF\u0000\u0749\u074a\u0005\u00cd\u0000"+
		"\u0000\u074a\u074c\u0003\u009eO\u0000\u074b\u0749\u0001\u0000\u0000\u0000"+
		"\u074b\u074c\u0001\u0000\u0000\u0000\u074c\u0750\u0001\u0000\u0000\u0000"+
		"\u074d\u074e\u0003\u0094J\u0000\u074e\u074f\u0005\u0134\u0000\u0000\u074f"+
		"\u0751\u0001\u0000\u0000\u0000\u0750\u074d\u0001\u0000\u0000\u0000\u0750"+
		"\u0751\u0001\u0000\u0000\u0000\u0751\u0759\u0001\u0000\u0000\u0000\u0752"+
		"\u0753\u0007\u000f\u0000\u0000\u0753\u0757\u0005\u00da\u0000\u0000\u0754"+
		"\u0755\u0005\u00be\u0000\u0000\u0755\u0756\u0005\u00f2\u0000\u0000\u0756"+
		"\u0758\u0005\u0108\u0000\u0000\u0757\u0754\u0001\u0000\u0000\u0000\u0757"+
		"\u0758\u0001\u0000\u0000\u0000\u0758\u075a\u0001\u0000\u0000\u0000\u0759"+
		"\u0752\u0001\u0000\u0000\u0000\u0759\u075a\u0001\u0000\u0000\u0000\u075a"+
		"\u075f\u0001\u0000\u0000\u0000\u075b\u075c\u0003\u0096K\u0000\u075c\u075d"+
		"\u0005\u00be\u0000\u0000\u075d\u075e\u0005U\u0000\u0000\u075e\u0760\u0001"+
		"\u0000\u0000\u0000\u075f\u075b\u0001\u0000\u0000\u0000\u075f\u0760\u0001"+
		"\u0000\u0000\u0000\u0760\u0765\u0001\u0000\u0000\u0000\u0761\u0762\u0003"+
		"\u0096K\u0000\u0762\u0763\u0005\u00be\u0000\u0000\u0763\u0764\u0005Y\u0000"+
		"\u0000\u0764\u0766\u0001\u0000\u0000\u0000\u0765\u0761\u0001\u0000\u0000"+
		"\u0000\u0765\u0766\u0001\u0000\u0000\u0000\u0766\u077d\u0001\u0000\u0000"+
		"\u0000\u0767\u0769\u0005\u00ad\u0000\u0000\u0768\u076a\u0005\u00cd\u0000"+
		"\u0000\u0769\u0768\u0001\u0000\u0000\u0000\u0769\u076a\u0001\u0000\u0000"+
		"\u0000\u076a\u076b\u0001\u0000\u0000\u0000\u076b\u076e\u0003\u009eO\u0000"+
		"\u076c\u076d\u0005\u001c\u0000\u0000\u076d\u076f\u0003\u0100\u0080\u0000"+
		"\u076e\u076c\u0001\u0000\u0000\u0000\u076e\u076f\u0001\u0000\u0000\u0000"+
		"\u076f\u0770\u0001\u0000\u0000\u0000\u0770\u0771\u0005-\u0000\u0000\u0771"+
		"\u0772\u0005\u0002\u0000\u0000\u0772\u0777\u0003f3\u0000\u0773\u0774\u0005"+
		"\u0004\u0000\u0000\u0774\u0776\u0003f3\u0000\u0775\u0773\u0001\u0000\u0000"+
		"\u0000\u0776\u0779\u0001\u0000\u0000\u0000\u0777\u0775\u0001\u0000\u0000"+
		"\u0000\u0777\u0778\u0001\u0000\u0000\u0000\u0778\u077a\u0001\u0000\u0000"+
		"\u0000\u0779\u0777\u0001\u0000\u0000\u0000\u077a\u077b\u0005\u0003\u0000"+
		"\u0000\u077b\u077d\u0001\u0000\u0000\u0000\u077c\u072f\u0001\u0000\u0000"+
		"\u0000\u077c\u0733\u0001\u0000\u0000\u0000\u077c\u0745\u0001\u0000\u0000"+
		"\u0000\u077c\u0767\u0001\u0000\u0000\u0000\u077dg\u0001\u0000\u0000\u0000"+
		"\u077e\u0798\u0003j5\u0000\u077f\u0780\u0003j5\u0000\u0780\u0781\u0007"+
		"\u0010\u0000\u0000\u0781\u0782\u0003l6\u0000\u0782\u0798\u0001\u0000\u0000"+
		"\u0000\u0783\u0784\u0003l6\u0000\u0784\u0785\u0005\u0119\u0000\u0000\u0785"+
		"\u078a\u0003l6\u0000\u0786\u0787\u0005\u0119\u0000\u0000\u0787\u0789\u0003"+
		"l6\u0000\u0788\u0786\u0001\u0000\u0000\u0000\u0789\u078c\u0001\u0000\u0000"+
		"\u0000\u078a\u0788\u0001\u0000\u0000\u0000\u078a\u078b\u0001\u0000\u0000"+
		"\u0000\u078b\u0798\u0001\u0000\u0000\u0000\u078c\u078a\u0001\u0000\u0000"+
		"\u0000\u078d\u078e\u0003l6\u0000\u078e\u078f\u00056\u0000\u0000\u078f"+
		"\u0794\u0003l6\u0000\u0790\u0791\u00056\u0000\u0000\u0791\u0793\u0003"+
		"l6\u0000\u0792\u0790\u0001\u0000\u0000\u0000\u0793\u0796\u0001\u0000\u0000"+
		"\u0000\u0794\u0792\u0001\u0000\u0000\u0000\u0794\u0795\u0001\u0000\u0000"+
		"\u0000\u0795\u0798\u0001\u0000\u0000\u0000\u0796\u0794\u0001\u0000\u0000"+
		"\u0000\u0797\u077e\u0001\u0000\u0000\u0000\u0797\u077f\u0001\u0000\u0000"+
		"\u0000\u0797\u0783\u0001\u0000\u0000\u0000\u0797\u078d\u0001\u0000\u0000"+
		"\u0000\u0798i\u0001\u0000\u0000\u0000\u0799\u079a\u0003\u0100\u0080\u0000"+
		"\u079ak\u0001\u0000\u0000\u0000\u079b\u07a1\u0003j5\u0000\u079c\u079d"+
		"\u0005\u0002\u0000\u0000\u079d\u079e\u0003h4\u0000\u079e\u079f\u0005\u0003"+
		"\u0000\u0000\u079f\u07a1\u0001\u0000\u0000\u0000\u07a0\u079b\u0001\u0000"+
		"\u0000\u0000\u07a0\u079c\u0001\u0000\u0000\u0000\u07a1m\u0001\u0000\u0000"+
		"\u0000\u07a2\u07a5\u0007\u0010\u0000\u0000\u07a3\u07a4\u0005\u0004\u0000"+
		"\u0000\u07a4\u07a6\u0007\u0011\u0000\u0000\u07a5\u07a3\u0001\u0000\u0000"+
		"\u0000\u07a5\u07a6\u0001\u0000\u0000\u0000\u07a6\u07ad\u0001\u0000\u0000"+
		"\u0000\u07a7\u07aa\u0007\u0011\u0000\u0000\u07a8\u07a9\u0005\u0004\u0000"+
		"\u0000\u07a9\u07ab\u0007\u0010\u0000\u0000\u07aa\u07a8\u0001\u0000\u0000"+
		"\u0000\u07aa\u07ab\u0001\u0000\u0000\u0000\u07ab\u07ad\u0001\u0000\u0000"+
		"\u0000\u07ac\u07a2\u0001\u0000\u0000\u0000\u07ac\u07a7\u0001\u0000\u0000"+
		"\u0000\u07ado\u0001\u0000\u0000\u0000\u07ae\u07af\u0003\u00f2y\u0000\u07af"+
		"\u07b8\u0005\u0002\u0000\u0000\u07b0\u07b5\u0003r9\u0000\u07b1\u07b2\u0005"+
		"\u0004\u0000\u0000\u07b2\u07b4\u0003r9\u0000\u07b3\u07b1\u0001\u0000\u0000"+
		"\u0000\u07b4\u07b7\u0001\u0000\u0000\u0000\u07b5\u07b3\u0001\u0000\u0000"+
		"\u0000\u07b5\u07b6\u0001\u0000\u0000\u0000\u07b6\u07b9\u0001\u0000\u0000"+
		"\u0000\u07b7\u07b5\u0001\u0000\u0000\u0000\u07b8\u07b0\u0001\u0000\u0000"+
		"\u0000\u07b8\u07b9\u0001\u0000\u0000\u0000\u07b9\u07c3\u0001\u0000\u0000"+
		"\u0000\u07ba\u07bb\u00054\u0000\u0000\u07bb\u07c0\u0003|>\u0000\u07bc"+
		"\u07bd\u0005\u0004\u0000\u0000\u07bd\u07bf\u0003|>\u0000\u07be\u07bc\u0001"+
		"\u0000\u0000\u0000\u07bf\u07c2\u0001\u0000\u0000\u0000\u07c0\u07be\u0001"+
		"\u0000\u0000\u0000\u07c0\u07c1\u0001\u0000\u0000\u0000\u07c1\u07c4\u0001"+
		"\u0000\u0000\u0000\u07c2\u07c0\u0001\u0000\u0000\u0000\u07c3\u07ba\u0001"+
		"\u0000\u0000\u0000\u07c3\u07c4\u0001\u0000\u0000\u0000\u07c4\u07c5\u0001"+
		"\u0000\u0000\u0000\u07c5\u07c6\u0005\u0003\u0000\u0000\u07c6q\u0001\u0000"+
		"\u0000\u0000\u07c7\u07c8\u0003\u0100\u0080\u0000\u07c8\u07c9\u0005\u0006"+
		"\u0000\u0000\u07c9\u07cb\u0001\u0000\u0000\u0000\u07ca\u07c7\u0001\u0000"+
		"\u0000\u0000\u07ca\u07cb\u0001\u0000\u0000\u0000\u07cb\u07cf\u0001\u0000"+
		"\u0000\u0000\u07cc\u07d0\u0003t:\u0000\u07cd\u07d0\u0003x<\u0000\u07ce"+
		"\u07d0\u0003~?\u0000\u07cf\u07cc\u0001\u0000\u0000\u0000\u07cf\u07cd\u0001"+
		"\u0000\u0000\u0000\u07cf\u07ce\u0001\u0000\u0000\u0000\u07d0s\u0001\u0000"+
		"\u0000\u0000\u07d1\u07e3\u0003v;\u0000\u07d2\u07d3\u0005\u00c9\u0000\u0000"+
		"\u07d3\u07e1\u0005$\u0000\u0000\u07d4\u07dd\u0005\u0002\u0000\u0000\u07d5"+
		"\u07da\u0003~?\u0000\u07d6\u07d7\u0005\u0004\u0000\u0000\u07d7\u07d9\u0003"+
		"~?\u0000\u07d8\u07d6\u0001\u0000\u0000\u0000\u07d9\u07dc\u0001\u0000\u0000"+
		"\u0000\u07da\u07d8\u0001\u0000\u0000\u0000\u07da\u07db\u0001\u0000\u0000"+
		"\u0000\u07db\u07de\u0001\u0000\u0000\u0000\u07dc\u07da\u0001\u0000\u0000"+
		"\u0000\u07dd\u07d5\u0001\u0000\u0000\u0000\u07dd\u07de\u0001\u0000\u0000"+
		"\u0000\u07de\u07df\u0001\u0000\u0000\u0000\u07df\u07e2\u0005\u0003\u0000"+
		"\u0000\u07e0\u07e2\u0003~?\u0000\u07e1\u07d4\u0001\u0000\u0000\u0000\u07e1"+
		"\u07e0\u0001\u0000\u0000\u0000\u07e2\u07e4\u0001\u0000\u0000\u0000\u07e3"+
		"\u07d2\u0001\u0000\u0000\u0000\u07e3\u07e4\u0001\u0000\u0000\u0000\u07e4"+
		"\u07eb\u0001\u0000\u0000\u0000\u07e5\u07e6\u0005\u00d9\u0000\u0000\u07e6"+
		"\u07e7\u0005\u012c\u0000\u0000\u07e7\u07ec\u0005U\u0000\u0000\u07e8\u07e9"+
		"\u0005\u0090\u0000\u0000\u07e9\u07ea\u0005\u012c\u0000\u0000\u07ea\u07ec"+
		"\u0005U\u0000\u0000\u07eb\u07e5\u0001\u0000\u0000\u0000\u07eb\u07e8\u0001"+
		"\u0000\u0000\u0000\u07eb\u07ec\u0001\u0000\u0000\u0000\u07ec\u07fd\u0001"+
		"\u0000\u0000\u0000\u07ed\u07ee\u0005\u00c3\u0000\u0000\u07ee\u07fb\u0005"+
		"$\u0000\u0000\u07ef\u07f0\u0005\u0002\u0000\u0000\u07f0\u07f5\u0003.\u0017"+
		"\u0000\u07f1\u07f2\u0005\u0004\u0000\u0000\u07f2\u07f4\u0003.\u0017\u0000"+
		"\u07f3\u07f1\u0001\u0000\u0000\u0000\u07f4\u07f7\u0001\u0000\u0000\u0000"+
		"\u07f5\u07f3\u0001\u0000\u0000\u0000\u07f5\u07f6\u0001\u0000\u0000\u0000"+
		"\u07f6\u07f8\u0001\u0000\u0000\u0000\u07f7\u07f5\u0001\u0000\u0000\u0000"+
		"\u07f8\u07f9\u0005\u0003\u0000\u0000\u07f9\u07fc\u0001\u0000\u0000\u0000"+
		"\u07fa\u07fc\u0003.\u0017\u0000\u07fb\u07ef\u0001\u0000\u0000\u0000\u07fb"+
		"\u07fa\u0001\u0000\u0000\u0000\u07fc\u07fe\u0001\u0000\u0000\u0000\u07fd"+
		"\u07ed\u0001\u0000\u0000\u0000\u07fd\u07fe\u0001\u0000\u0000\u0000\u07fe"+
		"u\u0001\u0000\u0000\u0000\u07ff\u0800\u0005\u0104\u0000\u0000\u0800\u0801"+
		"\u0005\u0002\u0000\u0000\u0801\u0802\u0003\u00f2y\u0000\u0802\u080a\u0005"+
		"\u0003\u0000\u0000\u0803\u0805\u0005\u001c\u0000\u0000\u0804\u0803\u0001"+
		"\u0000\u0000\u0000\u0804\u0805\u0001\u0000\u0000\u0000\u0805\u0806\u0001"+
		"\u0000\u0000\u0000\u0806\u0808\u0003\u0100\u0080\u0000\u0807\u0809\u0003"+
		"b1\u0000\u0808\u0807\u0001\u0000\u0000\u0000\u0808\u0809\u0001\u0000\u0000"+
		"\u0000\u0809\u080b\u0001\u0000\u0000\u0000\u080a\u0804\u0001\u0000\u0000"+
		"\u0000\u080a\u080b\u0001\u0000\u0000\u0000\u080b\u081a\u0001\u0000\u0000"+
		"\u0000\u080c\u080d\u0005\u0104\u0000\u0000\u080d\u080e\u0005\u0002\u0000"+
		"\u0000\u080e\u080f\u0003\u0012\t\u0000\u080f\u0817\u0005\u0003\u0000\u0000"+
		"\u0810\u0812\u0005\u001c\u0000\u0000\u0811\u0810\u0001\u0000\u0000\u0000"+
		"\u0811\u0812\u0001\u0000\u0000\u0000\u0812\u0813\u0001\u0000\u0000\u0000"+
		"\u0813\u0815\u0003\u0100\u0080\u0000\u0814\u0816\u0003b1\u0000\u0815\u0814"+
		"\u0001\u0000\u0000\u0000";
	private static final String _serializedATNSegment1 =
		"\u0815\u0816\u0001\u0000\u0000\u0000\u0816\u0818\u0001\u0000\u0000\u0000"+
		"\u0817\u0811\u0001\u0000\u0000\u0000\u0817\u0818\u0001\u0000\u0000\u0000"+
		"\u0818\u081a\u0001\u0000\u0000\u0000\u0819\u07ff\u0001\u0000\u0000\u0000"+
		"\u0819\u080c\u0001\u0000\u0000\u0000\u081aw\u0001\u0000\u0000\u0000\u081b"+
		"\u081c\u0005M\u0000\u0000\u081c\u081d\u0005\u0002\u0000\u0000\u081d\u0822"+
		"\u0003z=\u0000\u081e\u081f\u0005\u0004\u0000\u0000\u081f\u0821\u0003z"+
		"=\u0000\u0820\u081e\u0001\u0000\u0000\u0000\u0821\u0824\u0001\u0000\u0000"+
		"\u0000\u0822\u0820\u0001\u0000\u0000\u0000\u0822\u0823\u0001\u0000\u0000"+
		"\u0000\u0823\u0825\u0001\u0000\u0000\u0000\u0824\u0822\u0001\u0000\u0000"+
		"\u0000\u0825\u0826\u0005\u0003\u0000\u0000\u0826\u082e\u0001\u0000\u0000"+
		"\u0000\u0827\u0828\u0005)\u0000\u0000\u0828\u0829\u0005\u0002\u0000\u0000"+
		"\u0829\u082a\u0005\u00b7\u0000\u0000\u082a\u082b\u0005\u001c\u0000\u0000"+
		"\u082b\u082c\u0005M\u0000\u0000\u082c\u082e\u0005\u0003\u0000\u0000\u082d"+
		"\u081b\u0001\u0000\u0000\u0000\u082d\u0827\u0001\u0000\u0000\u0000\u082e"+
		"y\u0001\u0000\u0000\u0000\u082f\u0831\u0003\u0100\u0080\u0000\u0830\u0832"+
		"\u0003\u00aeW\u0000\u0831\u0830\u0001\u0000\u0000\u0000\u0831\u0832\u0001"+
		"\u0000\u0000\u0000\u0832{\u0001\u0000\u0000\u0000\u0833\u0834\u0005\u0002"+
		"\u0000\u0000\u0834\u0835\u0003\u00f2y\u0000\u0835\u0836\u0005\u0004\u0000"+
		"\u0000\u0836\u083b\u0003\u00f2y\u0000\u0837\u0838\u0005\u0004\u0000\u0000"+
		"\u0838\u083a\u0003\u00f2y\u0000\u0839\u0837\u0001\u0000\u0000\u0000\u083a"+
		"\u083d\u0001\u0000\u0000\u0000\u083b\u0839\u0001\u0000\u0000\u0000\u083b"+
		"\u083c\u0001\u0000\u0000\u0000\u083c\u083e\u0001\u0000\u0000\u0000\u083d"+
		"\u083b\u0001\u0000\u0000\u0000\u083e\u083f\u0005\u0003\u0000\u0000\u083f"+
		"}\u0001\u0000\u0000\u0000\u0840\u0841\u0003\u0080@\u0000\u0841\u007f\u0001"+
		"\u0000\u0000\u0000\u0842\u0843\u0006@\uffff\uffff\u0000\u0843\u0845\u0003"+
		"\u0084B\u0000\u0844\u0846\u0003\u0082A\u0000\u0845\u0844\u0001\u0000\u0000"+
		"\u0000\u0845\u0846\u0001\u0000\u0000\u0000\u0846\u084a\u0001\u0000\u0000"+
		"\u0000\u0847\u0848\u0005\u00b6\u0000\u0000\u0848\u084a\u0003\u0080@\u0003"+
		"\u0849\u0842\u0001\u0000\u0000\u0000\u0849\u0847\u0001\u0000\u0000\u0000"+
		"\u084a\u0853\u0001\u0000\u0000\u0000\u084b\u084c\n\u0002\u0000\u0000\u084c"+
		"\u084d\u0005\u0019\u0000\u0000\u084d\u0852\u0003\u0080@\u0003\u084e\u084f"+
		"\n\u0001\u0000\u0000\u084f\u0850\u0005\u00c2\u0000\u0000\u0850\u0852\u0003"+
		"\u0080@\u0002\u0851\u084b\u0001\u0000\u0000\u0000\u0851\u084e\u0001\u0000"+
		"\u0000\u0000\u0852\u0855\u0001\u0000\u0000\u0000\u0853\u0851\u0001\u0000"+
		"\u0000\u0000\u0853\u0854\u0001\u0000\u0000\u0000\u0854\u0081\u0001\u0000"+
		"\u0000\u0000\u0855\u0853\u0001\u0000\u0000\u0000\u0856\u0857\u0003\u00a2"+
		"Q\u0000\u0857\u0858\u0003\u0084B\u0000\u0858\u0894\u0001\u0000\u0000\u0000"+
		"\u0859\u085a\u0003\u00a2Q\u0000\u085a\u085b\u0003\u00a4R\u0000\u085b\u085c"+
		"\u0005\u0002\u0000\u0000\u085c\u085d\u0003\u0012\t\u0000\u085d\u085e\u0005"+
		"\u0003\u0000\u0000\u085e\u0894\u0001\u0000\u0000\u0000\u085f\u0861\u0005"+
		"\u00b6\u0000\u0000\u0860\u085f\u0001\u0000\u0000\u0000\u0860\u0861\u0001"+
		"\u0000\u0000\u0000\u0861\u0862\u0001\u0000\u0000\u0000\u0862\u0863\u0005"+
		"\"\u0000\u0000\u0863\u0864\u0003\u0084B\u0000\u0864\u0865\u0005\u0019"+
		"\u0000\u0000\u0865\u0866\u0003\u0084B\u0000\u0866\u0894\u0001\u0000\u0000"+
		"\u0000\u0867\u0869\u0005\u00b6\u0000\u0000\u0868\u0867\u0001\u0000\u0000"+
		"\u0000\u0868\u0869\u0001\u0000\u0000\u0000\u0869\u086a\u0001\u0000\u0000"+
		"\u0000\u086a\u086b\u0005z\u0000\u0000\u086b\u086c\u0005\u0002\u0000\u0000"+
		"\u086c\u0871\u0003~?\u0000\u086d\u086e\u0005\u0004\u0000\u0000\u086e\u0870"+
		"\u0003~?\u0000\u086f\u086d\u0001\u0000\u0000\u0000\u0870\u0873\u0001\u0000"+
		"\u0000\u0000\u0871\u086f\u0001\u0000\u0000\u0000\u0871\u0872\u0001\u0000"+
		"\u0000\u0000\u0872\u0874\u0001\u0000\u0000\u0000\u0873\u0871\u0001\u0000"+
		"\u0000\u0000\u0874\u0875\u0005\u0003\u0000\u0000\u0875\u0894\u0001\u0000"+
		"\u0000\u0000\u0876\u0878\u0005\u00b6\u0000\u0000\u0877\u0876\u0001\u0000"+
		"\u0000\u0000\u0877\u0878\u0001\u0000\u0000\u0000\u0878\u0879\u0001\u0000"+
		"\u0000\u0000\u0879\u087a\u0005z\u0000\u0000\u087a\u087b\u0005\u0002\u0000"+
		"\u0000\u087b\u087c\u0003\u0012\t\u0000\u087c\u087d\u0005\u0003\u0000\u0000"+
		"\u087d\u0894\u0001\u0000\u0000\u0000\u087e\u0880\u0005\u00b6\u0000\u0000"+
		"\u087f\u087e\u0001\u0000\u0000\u0000\u087f\u0880\u0001\u0000\u0000\u0000"+
		"\u0880\u0881\u0001\u0000\u0000\u0000\u0881\u0882\u0005\u009a\u0000\u0000"+
		"\u0882\u0885\u0003\u0084B\u0000\u0883\u0884\u0005Z\u0000\u0000\u0884\u0886"+
		"\u0003\u0084B\u0000\u0885\u0883\u0001\u0000\u0000\u0000\u0885\u0886\u0001"+
		"\u0000\u0000\u0000\u0886\u0894\u0001\u0000\u0000\u0000\u0887\u0889\u0005"+
		"\u0085\u0000\u0000\u0888\u088a\u0005\u00b6\u0000\u0000\u0889\u0888\u0001"+
		"\u0000\u0000\u0000\u0889\u088a\u0001\u0000\u0000\u0000\u088a\u088b\u0001"+
		"\u0000\u0000\u0000\u088b\u0894\u0005\u00b7\u0000\u0000\u088c\u088e\u0005"+
		"\u0085\u0000\u0000\u088d\u088f\u0005\u00b6\u0000\u0000\u088e\u088d\u0001"+
		"\u0000\u0000\u0000\u088e\u088f\u0001\u0000\u0000\u0000\u088f\u0890\u0001"+
		"\u0000\u0000\u0000\u0890\u0891\u0005O\u0000\u0000\u0891\u0892\u0005i\u0000"+
		"\u0000\u0892\u0894\u0003\u0084B\u0000\u0893\u0856\u0001\u0000\u0000\u0000"+
		"\u0893\u0859\u0001\u0000\u0000\u0000\u0893\u0860\u0001\u0000\u0000\u0000"+
		"\u0893\u0868\u0001\u0000\u0000\u0000\u0893\u0877\u0001\u0000\u0000\u0000"+
		"\u0893\u087f\u0001\u0000\u0000\u0000\u0893\u0887\u0001\u0000\u0000\u0000"+
		"\u0893\u088c\u0001\u0000\u0000\u0000\u0894\u0083\u0001\u0000\u0000\u0000"+
		"\u0895\u0896\u0006B\uffff\uffff\u0000\u0896\u089a\u0003\u0086C\u0000\u0897"+
		"\u0898\u0007\u0012\u0000\u0000\u0898\u089a\u0003\u0084B\u0004\u0899\u0895"+
		"\u0001\u0000\u0000\u0000\u0899\u0897\u0001\u0000\u0000\u0000\u089a\u08a9"+
		"\u0001\u0000\u0000\u0000\u089b\u089c\n\u0003\u0000\u0000\u089c\u089d\u0007"+
		"\u0013\u0000\u0000\u089d\u08a8\u0003\u0084B\u0004\u089e\u089f\n\u0002"+
		"\u0000\u0000\u089f\u08a0\u0007\u0012\u0000\u0000\u08a0\u08a8\u0003\u0084"+
		"B\u0003\u08a1\u08a2\n\u0001\u0000\u0000\u08a2\u08a3\u0005\u0143\u0000"+
		"\u0000\u08a3\u08a8\u0003\u0084B\u0002\u08a4\u08a5\n\u0005\u0000\u0000"+
		"\u08a5\u08a6\u0005\u001e\u0000\u0000\u08a6\u08a8\u0003\u00a0P\u0000\u08a7"+
		"\u089b\u0001\u0000\u0000\u0000\u08a7\u089e\u0001\u0000\u0000\u0000\u08a7"+
		"\u08a1\u0001\u0000\u0000\u0000\u08a7\u08a4\u0001\u0000\u0000\u0000\u08a8"+
		"\u08ab\u0001\u0000\u0000\u0000\u08a9\u08a7\u0001\u0000\u0000\u0000\u08a9"+
		"\u08aa\u0001\u0000\u0000\u0000\u08aa\u0085\u0001\u0000\u0000\u0000\u08ab"+
		"\u08a9\u0001\u0000\u0000\u0000\u08ac\u08ad\u0006C\uffff\uffff\u0000\u08ad"+
		"\u0a72\u0005\u00b7\u0000\u0000\u08ae\u0a72\u0003\u00a8T\u0000\u08af\u08b0"+
		"\u0003\u0100\u0080\u0000\u08b0\u08b1\u0003\u009eO\u0000\u08b1\u0a72\u0001"+
		"\u0000\u0000\u0000\u08b2\u08b3\u0005R\u0000\u0000\u08b3\u08b4\u0005\u00d5"+
		"\u0000\u0000\u08b4\u0a72\u0003\u009eO\u0000\u08b5\u0a72\u0003\u0102\u0081"+
		"\u0000\u08b6\u0a72\u0003\u00a6S\u0000\u08b7\u0a72\u0003\u009eO\u0000\u08b8"+
		"\u0a72\u0005\u0148\u0000\u0000\u08b9\u0a72\u0005\u0144\u0000\u0000\u08ba"+
		"\u08bb\u0005\u00d3\u0000\u0000\u08bb\u08bc\u0005\u0002\u0000\u0000\u08bc"+
		"\u08bd\u0003\u0084B\u0000\u08bd\u08be\u0005z\u0000\u0000\u08be\u08bf\u0003"+
		"\u0084B\u0000\u08bf\u08c0\u0005\u0003\u0000\u0000\u08c0\u0a72\u0001\u0000"+
		"\u0000\u0000\u08c1\u08c2\u0005\u0002\u0000\u0000\u08c2\u08c5\u0003~?\u0000"+
		"\u08c3\u08c4\u0005\u0004\u0000\u0000\u08c4\u08c6\u0003~?\u0000\u08c5\u08c3"+
		"\u0001\u0000\u0000\u0000\u08c6\u08c7\u0001\u0000\u0000\u0000\u08c7\u08c5"+
		"\u0001\u0000\u0000\u0000\u08c7\u08c8\u0001\u0000\u0000\u0000\u08c8\u08c9"+
		"\u0001\u0000\u0000\u0000\u08c9\u08ca\u0005\u0003\u0000\u0000\u08ca\u0a72"+
		"\u0001\u0000\u0000\u0000\u08cb\u08cc\u0005\u00ef\u0000\u0000\u08cc\u08cd"+
		"\u0005\u0002\u0000\u0000\u08cd\u08d2\u0003~?\u0000\u08ce\u08cf\u0005\u0004"+
		"\u0000\u0000\u08cf\u08d1\u0003~?\u0000\u08d0\u08ce\u0001\u0000\u0000\u0000"+
		"\u08d1\u08d4\u0001\u0000\u0000\u0000\u08d2\u08d0\u0001\u0000\u0000\u0000"+
		"\u08d2\u08d3\u0001\u0000\u0000\u0000\u08d3\u08d5\u0001\u0000\u0000\u0000"+
		"\u08d4\u08d2\u0001\u0000\u0000\u0000\u08d5\u08d6\u0005\u0003\u0000\u0000"+
		"\u08d6\u0a72\u0001\u0000\u0000\u0000\u08d7\u08d8\u0005\u009c\u0000\u0000"+
		"\u08d8\u08da\u0005\u0002\u0000\u0000\u08d9\u08db\u0003>\u001f\u0000\u08da"+
		"\u08d9\u0001\u0000\u0000\u0000\u08da\u08db\u0001\u0000\u0000\u0000\u08db"+
		"\u08dc\u0001\u0000\u0000\u0000\u08dc\u08df\u0003~?\u0000\u08dd\u08de\u0005"+
		"\u0004\u0000\u0000\u08de\u08e0\u0003\u009eO\u0000\u08df\u08dd\u0001\u0000"+
		"\u0000\u0000\u08df\u08e0\u0001\u0000\u0000\u0000\u08e0\u08e4\u0001\u0000"+
		"\u0000\u0000\u08e1\u08e2\u0005\u00be\u0000\u0000\u08e2\u08e3\u0005\u00c8"+
		"\u0000\u0000\u08e3\u08e5\u0003N\'\u0000\u08e4\u08e1\u0001\u0000\u0000"+
		"\u0000\u08e4\u08e5\u0001\u0000\u0000\u0000\u08e5\u08e6\u0001\u0000\u0000"+
		"\u0000\u08e6\u08e7\u0005\u0003\u0000\u0000\u08e7\u08e8\u0005\u0131\u0000"+
		"\u0000\u08e8\u08e9\u0005r\u0000\u0000\u08e9\u08ea\u0005\u0002\u0000\u0000"+
		"\u08ea\u08eb\u0005\u00c3\u0000\u0000\u08eb\u08ec\u0005$\u0000\u0000\u08ec"+
		"\u08f1\u0003.\u0017\u0000\u08ed\u08ee\u0005\u0004\u0000\u0000\u08ee\u08f0"+
		"\u0003.\u0017\u0000\u08ef\u08ed\u0001\u0000\u0000\u0000\u08f0\u08f3\u0001"+
		"\u0000\u0000\u0000\u08f1\u08ef\u0001\u0000\u0000\u0000\u08f1\u08f2\u0001"+
		"\u0000\u0000\u0000\u08f2\u08f4\u0001\u0000\u0000\u0000\u08f3\u08f1\u0001"+
		"\u0000\u0000\u0000\u08f4\u08f5\u0005\u0003\u0000\u0000\u08f5\u08f7\u0001"+
		"\u0000\u0000\u0000\u08f6\u08f8\u0003\u00b6[\u0000\u08f7\u08f6\u0001\u0000"+
		"\u0000\u0000\u08f7\u08f8\u0001\u0000\u0000\u0000\u08f8\u0a72\u0001\u0000"+
		"\u0000\u0000\u08f9\u08fb\u0003\u009aM\u0000\u08fa\u08f9\u0001\u0000\u0000"+
		"\u0000\u08fa\u08fb\u0001\u0000\u0000\u0000\u08fb\u08fc\u0001\u0000\u0000"+
		"\u0000\u08fc\u08fd\u0003\u00f2y\u0000\u08fd\u0901\u0005\u0002\u0000\u0000"+
		"\u08fe\u08ff\u0003\u0100\u0080\u0000\u08ff\u0900\u0005\u0001\u0000\u0000"+
		"\u0900\u0902\u0001\u0000\u0000\u0000\u0901\u08fe\u0001\u0000\u0000\u0000"+
		"\u0901\u0902\u0001\u0000\u0000\u0000\u0902\u0903\u0001\u0000\u0000\u0000"+
		"\u0903\u0904\u0005\u0140\u0000\u0000\u0904\u0906\u0005\u0003\u0000\u0000"+
		"\u0905\u0907\u0003\u00b6[\u0000\u0906\u0905\u0001\u0000\u0000\u0000\u0906"+
		"\u0907\u0001\u0000\u0000\u0000\u0907\u0909\u0001\u0000\u0000\u0000\u0908"+
		"\u090a\u0003\u00ba]\u0000\u0909\u0908\u0001\u0000\u0000\u0000\u0909\u090a"+
		"\u0001\u0000\u0000\u0000\u090a\u0a72\u0001\u0000\u0000\u0000\u090b\u090d"+
		"\u0003\u009aM\u0000\u090c\u090b\u0001\u0000\u0000\u0000\u090c\u090d\u0001"+
		"\u0000\u0000\u0000\u090d\u090e\u0001\u0000\u0000\u0000\u090e\u090f\u0003"+
		"\u00f2y\u0000\u090f\u091b\u0005\u0002\u0000\u0000\u0910\u0912\u0003>\u001f"+
		"\u0000\u0911\u0910\u0001\u0000\u0000\u0000\u0911\u0912\u0001\u0000\u0000"+
		"\u0000\u0912\u0913\u0001\u0000\u0000\u0000\u0913\u0918\u0003~?\u0000\u0914"+
		"\u0915\u0005\u0004\u0000\u0000\u0915\u0917\u0003~?\u0000\u0916\u0914\u0001"+
		"\u0000\u0000\u0000\u0917\u091a\u0001\u0000\u0000\u0000\u0918\u0916\u0001"+
		"\u0000\u0000\u0000\u0918\u0919\u0001\u0000\u0000\u0000\u0919\u091c\u0001"+
		"\u0000\u0000\u0000\u091a\u0918\u0001\u0000\u0000\u0000\u091b\u0911\u0001"+
		"\u0000\u0000\u0000\u091b\u091c\u0001\u0000\u0000\u0000\u091c\u0927\u0001"+
		"\u0000\u0000\u0000\u091d\u091e\u0005\u00c3\u0000\u0000\u091e\u091f\u0005"+
		"$\u0000\u0000\u091f\u0924\u0003.\u0017\u0000\u0920\u0921\u0005\u0004\u0000"+
		"\u0000\u0921\u0923\u0003.\u0017\u0000\u0922\u0920\u0001\u0000\u0000\u0000"+
		"\u0923\u0926\u0001\u0000\u0000\u0000\u0924\u0922\u0001\u0000\u0000\u0000"+
		"\u0924\u0925\u0001\u0000\u0000\u0000\u0925\u0928\u0001\u0000\u0000\u0000"+
		"\u0926\u0924\u0001\u0000\u0000\u0000\u0927\u091d\u0001\u0000\u0000\u0000"+
		"\u0927\u0928\u0001\u0000\u0000\u0000\u0928\u0929\u0001\u0000\u0000\u0000"+
		"\u0929\u092b\u0005\u0003\u0000\u0000\u092a\u092c\u0003\u00b6[\u0000\u092b"+
		"\u092a\u0001\u0000\u0000\u0000\u092b\u092c\u0001\u0000\u0000\u0000\u092c"+
		"\u0931\u0001\u0000\u0000\u0000\u092d\u092f\u0003\u009cN\u0000\u092e\u092d"+
		"\u0001\u0000\u0000\u0000\u092e\u092f\u0001\u0000\u0000\u0000\u092f\u0930"+
		"\u0001\u0000\u0000\u0000\u0930\u0932\u0003\u00ba]\u0000\u0931\u092e\u0001"+
		"\u0000\u0000\u0000\u0931\u0932\u0001\u0000\u0000\u0000\u0932\u0a72\u0001"+
		"\u0000\u0000\u0000\u0933\u0934\u0003\u0100\u0080\u0000\u0934\u0935\u0003"+
		"\u00ba]\u0000\u0935\u0a72\u0001\u0000\u0000\u0000\u0936\u0937\u0003\u0100"+
		"\u0080\u0000\u0937\u0938\u0005\u0007\u0000\u0000\u0938\u0939\u0003~?\u0000"+
		"\u0939\u0a72\u0001\u0000\u0000\u0000\u093a\u0943\u0005\u0002\u0000\u0000"+
		"\u093b\u0940\u0003\u0100\u0080\u0000\u093c\u093d\u0005\u0004\u0000\u0000"+
		"\u093d\u093f\u0003\u0100\u0080\u0000\u093e\u093c\u0001\u0000\u0000\u0000"+
		"\u093f\u0942\u0001\u0000\u0000\u0000\u0940\u093e\u0001\u0000\u0000\u0000"+
		"\u0940\u0941\u0001\u0000\u0000\u0000\u0941\u0944\u0001\u0000\u0000\u0000"+
		"\u0942\u0940\u0001\u0000\u0000\u0000\u0943\u093b\u0001\u0000\u0000\u0000"+
		"\u0943\u0944\u0001\u0000\u0000\u0000\u0944\u0945\u0001\u0000\u0000\u0000"+
		"\u0945\u0946\u0005\u0003\u0000\u0000\u0946\u0947\u0005\u0007\u0000\u0000"+
		"\u0947\u0a72\u0003~?\u0000\u0948\u0949\u0005\u0002\u0000\u0000\u0949\u094a"+
		"\u0003\u0012\t\u0000\u094a\u094b\u0005\u0003\u0000\u0000\u094b\u0a72\u0001"+
		"\u0000\u0000\u0000\u094c\u094d\u0005^\u0000\u0000\u094d\u094e\u0005\u0002"+
		"\u0000\u0000\u094e\u094f\u0003\u0012\t\u0000\u094f\u0950\u0005\u0003\u0000"+
		"\u0000\u0950\u0a72\u0001\u0000\u0000\u0000\u0951\u0952\u0005(\u0000\u0000"+
		"\u0952\u0954\u0003~?\u0000\u0953\u0955\u0003\u00b4Z\u0000\u0954\u0953"+
		"\u0001\u0000\u0000\u0000\u0955\u0956\u0001\u0000\u0000\u0000\u0956\u0954"+
		"\u0001\u0000\u0000\u0000\u0956\u0957\u0001\u0000\u0000\u0000\u0957\u095a"+
		"\u0001\u0000\u0000\u0000\u0958\u0959\u0005T\u0000\u0000\u0959\u095b\u0003"+
		"~?\u0000\u095a\u0958\u0001\u0000\u0000\u0000\u095a\u095b\u0001\u0000\u0000"+
		"\u0000\u095b\u095c\u0001\u0000\u0000\u0000\u095c\u095d\u0005X\u0000\u0000"+
		"\u095d\u0a72\u0001\u0000\u0000\u0000\u095e\u0960\u0005(\u0000\u0000\u095f"+
		"\u0961\u0003\u00b4Z\u0000\u0960\u095f\u0001\u0000\u0000\u0000\u0961\u0962"+
		"\u0001\u0000\u0000\u0000\u0962\u0960\u0001\u0000\u0000\u0000\u0962\u0963"+
		"\u0001\u0000\u0000\u0000\u0963\u0966\u0001\u0000\u0000\u0000\u0964\u0965"+
		"\u0005T\u0000\u0000\u0965\u0967\u0003~?\u0000\u0966\u0964\u0001\u0000"+
		"\u0000\u0000\u0966\u0967\u0001\u0000\u0000\u0000\u0967\u0968\u0001\u0000"+
		"\u0000\u0000\u0968\u0969\u0005X\u0000\u0000\u0969\u0a72\u0001\u0000\u0000"+
		"\u0000\u096a\u096b\u0005)\u0000\u0000\u096b\u096c\u0005\u0002\u0000\u0000"+
		"\u096c\u096d\u0003~?\u0000\u096d\u096e\u0005\u001c\u0000\u0000\u096e\u096f"+
		"\u0003\u00aeW\u0000\u096f\u0970\u0005\u0003\u0000\u0000\u0970\u0a72\u0001"+
		"\u0000\u0000\u0000\u0971\u0972\u0005\u0113\u0000\u0000\u0972\u0973\u0005"+
		"\u0002\u0000\u0000\u0973\u0974\u0003~?\u0000\u0974\u0975\u0005\u001c\u0000"+
		"\u0000\u0975\u0976\u0003\u00aeW\u0000\u0976\u0977\u0005\u0003\u0000\u0000"+
		"\u0977\u0a72\u0001\u0000\u0000\u0000\u0978\u0979\u0005\u001b\u0000\u0000"+
		"\u0979\u0982\u0005\b\u0000\u0000\u097a\u097f\u0003~?\u0000\u097b\u097c"+
		"\u0005\u0004\u0000\u0000\u097c\u097e\u0003~?\u0000\u097d\u097b\u0001\u0000"+
		"\u0000\u0000\u097e\u0981\u0001\u0000\u0000\u0000\u097f\u097d\u0001\u0000"+
		"\u0000\u0000\u097f\u0980\u0001\u0000\u0000\u0000\u0980\u0983\u0001\u0000"+
		"\u0000\u0000\u0981\u097f\u0001\u0000\u0000\u0000\u0982\u097a\u0001\u0000"+
		"\u0000\u0000\u0982\u0983\u0001\u0000\u0000\u0000\u0983\u0984\u0001\u0000"+
		"\u0000\u0000\u0984\u0a72\u0005\t\u0000\u0000\u0985\u0a72\u0003\u0100\u0080"+
		"\u0000\u0986\u0a72\u0005:\u0000\u0000\u0987\u098b\u0005>\u0000\u0000\u0988"+
		"\u0989\u0005\u0002\u0000\u0000\u0989\u098a\u0005\u0149\u0000\u0000\u098a"+
		"\u098c\u0005\u0003\u0000\u0000\u098b\u0988\u0001\u0000\u0000\u0000\u098b"+
		"\u098c\u0001\u0000\u0000\u0000\u098c\u0a72\u0001\u0000\u0000\u0000\u098d"+
		"\u0991\u0005?\u0000\u0000\u098e\u098f\u0005\u0002\u0000\u0000\u098f\u0990"+
		"\u0005\u0149\u0000\u0000\u0990\u0992\u0005\u0003\u0000\u0000\u0991\u098e"+
		"\u0001\u0000\u0000\u0000\u0991\u0992\u0001\u0000\u0000\u0000\u0992\u0a72"+
		"\u0001\u0000\u0000\u0000\u0993\u0997\u0005\u009e\u0000\u0000\u0994\u0995"+
		"\u0005\u0002\u0000\u0000\u0995\u0996\u0005\u0149\u0000\u0000\u0996\u0998"+
		"\u0005\u0003\u0000\u0000\u0997\u0994\u0001\u0000\u0000\u0000\u0997\u0998"+
		"\u0001\u0000\u0000\u0000\u0998\u0a72\u0001\u0000\u0000\u0000\u0999\u099d"+
		"\u0005\u009f\u0000\u0000\u099a\u099b\u0005\u0002\u0000\u0000\u099b\u099c"+
		"\u0005\u0149\u0000\u0000\u099c\u099e\u0005\u0003\u0000\u0000\u099d\u099a"+
		"\u0001\u0000\u0000\u0000\u099d\u099e\u0001\u0000\u0000\u0000\u099e\u0a72"+
		"\u0001\u0000\u0000\u0000\u099f\u0a72\u0005@\u0000\u0000\u09a0\u0a72\u0005"+
		"9\u0000\u0000\u09a1\u0a72\u0005=\u0000\u0000\u09a2\u0a72\u0005;\u0000"+
		"\u0000\u09a3\u09a4\u0005\u0110\u0000\u0000\u09a4\u09ac\u0005\u0002\u0000"+
		"\u0000\u09a5\u09a7\u0003L&\u0000\u09a6\u09a5\u0001\u0000\u0000\u0000\u09a6"+
		"\u09a7\u0001\u0000\u0000\u0000\u09a7\u09a9\u0001\u0000\u0000\u0000\u09a8"+
		"\u09aa\u0003\u0084B\u0000\u09a9\u09a8\u0001\u0000\u0000\u0000\u09a9\u09aa"+
		"\u0001\u0000\u0000\u0000\u09aa\u09ab\u0001\u0000\u0000\u0000\u09ab\u09ad"+
		"\u0005i\u0000\u0000\u09ac\u09a6\u0001\u0000\u0000\u0000\u09ac\u09ad\u0001"+
		"\u0000\u0000\u0000\u09ad\u09ae\u0001\u0000\u0000\u0000\u09ae\u09af\u0003"+
		"\u0084B\u0000\u09af\u09b0\u0005\u0003\u0000\u0000\u09b0\u0a72\u0001\u0000"+
		"\u0000\u0000\u09b1\u09b2\u0005\u0110\u0000\u0000\u09b2\u09b3\u0005\u0002"+
		"\u0000\u0000\u09b3\u09b4\u0003\u0084B\u0000\u09b4\u09b5\u0005\u0004\u0000"+
		"\u0000\u09b5\u09b6\u0003\u0084B\u0000\u09b6\u09b7\u0005\u0003\u0000\u0000"+
		"\u09b7\u0a72\u0001\u0000\u0000\u0000\u09b8\u09b9\u0005\u0102\u0000\u0000"+
		"\u09b9\u09ba\u0005\u0002\u0000\u0000\u09ba\u09bb\u0003\u0084B\u0000\u09bb"+
		"\u09bc\u0005i\u0000\u0000\u09bc\u09bf\u0003\u0084B\u0000\u09bd\u09be\u0005"+
		"g\u0000\u0000\u09be\u09c0\u0003\u0084B\u0000\u09bf\u09bd\u0001\u0000\u0000"+
		"\u0000\u09bf\u09c0\u0001\u0000\u0000\u0000\u09c0\u09c1\u0001\u0000\u0000"+
		"\u0000\u09c1\u09c2\u0005\u0003\u0000\u0000\u09c2\u0a72\u0001\u0000\u0000"+
		"\u0000\u09c3\u09c4\u0005\u00b5\u0000\u0000\u09c4\u09c5\u0005\u0002\u0000"+
		"\u0000\u09c5\u09c8\u0003\u0084B\u0000\u09c6\u09c7\u0005\u0004\u0000\u0000"+
		"\u09c7\u09c9\u0003\u00acV\u0000\u09c8\u09c6\u0001\u0000\u0000\u0000\u09c8"+
		"\u09c9\u0001\u0000\u0000\u0000\u09c9\u09ca\u0001\u0000\u0000\u0000\u09ca"+
		"\u09cb\u0005\u0003\u0000\u0000\u09cb\u0a72\u0001\u0000\u0000\u0000\u09cc"+
		"\u09cd\u0005`\u0000\u0000\u09cd\u09ce\u0005\u0002\u0000\u0000\u09ce\u09cf"+
		"\u0003\u0100\u0080\u0000\u09cf\u09d0\u0005i\u0000\u0000\u09d0\u09d1\u0003"+
		"\u0084B\u0000\u09d1\u09d2\u0005\u0003\u0000\u0000\u09d2\u0a72\u0001\u0000"+
		"\u0000\u0000\u09d3\u09d4\u0005\u0002\u0000\u0000\u09d4\u09d5\u0003~?\u0000"+
		"\u09d5\u09d6\u0005\u0003\u0000\u0000\u09d6\u0a72\u0001\u0000\u0000\u0000"+
		"\u09d7\u09d8\u0005s\u0000\u0000\u09d8\u09e1\u0005\u0002\u0000\u0000\u09d9"+
		"\u09de\u0003\u00f2y\u0000\u09da\u09db\u0005\u0004\u0000\u0000\u09db\u09dd"+
		"\u0003\u00f2y\u0000\u09dc\u09da\u0001\u0000\u0000\u0000\u09dd\u09e0\u0001"+
		"\u0000\u0000\u0000\u09de\u09dc\u0001\u0000\u0000\u0000\u09de\u09df\u0001"+
		"\u0000\u0000\u0000\u09df\u09e2\u0001\u0000\u0000\u0000\u09e0\u09de\u0001"+
		"\u0000\u0000\u0000\u09e1\u09d9\u0001\u0000\u0000\u0000\u09e1\u09e2\u0001"+
		"\u0000\u0000\u0000\u09e2\u09e3\u0001\u0000\u0000\u0000\u09e3\u0a72\u0005"+
		"\u0003\u0000\u0000\u09e4\u09e5\u0005\u008b\u0000\u0000\u09e5\u09e6\u0005"+
		"\u0002\u0000\u0000\u09e6\u09eb\u0003\u0088D\u0000\u09e7\u09e8\u0003\u0090"+
		"H\u0000\u09e8\u09e9\u0005\u00be\u0000\u0000\u09e9\u09ea\u0005Y\u0000\u0000"+
		"\u09ea\u09ec\u0001\u0000\u0000\u0000\u09eb\u09e7\u0001\u0000\u0000\u0000"+
		"\u09eb\u09ec\u0001\u0000\u0000\u0000\u09ec\u09ed\u0001\u0000\u0000\u0000"+
		"\u09ed\u09ee\u0005\u0003\u0000\u0000\u09ee\u0a72\u0001\u0000\u0000\u0000"+
		"\u09ef\u09f0\u0005\u008f\u0000\u0000\u09f0\u09f1\u0005\u0002\u0000\u0000"+
		"\u09f1\u09f4\u0003\u0088D\u0000\u09f2\u09f3\u0005\u00e7\u0000\u0000\u09f3"+
		"\u09f5\u0003\u00aeW\u0000\u09f4\u09f2\u0001\u0000\u0000\u0000\u09f4\u09f5"+
		"\u0001\u0000\u0000\u0000\u09f5\u09fa\u0001\u0000\u0000\u0000\u09f6\u09f7"+
		"\u0003\u0092I\u0000\u09f7\u09f8\u0005\u00be\u0000\u0000\u09f8\u09f9\u0005"+
		"U\u0000\u0000\u09f9\u09fb\u0001\u0000\u0000\u0000\u09fa\u09f6\u0001\u0000"+
		"\u0000\u0000\u09fa\u09fb\u0001\u0000\u0000\u0000\u09fb\u0a00\u0001\u0000"+
		"\u0000\u0000\u09fc\u09fd\u0003\u0092I\u0000\u09fd\u09fe\u0005\u00be\u0000"+
		"\u0000\u09fe\u09ff\u0005Y\u0000\u0000\u09ff\u0a01\u0001\u0000\u0000\u0000"+
		"\u0a00\u09fc\u0001\u0000\u0000\u0000\u0a00\u0a01\u0001\u0000\u0000\u0000"+
		"\u0a01\u0a02\u0001\u0000\u0000\u0000\u0a02\u0a03\u0005\u0003\u0000\u0000"+
		"\u0a03\u0a72\u0001\u0000\u0000\u0000\u0a04\u0a05\u0005\u008d\u0000\u0000"+
		"\u0a05\u0a06\u0005\u0002\u0000\u0000\u0a06\u0a0d\u0003\u0088D\u0000\u0a07"+
		"\u0a08\u0005\u00e7\u0000\u0000\u0a08\u0a0b\u0003\u00aeW\u0000\u0a09\u0a0a"+
		"\u0005h\u0000\u0000\u0a0a\u0a0c\u0003\u008cF\u0000\u0a0b\u0a09\u0001\u0000"+
		"\u0000\u0000\u0a0b\u0a0c\u0001\u0000\u0000\u0000\u0a0c\u0a0e\u0001\u0000"+
		"\u0000\u0000\u0a0d\u0a07\u0001\u0000\u0000\u0000\u0a0d\u0a0e\u0001\u0000"+
		"\u0000\u0000\u0a0e\u0a12\u0001\u0000\u0000\u0000\u0a0f\u0a10\u0003\u0094"+
		"J\u0000\u0a10\u0a11\u0005\u0134\u0000\u0000\u0a11\u0a13\u0001\u0000\u0000"+
		"\u0000\u0a12\u0a0f\u0001\u0000\u0000\u0000\u0a12\u0a13\u0001\u0000\u0000"+
		"\u0000\u0a13\u0a1b\u0001\u0000\u0000\u0000\u0a14\u0a15\u0007\u000f\u0000"+
		"\u0000\u0a15\u0a19\u0005\u00da\u0000\u0000\u0a16\u0a17\u0005\u00be\u0000"+
		"\u0000\u0a17\u0a18\u0005\u00f2\u0000\u0000\u0a18\u0a1a\u0005\u0108\u0000"+
		"\u0000\u0a19\u0a16\u0001\u0000\u0000\u0000\u0a19\u0a1a\u0001\u0000\u0000"+
		"\u0000\u0a1a\u0a1c\u0001\u0000\u0000\u0000\u0a1b\u0a14\u0001\u0000\u0000"+
		"\u0000\u0a1b\u0a1c\u0001\u0000\u0000\u0000\u0a1c\u0a21\u0001\u0000\u0000"+
		"\u0000\u0a1d\u0a1e\u0003\u0096K\u0000\u0a1e\u0a1f\u0005\u00be\u0000\u0000"+
		"\u0a1f\u0a20\u0005U\u0000\u0000\u0a20\u0a22\u0001\u0000\u0000\u0000\u0a21"+
		"\u0a1d\u0001\u0000\u0000\u0000\u0a21\u0a22\u0001\u0000\u0000\u0000\u0a22"+
		"\u0a27\u0001\u0000\u0000\u0000\u0a23\u0a24\u0003\u0096K\u0000\u0a24\u0a25"+
		"\u0005\u00be\u0000\u0000\u0a25\u0a26\u0005Y\u0000\u0000\u0a26\u0a28\u0001"+
		"\u0000\u0000\u0000\u0a27\u0a23\u0001\u0000\u0000\u0000\u0a27\u0a28\u0001"+
		"\u0000\u0000\u0000\u0a28\u0a29\u0001\u0000\u0000\u0000\u0a29\u0a2a\u0005"+
		"\u0003\u0000\u0000\u0a2a\u0a72\u0001\u0000\u0000\u0000\u0a2b\u0a2c\u0005"+
		"\u008c\u0000\u0000\u0a2c\u0a49\u0005\u0002\u0000\u0000\u0a2d\u0a32\u0003"+
		"\u0098L\u0000\u0a2e\u0a2f\u0005\u0004\u0000\u0000\u0a2f\u0a31\u0003\u0098"+
		"L\u0000\u0a30\u0a2e\u0001\u0000\u0000\u0000\u0a31\u0a34\u0001\u0000\u0000"+
		"\u0000\u0a32\u0a30\u0001\u0000\u0000\u0000\u0a32\u0a33\u0001\u0000\u0000"+
		"\u0000\u0a33\u0a3b\u0001\u0000\u0000\u0000\u0a34\u0a32\u0001\u0000\u0000"+
		"\u0000\u0a35\u0a36\u0005\u00b7\u0000\u0000\u0a36\u0a37\u0005\u00be\u0000"+
		"\u0000\u0a37\u0a3c\u0005\u00b7\u0000\u0000\u0a38\u0a39\u0005\u0012\u0000"+
		"\u0000\u0a39\u0a3a\u0005\u00be\u0000\u0000\u0a3a\u0a3c\u0005\u00b7\u0000"+
		"\u0000\u0a3b\u0a35\u0001\u0000\u0000\u0000\u0a3b\u0a38\u0001\u0000\u0000"+
		"\u0000\u0a3b\u0a3c\u0001\u0000\u0000\u0000\u0a3c\u0a47\u0001\u0000\u0000"+
		"\u0000\u0a3d\u0a3e\u0005\u0130\u0000\u0000\u0a3e\u0a40\u0005\u011a\u0000"+
		"\u0000\u0a3f\u0a41\u0005\u0092\u0000\u0000\u0a40\u0a3f\u0001\u0000\u0000"+
		"\u0000\u0a40\u0a41\u0001\u0000\u0000\u0000\u0a41\u0a48\u0001\u0000\u0000"+
		"\u0000\u0a42\u0a43\u0005\u0132\u0000\u0000\u0a43\u0a45\u0005\u011a\u0000"+
		"\u0000\u0a44\u0a46\u0005\u0092\u0000\u0000\u0a45\u0a44\u0001\u0000\u0000"+
		"\u0000\u0a45\u0a46\u0001\u0000\u0000\u0000\u0a46\u0a48\u0001\u0000\u0000"+
		"\u0000\u0a47\u0a3d\u0001\u0000\u0000\u0000\u0a47\u0a42\u0001\u0000\u0000"+
		"\u0000\u0a47\u0a48\u0001\u0000\u0000\u0000\u0a48\u0a4a\u0001\u0000\u0000"+
		"\u0000\u0a49\u0a2d\u0001\u0000\u0000\u0000\u0a49\u0a4a\u0001\u0000\u0000"+
		"\u0000\u0a4a\u0a51\u0001\u0000\u0000\u0000\u0a4b\u0a4c\u0005\u00e7\u0000"+
		"\u0000\u0a4c\u0a4f\u0003\u00aeW\u0000\u0a4d\u0a4e\u0005h\u0000\u0000\u0a4e"+
		"\u0a50\u0003\u008cF\u0000\u0a4f\u0a4d\u0001\u0000\u0000\u0000\u0a4f\u0a50"+
		"\u0001\u0000\u0000\u0000\u0a50\u0a52\u0001\u0000\u0000\u0000\u0a51\u0a4b"+
		"\u0001\u0000\u0000\u0000\u0a51\u0a52\u0001\u0000\u0000\u0000\u0a52\u0a53"+
		"\u0001\u0000\u0000\u0000\u0a53\u0a72\u0005\u0003\u0000\u0000\u0a54\u0a55"+
		"\u0005\u008a\u0000\u0000\u0a55\u0a66\u0005\u0002\u0000\u0000\u0a56\u0a5b"+
		"\u0003\u008aE\u0000\u0a57\u0a58\u0005\u0004\u0000\u0000\u0a58\u0a5a\u0003"+
		"\u008aE\u0000\u0a59\u0a57\u0001\u0000\u0000\u0000\u0a5a\u0a5d\u0001\u0000"+
		"\u0000\u0000\u0a5b\u0a59\u0001\u0000\u0000\u0000\u0a5b\u0a5c\u0001\u0000"+
		"\u0000\u0000\u0a5c\u0a64\u0001\u0000\u0000\u0000\u0a5d\u0a5b\u0001\u0000"+
		"\u0000\u0000\u0a5e\u0a5f\u0005\u00b7\u0000\u0000\u0a5f\u0a60\u0005\u00be"+
		"\u0000\u0000\u0a60\u0a65\u0005\u00b7\u0000\u0000\u0a61\u0a62\u0005\u0012"+
		"\u0000\u0000\u0a62\u0a63\u0005\u00be\u0000\u0000\u0a63\u0a65\u0005\u00b7"+
		"\u0000\u0000\u0a64\u0a5e\u0001\u0000\u0000\u0000\u0a64\u0a61\u0001\u0000"+
		"\u0000\u0000\u0a64\u0a65\u0001\u0000\u0000\u0000\u0a65\u0a67\u0001\u0000"+
		"\u0000\u0000\u0a66\u0a56\u0001\u0000\u0000\u0000\u0a66\u0a67\u0001\u0000"+
		"\u0000\u0000\u0a67\u0a6e\u0001\u0000\u0000\u0000\u0a68\u0a69\u0005\u00e7"+
		"\u0000\u0000\u0a69\u0a6c\u0003\u00aeW\u0000\u0a6a\u0a6b\u0005h\u0000\u0000"+
		"\u0a6b\u0a6d\u0003\u008cF\u0000\u0a6c\u0a6a\u0001\u0000\u0000\u0000\u0a6c"+
		"\u0a6d\u0001\u0000\u0000\u0000\u0a6d\u0a6f\u0001\u0000\u0000\u0000\u0a6e"+
		"\u0a68\u0001\u0000\u0000\u0000\u0a6e\u0a6f\u0001\u0000\u0000\u0000\u0a6f"+
		"\u0a70\u0001\u0000\u0000\u0000\u0a70\u0a72\u0005\u0003\u0000\u0000\u0a71"+
		"\u08ac\u0001\u0000\u0000\u0000\u0a71\u08ae\u0001\u0000\u0000\u0000\u0a71"+
		"\u08af\u0001\u0000\u0000\u0000\u0a71\u08b2\u0001\u0000\u0000\u0000\u0a71"+
		"\u08b5\u0001\u0000\u0000\u0000\u0a71\u08b6\u0001\u0000\u0000\u0000\u0a71"+
		"\u08b7\u0001\u0000\u0000\u0000\u0a71\u08b8\u0001\u0000\u0000\u0000\u0a71"+
		"\u08b9\u0001\u0000\u0000\u0000\u0a71\u08ba\u0001\u0000\u0000\u0000\u0a71"+
		"\u08c1\u0001\u0000\u0000\u0000\u0a71\u08cb\u0001\u0000\u0000\u0000\u0a71"+
		"\u08d7\u0001\u0000\u0000\u0000\u0a71\u08fa\u0001\u0000\u0000\u0000\u0a71"+
		"\u090c\u0001\u0000\u0000\u0000\u0a71\u0933\u0001\u0000\u0000\u0000\u0a71"+
		"\u0936\u0001\u0000\u0000\u0000\u0a71\u093a\u0001\u0000\u0000\u0000\u0a71"+
		"\u0948\u0001\u0000\u0000\u0000\u0a71\u094c\u0001\u0000\u0000\u0000\u0a71"+
		"\u0951\u0001\u0000\u0000\u0000\u0a71\u095e\u0001\u0000\u0000\u0000\u0a71"+
		"\u096a\u0001\u0000\u0000\u0000\u0a71\u0971\u0001\u0000\u0000\u0000\u0a71"+
		"\u0978\u0001\u0000\u0000\u0000\u0a71\u0985\u0001\u0000\u0000\u0000\u0a71"+
		"\u0986\u0001\u0000\u0000\u0000\u0a71\u0987\u0001\u0000\u0000\u0000\u0a71"+
		"\u098d\u0001\u0000\u0000\u0000\u0a71\u0993\u0001\u0000\u0000\u0000\u0a71"+
		"\u0999\u0001\u0000\u0000\u0000\u0a71\u099f\u0001\u0000\u0000\u0000\u0a71"+
		"\u09a0\u0001\u0000\u0000\u0000\u0a71\u09a1\u0001\u0000\u0000\u0000\u0a71"+
		"\u09a2\u0001\u0000\u0000\u0000\u0a71\u09a3\u0001\u0000\u0000\u0000\u0a71"+
		"\u09b1\u0001\u0000\u0000\u0000\u0a71\u09b8\u0001\u0000\u0000\u0000\u0a71"+
		"\u09c3\u0001\u0000\u0000\u0000\u0a71\u09cc\u0001\u0000\u0000\u0000\u0a71"+
		"\u09d3\u0001\u0000\u0000\u0000\u0a71\u09d7\u0001\u0000\u0000\u0000\u0a71"+
		"\u09e4\u0001\u0000\u0000\u0000\u0a71\u09ef\u0001\u0000\u0000\u0000\u0a71"+
		"\u0a04\u0001\u0000\u0000\u0000\u0a71\u0a2b\u0001\u0000\u0000\u0000\u0a71"+
		"\u0a54\u0001\u0000\u0000\u0000\u0a72\u0a7d\u0001\u0000\u0000\u0000\u0a73"+
		"\u0a74\n\u0018\u0000\u0000\u0a74\u0a75\u0005\b\u0000\u0000\u0a75\u0a76"+
		"\u0003\u0084B\u0000\u0a76\u0a77\u0005\t\u0000\u0000\u0a77\u0a7c\u0001"+
		"\u0000\u0000\u0000\u0a78\u0a79\n\u0016\u0000\u0000\u0a79\u0a7a\u0005\u0001"+
		"\u0000\u0000\u0a7a\u0a7c\u0003\u0100\u0080\u0000\u0a7b\u0a73\u0001\u0000"+
		"\u0000\u0000\u0a7b\u0a78\u0001\u0000\u0000\u0000\u0a7c\u0a7f\u0001\u0000"+
		"\u0000\u0000\u0a7d\u0a7b\u0001\u0000\u0000\u0000\u0a7d\u0a7e\u0001\u0000"+
		"\u0000\u0000\u0a7e\u0087\u0001\u0000\u0000\u0000\u0a7f\u0a7d\u0001\u0000"+
		"\u0000\u0000\u0a80\u0a81\u0003\u008aE\u0000\u0a81\u0a82\u0005\u0004\u0000"+
		"\u0000\u0a82\u0a85\u0003\u009eO\u0000\u0a83\u0a84\u0005\u001c\u0000\u0000"+
		"\u0a84\u0a86\u0003\u0100\u0080\u0000\u0a85\u0a83\u0001\u0000\u0000\u0000"+
		"\u0a85\u0a86\u0001\u0000\u0000\u0000\u0a86\u0a90\u0001\u0000\u0000\u0000"+
		"\u0a87\u0a88\u0005\u00cb\u0000\u0000\u0a88\u0a8d\u0003\u008eG\u0000\u0a89"+
		"\u0a8a\u0005\u0004\u0000\u0000\u0a8a\u0a8c\u0003\u008eG\u0000\u0a8b\u0a89"+
		"\u0001\u0000\u0000\u0000\u0a8c\u0a8f\u0001\u0000\u0000\u0000\u0a8d\u0a8b"+
		"\u0001\u0000\u0000\u0000\u0a8d\u0a8e\u0001\u0000\u0000\u0000\u0a8e\u0a91"+
		"\u0001\u0000\u0000\u0000\u0a8f\u0a8d\u0001\u0000\u0000\u0000\u0a90\u0a87"+
		"\u0001\u0000\u0000\u0000\u0a90\u0a91\u0001\u0000\u0000\u0000\u0a91\u0089"+
		"\u0001\u0000\u0000\u0000\u0a92\u0a95\u0003~?\u0000\u0a93\u0a94\u0005h"+
		"\u0000\u0000\u0a94\u0a96\u0003\u008cF\u0000\u0a95\u0a93\u0001\u0000\u0000"+
		"\u0000\u0a95\u0a96\u0001\u0000\u0000\u0000\u0a96\u008b\u0001\u0000\u0000"+
		"\u0000\u0a97\u0a9a\u0005\u0089\u0000\u0000\u0a98\u0a99\u0005W\u0000\u0000"+
		"\u0a99\u0a9b\u0007\u0014\u0000\u0000\u0a9a\u0a98\u0001\u0000\u0000\u0000"+
		"\u0a9a\u0a9b\u0001\u0000\u0000\u0000\u0a9b\u008d\u0001\u0000\u0000\u0000"+
		"\u0a9c\u0a9d\u0003\u008aE\u0000\u0a9d\u0a9e\u0005\u001c\u0000\u0000\u0a9e"+
		"\u0a9f\u0003\u0100\u0080\u0000\u0a9f\u008f\u0001\u0000\u0000\u0000\u0aa0"+
		"\u0aa1\u0007\u0015\u0000\u0000\u0aa1\u0091\u0001\u0000\u0000\u0000\u0aa2"+
		"\u0aa7\u0005Y\u0000\u0000\u0aa3\u0aa7\u0005\u00b7\u0000\u0000\u0aa4\u0aa5"+
		"\u0005F\u0000\u0000\u0aa5\u0aa7\u0003~?\u0000\u0aa6\u0aa2\u0001\u0000"+
		"\u0000\u0000\u0aa6\u0aa3\u0001\u0000\u0000\u0000\u0aa6\u0aa4\u0001\u0000"+
		"\u0000\u0000\u0aa7\u0093\u0001\u0000\u0000\u0000\u0aa8\u0aaa\u0005\u0132"+
		"\u0000\u0000\u0aa9\u0aab\u0005\u001b\u0000\u0000\u0aaa\u0aa9\u0001\u0000"+
		"\u0000\u0000\u0aaa\u0aab\u0001\u0000\u0000\u0000\u0aab\u0ab4\u0001\u0000"+
		"\u0000\u0000\u0aac\u0aae\u0005\u0130\u0000\u0000\u0aad\u0aaf\u0007\u0016"+
		"\u0000\u0000\u0aae\u0aad\u0001\u0000\u0000\u0000\u0aae\u0aaf\u0001\u0000"+
		"\u0000\u0000\u0aaf\u0ab1\u0001\u0000\u0000\u0000\u0ab0\u0ab2\u0005\u001b"+
		"\u0000\u0000\u0ab1\u0ab0\u0001\u0000\u0000\u0000\u0ab1\u0ab2\u0001\u0000"+
		"\u0000\u0000\u0ab2\u0ab4\u0001\u0000\u0000\u0000\u0ab3\u0aa8\u0001\u0000"+
		"\u0000\u0000\u0ab3\u0aac\u0001\u0000\u0000\u0000\u0ab4\u0095\u0001\u0000"+
		"\u0000\u0000\u0ab5\u0abc\u0005Y\u0000\u0000\u0ab6\u0abc\u0005\u00b7\u0000"+
		"\u0000\u0ab7\u0ab8\u0005U\u0000\u0000\u0ab8\u0abc\u0005\u001b\u0000\u0000"+
		"\u0ab9\u0aba\u0005U\u0000\u0000\u0aba\u0abc\u0005\u00ba\u0000\u0000\u0abb"+
		"\u0ab5\u0001\u0000\u0000\u0000\u0abb\u0ab6\u0001\u0000\u0000\u0000\u0abb"+
		"\u0ab7\u0001\u0000\u0000\u0000\u0abb\u0ab9\u0001\u0000\u0000\u0000\u0abc"+
		"\u0097\u0001\u0000\u0000\u0000\u0abd\u0abf\u0005\u0091\u0000\u0000\u0abe"+
		"\u0abd\u0001\u0000\u0000\u0000\u0abe\u0abf\u0001\u0000\u0000\u0000\u0abf"+
		"\u0ac0\u0001\u0000\u0000\u0000\u0ac0\u0ac1\u0003~?\u0000\u0ac1\u0ac2\u0005"+
		"\u0127\u0000\u0000\u0ac2\u0ac3\u0003\u008aE\u0000\u0ac3\u0ac9\u0001\u0000"+
		"\u0000\u0000\u0ac4\u0ac5\u0003~?\u0000\u0ac5\u0ac6\u0005\n\u0000\u0000"+
		"\u0ac6\u0ac7\u0003\u008aE\u0000\u0ac7\u0ac9\u0001\u0000\u0000\u0000\u0ac8"+
		"\u0abe\u0001\u0000\u0000\u0000\u0ac8\u0ac4\u0001\u0000\u0000\u0000\u0ac9"+
		"\u0099\u0001\u0000\u0000\u0000\u0aca\u0acb\u0007\u0017\u0000\u0000\u0acb"+
		"\u009b\u0001\u0000\u0000\u0000\u0acc\u0acd\u0005x\u0000\u0000\u0acd\u0ad1"+
		"\u0005\u00b9\u0000\u0000\u0ace\u0acf\u0005\u00e4\u0000\u0000\u0acf\u0ad1"+
		"\u0005\u00b9\u0000\u0000\u0ad0\u0acc\u0001\u0000\u0000\u0000\u0ad0\u0ace"+
		"\u0001\u0000\u0000\u0000\u0ad1\u009d\u0001\u0000\u0000\u0000\u0ad2\u0ad9"+
		"\u0005\u0146\u0000\u0000\u0ad3\u0ad6\u0005\u0147\u0000\u0000\u0ad4\u0ad5"+
		"\u0005\u0115\u0000\u0000\u0ad5\u0ad7\u0005\u0146\u0000\u0000\u0ad6\u0ad4"+
		"\u0001\u0000\u0000\u0000\u0ad6\u0ad7\u0001\u0000\u0000\u0000\u0ad7\u0ad9"+
		"\u0001\u0000\u0000\u0000\u0ad8\u0ad2\u0001\u0000\u0000\u0000\u0ad8\u0ad3"+
		"\u0001\u0000\u0000\u0000\u0ad9\u009f\u0001\u0000\u0000\u0000\u0ada\u0adb"+
		"\u0005\u010b\u0000\u0000\u0adb\u0adc\u0005\u0137\u0000\u0000\u0adc\u0ae1"+
		"\u0003\u00a8T\u0000\u0add\u0ade\u0005\u010b\u0000\u0000\u0ade\u0adf\u0005"+
		"\u0137\u0000\u0000\u0adf\u0ae1\u0003\u009eO\u0000\u0ae0\u0ada\u0001\u0000"+
		"\u0000\u0000\u0ae0\u0add\u0001\u0000\u0000\u0000\u0ae1\u00a1\u0001\u0000"+
		"\u0000\u0000\u0ae2\u0ae3\u0007\u0018\u0000\u0000\u0ae3\u00a3\u0001\u0000"+
		"\u0000\u0000\u0ae4\u0ae5\u0007\u0019\u0000\u0000\u0ae5\u00a5\u0001\u0000"+
		"\u0000\u0000\u0ae6\u0ae7\u0007\u001a\u0000\u0000\u0ae7\u00a7\u0001\u0000"+
		"\u0000\u0000\u0ae8\u0aea\u0005\u0081\u0000\u0000\u0ae9\u0aeb\u0007\u0012"+
		"\u0000\u0000\u0aea\u0ae9\u0001\u0000\u0000\u0000\u0aea\u0aeb\u0001\u0000"+
		"\u0000\u0000\u0aeb\u0aec\u0001\u0000\u0000\u0000\u0aec\u0aed\u0003\u009e"+
		"O\u0000\u0aed\u0af0\u0003\u00aaU\u0000\u0aee\u0aef\u0005\u010d\u0000\u0000"+
		"\u0aef\u0af1\u0003\u00aaU\u0000\u0af0\u0aee\u0001\u0000\u0000\u0000\u0af0"+
		"\u0af1\u0001\u0000\u0000\u0000\u0af1\u00a9\u0001\u0000\u0000\u0000\u0af2"+
		"\u0af3\u0007\u001b\u0000\u0000\u0af3\u00ab\u0001\u0000\u0000\u0000\u0af4"+
		"\u0af5\u0007\u001c\u0000\u0000\u0af5\u00ad\u0001\u0000\u0000\u0000\u0af6"+
		"\u0af7\u0006W\uffff\uffff\u0000\u0af7\u0af8\u0005\u00ef\u0000\u0000\u0af8"+
		"\u0af9\u0005\u0002\u0000\u0000\u0af9\u0afe\u0003\u00b0X\u0000\u0afa\u0afb"+
		"\u0005\u0004\u0000\u0000\u0afb\u0afd\u0003\u00b0X\u0000\u0afc\u0afa\u0001"+
		"\u0000\u0000\u0000\u0afd\u0b00\u0001\u0000\u0000\u0000\u0afe\u0afc\u0001"+
		"\u0000\u0000\u0000\u0afe\u0aff\u0001\u0000\u0000\u0000\u0aff\u0b01\u0001"+
		"\u0000\u0000\u0000\u0b00\u0afe\u0001\u0000\u0000\u0000\u0b01\u0b02\u0005"+
		"\u0003\u0000\u0000\u0b02\u0b52\u0001\u0000\u0000\u0000\u0b03\u0b04\u0005"+
		"\u0081\u0000\u0000\u0b04\u0b07\u0003\u00aaU\u0000\u0b05\u0b06\u0005\u010d"+
		"\u0000\u0000\u0b06\u0b08\u0003\u00aaU\u0000\u0b07\u0b05\u0001\u0000\u0000"+
		"\u0000\u0b07\u0b08\u0001\u0000\u0000\u0000\u0b08\u0b52\u0001\u0000\u0000"+
		"\u0000\u0b09\u0b0e\u0005\u010c\u0000\u0000\u0b0a\u0b0b\u0005\u0002\u0000"+
		"\u0000\u0b0b\u0b0c\u0003\u00b2Y\u0000\u0b0c\u0b0d\u0005\u0003\u0000\u0000"+
		"\u0b0d\u0b0f\u0001\u0000\u0000\u0000\u0b0e\u0b0a\u0001\u0000\u0000\u0000"+
		"\u0b0e\u0b0f\u0001\u0000\u0000\u0000\u0b0f\u0b13\u0001\u0000\u0000\u0000"+
		"\u0b10\u0b11\u0005\u0132\u0000\u0000\u0b11\u0b12\u0005\u010b\u0000\u0000"+
		"\u0b12\u0b14\u0005\u0137\u0000\u0000\u0b13\u0b10\u0001\u0000\u0000\u0000"+
		"\u0b13\u0b14\u0001\u0000\u0000\u0000\u0b14\u0b52\u0001\u0000\u0000\u0000"+
		"\u0b15\u0b1a\u0005\u010c\u0000\u0000\u0b16\u0b17\u0005\u0002\u0000\u0000"+
		"\u0b17\u0b18\u0003\u00b2Y\u0000\u0b18\u0b19\u0005\u0003\u0000\u0000\u0b19"+
		"\u0b1b\u0001\u0000\u0000\u0000\u0b1a\u0b16\u0001\u0000\u0000\u0000\u0b1a"+
		"\u0b1b\u0001\u0000\u0000\u0000\u0b1b\u0b1c\u0001\u0000\u0000\u0000\u0b1c"+
		"\u0b1d\u0005\u0130\u0000\u0000\u0b1d\u0b1e\u0005\u010b\u0000\u0000\u0b1e"+
		"\u0b52\u0005\u0137\u0000\u0000\u0b1f\u0b24\u0005\u010b\u0000\u0000\u0b20"+
		"\u0b21\u0005\u0002\u0000\u0000\u0b21\u0b22\u0003\u00b2Y\u0000\u0b22\u0b23"+
		"\u0005\u0003\u0000\u0000\u0b23\u0b25\u0001\u0000\u0000\u0000\u0b24\u0b20"+
		"\u0001\u0000\u0000\u0000\u0b24\u0b25\u0001\u0000\u0000\u0000\u0b25\u0b29"+
		"\u0001\u0000\u0000\u0000\u0b26\u0b27\u0005\u0132\u0000\u0000\u0b27\u0b28"+
		"\u0005\u010b\u0000\u0000\u0b28\u0b2a\u0005\u0137\u0000\u0000\u0b29\u0b26"+
		"\u0001\u0000\u0000\u0000\u0b29\u0b2a\u0001\u0000\u0000\u0000\u0b2a\u0b52"+
		"\u0001\u0000\u0000\u0000\u0b2b\u0b30\u0005\u010b\u0000\u0000\u0b2c\u0b2d"+
		"\u0005\u0002\u0000\u0000\u0b2d\u0b2e\u0003\u00b2Y\u0000\u0b2e\u0b2f\u0005"+
		"\u0003\u0000\u0000\u0b2f\u0b31\u0001\u0000\u0000\u0000\u0b30\u0b2c\u0001"+
		"\u0000\u0000\u0000\u0b30\u0b31\u0001\u0000\u0000\u0000\u0b31\u0b32\u0001"+
		"\u0000\u0000\u0000\u0b32\u0b33\u0005\u0130\u0000\u0000\u0b33\u0b34\u0005"+
		"\u010b\u0000\u0000\u0b34\u0b52\u0005\u0137\u0000\u0000\u0b35\u0b36\u0005"+
		"R\u0000\u0000\u0b36\u0b52\u0005\u00d5\u0000\u0000\u0b37\u0b38\u0005\u001b"+
		"\u0000\u0000\u0b38\u0b39\u0005\u013a\u0000\u0000\u0b39\u0b3a\u0003\u00ae"+
		"W\u0000\u0b3a\u0b3b\u0005\u013c\u0000\u0000\u0b3b\u0b52\u0001\u0000\u0000"+
		"\u0000\u0b3c\u0b3d\u0005\u00a2\u0000\u0000\u0b3d\u0b3e\u0005\u013a\u0000"+
		"\u0000\u0b3e\u0b3f\u0003\u00aeW\u0000\u0b3f\u0b40\u0005\u0004\u0000\u0000"+
		"\u0b40\u0b41\u0003\u00aeW\u0000\u0b41\u0b42\u0005\u013c\u0000\u0000\u0b42"+
		"\u0b52\u0001\u0000\u0000\u0000\u0b43\u0b4f\u0003\u0100\u0080\u0000\u0b44"+
		"\u0b45\u0005\u0002\u0000\u0000\u0b45\u0b4a\u0003\u00b2Y\u0000\u0b46\u0b47"+
		"\u0005\u0004\u0000\u0000\u0b47\u0b49\u0003\u00b2Y\u0000\u0b48\u0b46\u0001"+
		"\u0000\u0000\u0000\u0b49\u0b4c\u0001\u0000\u0000\u0000\u0b4a\u0b48\u0001"+
		"\u0000\u0000\u0000\u0b4a\u0b4b\u0001\u0000\u0000\u0000\u0b4b\u0b4d\u0001"+
		"\u0000\u0000\u0000\u0b4c\u0b4a\u0001\u0000\u0000\u0000\u0b4d\u0b4e\u0005"+
		"\u0003\u0000\u0000\u0b4e\u0b50\u0001\u0000\u0000\u0000\u0b4f\u0b44\u0001"+
		"\u0000\u0000\u0000\u0b4f\u0b50\u0001\u0000\u0000\u0000\u0b50\u0b52\u0001"+
		"\u0000\u0000\u0000\u0b51\u0af6\u0001\u0000\u0000\u0000\u0b51\u0b03\u0001"+
		"\u0000\u0000\u0000\u0b51\u0b09\u0001\u0000\u0000\u0000\u0b51\u0b15\u0001"+
		"\u0000\u0000\u0000\u0b51\u0b1f\u0001\u0000\u0000\u0000\u0b51\u0b2b\u0001"+
		"\u0000\u0000\u0000\u0b51\u0b35\u0001\u0000\u0000\u0000\u0b51\u0b37\u0001"+
		"\u0000\u0000\u0000\u0b51\u0b3c\u0001\u0000\u0000\u0000\u0b51\u0b43\u0001"+
		"\u0000\u0000\u0000\u0b52\u0b5c\u0001\u0000\u0000\u0000\u0b53\u0b54\n\u0002"+
		"\u0000\u0000\u0b54\u0b58\u0005\u001b\u0000\u0000\u0b55\u0b56\u0005\b\u0000"+
		"\u0000\u0b56\u0b57\u0005\u0149\u0000\u0000\u0b57\u0b59\u0005\t\u0000\u0000"+
		"\u0b58\u0b55\u0001\u0000\u0000\u0000\u0b58\u0b59\u0001\u0000\u0000\u0000"+
		"\u0b59\u0b5b\u0001\u0000\u0000\u0000\u0b5a\u0b53\u0001\u0000\u0000\u0000"+
		"\u0b5b\u0b5e\u0001\u0000\u0000\u0000\u0b5c\u0b5a\u0001\u0000\u0000\u0000"+
		"\u0b5c\u0b5d\u0001\u0000\u0000\u0000\u0b5d\u00af\u0001\u0000\u0000\u0000"+
		"\u0b5e\u0b5c\u0001\u0000\u0000\u0000\u0b5f\u0b64\u0003\u00aeW\u0000\u0b60"+
		"\u0b61\u0003\u0100\u0080\u0000\u0b61\u0b62\u0003\u00aeW\u0000\u0b62\u0b64"+
		"\u0001\u0000\u0000\u0000\u0b63\u0b5f\u0001\u0000\u0000\u0000\u0b63\u0b60"+
		"\u0001\u0000\u0000\u0000\u0b64\u00b1\u0001\u0000\u0000\u0000\u0b65\u0b68"+
		"\u0005\u0149\u0000\u0000\u0b66\u0b68\u0003\u00aeW\u0000\u0b67\u0b65\u0001"+
		"\u0000\u0000\u0000\u0b67\u0b66\u0001\u0000\u0000\u0000\u0b68\u00b3\u0001"+
		"\u0000\u0000\u0000\u0b69\u0b6a\u0005\u012c\u0000\u0000\u0b6a\u0b6b\u0003"+
		"~?\u0000\u0b6b\u0b6c\u0005\u0109\u0000\u0000\u0b6c\u0b6d\u0003~?\u0000"+
		"\u0b6d\u00b5\u0001\u0000\u0000\u0000\u0b6e\u0b6f\u0005c\u0000\u0000\u0b6f"+
		"\u0b70\u0005\u0002\u0000\u0000\u0b70\u0b71\u0005\u012d\u0000\u0000\u0b71"+
		"\u0b72\u0003\u0080@\u0000\u0b72\u0b73\u0005\u0003\u0000\u0000\u0b73\u00b7"+
		"\u0001\u0000\u0000\u0000\u0b74\u0b75\u0005\u012c\u0000\u0000\u0b75\u0b78"+
		"\u0005\u00a4\u0000\u0000\u0b76\u0b77\u0005\u0019\u0000\u0000\u0b77\u0b79"+
		"\u0003~?\u0000\u0b78\u0b76\u0001\u0000\u0000\u0000\u0b78\u0b79\u0001\u0000"+
		"\u0000\u0000\u0b79\u0b7a\u0001\u0000\u0000\u0000\u0b7a\u0b7b\u0005\u0109"+
		"\u0000\u0000\u0b7b\u0b7c\u0005\u011f\u0000\u0000\u0b7c\u0b7d\u0005\u00fb"+
		"\u0000\u0000\u0b7d\u0b7e\u0003\u0100\u0080\u0000\u0b7e\u0b7f\u0005\u0138"+
		"\u0000\u0000\u0b7f\u0b87\u0003~?\u0000\u0b80\u0b81\u0005\u0004\u0000\u0000"+
		"\u0b81\u0b82\u0003\u0100\u0080\u0000\u0b82\u0b83\u0005\u0138\u0000\u0000"+
		"\u0b83\u0b84\u0003~?\u0000\u0b84\u0b86\u0001\u0000\u0000\u0000\u0b85\u0b80"+
		"\u0001\u0000\u0000\u0000\u0b86\u0b89\u0001\u0000\u0000\u0000\u0b87\u0b85"+
		"\u0001\u0000\u0000\u0000\u0b87\u0b88\u0001\u0000\u0000\u0000\u0b88\u0bb5"+
		"\u0001\u0000\u0000\u0000\u0b89\u0b87\u0001\u0000\u0000\u0000\u0b8a\u0b8b"+
		"\u0005\u012c\u0000\u0000\u0b8b\u0b8e\u0005\u00a4\u0000\u0000\u0b8c\u0b8d"+
		"\u0005\u0019\u0000\u0000\u0b8d\u0b8f\u0003~?\u0000\u0b8e\u0b8c\u0001\u0000"+
		"\u0000\u0000\u0b8e\u0b8f\u0001\u0000\u0000\u0000\u0b8f\u0b90\u0001\u0000"+
		"\u0000\u0000\u0b90\u0b91\u0005\u0109\u0000\u0000\u0b91\u0bb5\u0005I\u0000"+
		"\u0000\u0b92\u0b93\u0005\u012c\u0000\u0000\u0b93\u0b94\u0005\u00b6\u0000"+
		"\u0000\u0b94\u0b97\u0005\u00a4\u0000\u0000\u0b95\u0b96\u0005\u0019\u0000"+
		"\u0000\u0b96\u0b98\u0003~?\u0000\u0b97\u0b95\u0001\u0000\u0000\u0000\u0b97"+
		"\u0b98\u0001\u0000\u0000\u0000\u0b98\u0b99\u0001\u0000\u0000\u0000\u0b99"+
		"\u0b9a\u0005\u0109\u0000\u0000\u0b9a\u0ba6\u0005\u007f\u0000\u0000\u0b9b"+
		"\u0b9c\u0005\u0002\u0000\u0000\u0b9c\u0ba1\u0003\u0100\u0080\u0000\u0b9d"+
		"\u0b9e\u0005\u0004\u0000\u0000\u0b9e\u0ba0\u0003\u0100\u0080\u0000\u0b9f"+
		"\u0b9d\u0001\u0000\u0000\u0000\u0ba0\u0ba3\u0001\u0000\u0000\u0000\u0ba1"+
		"\u0b9f\u0001\u0000\u0000\u0000\u0ba1\u0ba2\u0001\u0000\u0000\u0000\u0ba2"+
		"\u0ba4\u0001\u0000\u0000\u0000\u0ba3\u0ba1\u0001\u0000\u0000\u0000\u0ba4"+
		"\u0ba5\u0005\u0003\u0000\u0000\u0ba5\u0ba7\u0001\u0000\u0000\u0000\u0ba6"+
		"\u0b9b\u0001\u0000\u0000\u0000\u0ba6\u0ba7\u0001\u0000\u0000\u0000\u0ba7"+
		"\u0ba8\u0001\u0000\u0000\u0000\u0ba8\u0ba9\u0005\u0128\u0000\u0000\u0ba9"+
		"\u0baa\u0005\u0002\u0000\u0000\u0baa\u0baf\u0003~?\u0000\u0bab\u0bac\u0005"+
		"\u0004\u0000\u0000\u0bac\u0bae\u0003~?\u0000\u0bad\u0bab\u0001\u0000\u0000"+
		"\u0000\u0bae\u0bb1\u0001\u0000\u0000\u0000\u0baf\u0bad\u0001\u0000\u0000"+
		"\u0000\u0baf\u0bb0\u0001\u0000\u0000\u0000\u0bb0\u0bb2\u0001\u0000\u0000"+
		"\u0000\u0bb1\u0baf\u0001\u0000\u0000\u0000\u0bb2\u0bb3\u0005\u0003\u0000"+
		"\u0000\u0bb3\u0bb5\u0001\u0000\u0000\u0000\u0bb4\u0b74\u0001\u0000\u0000"+
		"\u0000\u0bb4\u0b8a\u0001\u0000\u0000\u0000\u0bb4\u0b92\u0001\u0000\u0000"+
		"\u0000\u0bb5\u00b9\u0001\u0000\u0000\u0000\u0bb6\u0bbc\u0005\u00c7\u0000"+
		"\u0000\u0bb7\u0bbd\u0003\u0100\u0080\u0000\u0bb8\u0bb9\u0005\u0002\u0000"+
		"\u0000\u0bb9\u0bba\u0003:\u001d\u0000\u0bba\u0bbb\u0005\u0003\u0000\u0000"+
		"\u0bbb\u0bbd\u0001\u0000\u0000\u0000\u0bbc\u0bb7\u0001\u0000\u0000\u0000"+
		"\u0bbc\u0bb8\u0001\u0000\u0000\u0000\u0bbd\u00bb\u0001\u0000\u0000\u0000"+
		"\u0bbe\u0bbf\u0005\u00a8\u0000\u0000\u0bbf\u0bc4\u0003T*\u0000\u0bc0\u0bc1"+
		"\u0005\u0004\u0000\u0000\u0bc1\u0bc3\u0003T*\u0000\u0bc2\u0bc0\u0001\u0000"+
		"\u0000\u0000\u0bc3\u0bc6\u0001\u0000\u0000\u0000\u0bc4\u0bc2\u0001\u0000"+
		"\u0000\u0000\u0bc4\u0bc5\u0001\u0000\u0000\u0000\u0bc5\u0bc8\u0001\u0000"+
		"\u0000\u0000\u0bc6\u0bc4\u0001\u0000\u0000\u0000\u0bc7\u0bbe\u0001\u0000"+
		"\u0000\u0000\u0bc7\u0bc8\u0001\u0000\u0000\u0000\u0bc8\u0bc9\u0001\u0000"+
		"\u0000\u0000\u0bc9\u0bcd\u0003\u00be_\u0000\u0bca\u0bcb\u0005\u0015\u0000"+
		"\u0000\u0bcb\u0bcc\u0005\u00a3\u0000\u0000\u0bcc\u0bce\u0003Z-\u0000\u0bcd"+
		"\u0bca\u0001\u0000\u0000\u0000\u0bcd\u0bce\u0001\u0000\u0000\u0000\u0bce"+
		"\u0bd0\u0001\u0000\u0000\u0000\u0bcf\u0bd1\u0007\r\u0000\u0000\u0bd0\u0bcf"+
		"\u0001\u0000\u0000\u0000\u0bd0\u0bd1\u0001\u0000\u0000\u0000\u0bd1\u0bd7"+
		"\u0001\u0000\u0000\u0000\u0bd2\u0bd3\u0005\u00ce\u0000\u0000\u0bd3\u0bd4"+
		"\u0005\u0002\u0000\u0000\u0bd4\u0bd5\u0003\u00c2a\u0000\u0bd5\u0bd6\u0005"+
		"\u0003\u0000\u0000\u0bd6\u0bd8\u0001\u0000\u0000\u0000\u0bd7\u0bd2\u0001"+
		"\u0000\u0000\u0000\u0bd7\u0bd8\u0001\u0000\u0000\u0000\u0bd8\u0be2\u0001"+
		"\u0000\u0000\u0000\u0bd9\u0bda\u0005\u0101\u0000\u0000\u0bda\u0bdf\u0003"+
		"\\.\u0000\u0bdb\u0bdc\u0005\u0004\u0000\u0000\u0bdc\u0bde\u0003\\.\u0000"+
		"\u0bdd\u0bdb\u0001\u0000\u0000\u0000\u0bde\u0be1\u0001\u0000\u0000\u0000"+
		"\u0bdf\u0bdd\u0001\u0000\u0000\u0000\u0bdf\u0be0\u0001\u0000\u0000\u0000"+
		"\u0be0\u0be3\u0001\u0000\u0000\u0000\u0be1\u0bdf\u0001\u0000\u0000\u0000"+
		"\u0be2\u0bd9\u0001\u0000\u0000\u0000\u0be2\u0be3\u0001\u0000\u0000\u0000"+
		"\u0be3\u0bed\u0001\u0000\u0000\u0000\u0be4\u0be5\u0005G\u0000\u0000\u0be5"+
		"\u0bea\u0003^/\u0000\u0be6\u0be7\u0005\u0004\u0000\u0000\u0be7\u0be9\u0003"+
		"^/\u0000\u0be8\u0be6\u0001\u0000\u0000\u0000\u0be9\u0bec\u0001\u0000\u0000"+
		"\u0000\u0bea\u0be8\u0001\u0000\u0000\u0000\u0bea\u0beb\u0001\u0000\u0000"+
		"\u0000\u0beb\u0bee\u0001\u0000\u0000\u0000\u0bec\u0bea\u0001\u0000\u0000"+
		"\u0000\u0bed\u0be4\u0001\u0000\u0000\u0000\u0bed\u0bee\u0001\u0000\u0000"+
		"\u0000\u0bee\u00bd\u0001\u0000\u0000\u0000\u0bef\u0bf0\u0005\u00db\u0000"+
		"\u0000\u0bf0\u0c08\u0003\u00c0`\u0000\u0bf1\u0bf2\u0005\u00f0\u0000\u0000"+
		"\u0bf2\u0c08\u0003\u00c0`\u0000\u0bf3\u0bf4\u0005t\u0000\u0000\u0bf4\u0c08"+
		"\u0003\u00c0`\u0000\u0bf5\u0bf6\u0005\u00db\u0000\u0000\u0bf6\u0bf7\u0005"+
		"\"\u0000\u0000\u0bf7\u0bf8\u0003\u00c0`\u0000\u0bf8\u0bf9\u0005\u0019"+
		"\u0000\u0000\u0bf9\u0bfa\u0003\u00c0`\u0000\u0bfa\u0c08\u0001\u0000\u0000"+
		"\u0000\u0bfb\u0bfc\u0005\u00f0\u0000\u0000\u0bfc\u0bfd\u0005\"\u0000\u0000"+
		"\u0bfd\u0bfe\u0003\u00c0`\u0000\u0bfe\u0bff\u0005\u0019\u0000\u0000\u0bff"+
		"\u0c00\u0003\u00c0`\u0000\u0c00\u0c08\u0001\u0000\u0000\u0000\u0c01\u0c02"+
		"\u0005t\u0000\u0000\u0c02\u0c03\u0005\"\u0000\u0000\u0c03\u0c04\u0003"+
		"\u00c0`\u0000\u0c04\u0c05\u0005\u0019\u0000\u0000\u0c05\u0c06\u0003\u00c0"+
		"`\u0000\u0c06\u0c08\u0001\u0000\u0000\u0000\u0c07\u0bef\u0001\u0000\u0000"+
		"\u0000\u0c07\u0bf1\u0001\u0000\u0000\u0000\u0c07\u0bf3\u0001\u0000\u0000"+
		"\u0000\u0c07\u0bf5\u0001\u0000\u0000\u0000\u0c07\u0bfb\u0001\u0000\u0000"+
		"\u0000\u0c07\u0c01\u0001\u0000\u0000\u0000\u0c08\u00bf\u0001\u0000\u0000"+
		"\u0000\u0c09\u0c0a\u0005\u0116\u0000\u0000\u0c0a\u0c13\u0005\u00d4\u0000"+
		"\u0000\u0c0b\u0c0c\u0005\u0116\u0000\u0000\u0c0c\u0c13\u0005f\u0000\u0000"+
		"\u0c0d\u0c0e\u00058\u0000\u0000\u0c0e\u0c13\u0005\u00ef\u0000\u0000\u0c0f"+
		"\u0c10\u0003~?\u0000\u0c10\u0c11\u0007\u001d\u0000\u0000\u0c11\u0c13\u0001"+
		"\u0000\u0000\u0000\u0c12\u0c09\u0001\u0000\u0000\u0000\u0c12\u0c0b\u0001"+
		"\u0000\u0000\u0000\u0c12\u0c0d\u0001\u0000\u0000\u0000\u0c12\u0c0f\u0001"+
		"\u0000\u0000\u0000\u0c13\u00c1\u0001\u0000\u0000\u0000\u0c14\u0c15\u0006"+
		"a\uffff\uffff\u0000\u0c15\u0c17\u0003\u00c4b\u0000\u0c16\u0c18\u0003\u00c6"+
		"c\u0000\u0c17\u0c16\u0001\u0000\u0000\u0000\u0c17\u0c18\u0001\u0000\u0000"+
		"\u0000\u0c18\u0c20\u0001\u0000\u0000\u0000\u0c19\u0c1a\n\u0002\u0000\u0000"+
		"\u0c1a\u0c1f\u0003\u00c2a\u0003\u0c1b\u0c1c\n\u0001\u0000\u0000\u0c1c"+
		"\u0c1d\u0005\u000b\u0000\u0000\u0c1d\u0c1f\u0003\u00c2a\u0002\u0c1e\u0c19"+
		"\u0001\u0000\u0000\u0000\u0c1e\u0c1b\u0001\u0000\u0000\u0000\u0c1f\u0c22"+
		"\u0001\u0000\u0000\u0000\u0c20\u0c1e\u0001\u0000\u0000\u0000\u0c20\u0c21"+
		"\u0001\u0000\u0000\u0000\u0c21\u00c3\u0001\u0000\u0000\u0000\u0c22\u0c20"+
		"\u0001\u0000\u0000\u0000\u0c23\u0c3d\u0003\u0100\u0080\u0000\u0c24\u0c25"+
		"\u0005\u0002\u0000\u0000\u0c25\u0c3d\u0005\u0003\u0000\u0000\u0c26\u0c27"+
		"\u0005\u00d1\u0000\u0000\u0c27\u0c28\u0005\u0002\u0000\u0000\u0c28\u0c2d"+
		"\u0003\u00c2a\u0000\u0c29\u0c2a\u0005\u0004\u0000\u0000\u0c2a\u0c2c\u0003"+
		"\u00c2a\u0000\u0c2b\u0c29\u0001\u0000\u0000\u0000\u0c2c\u0c2f\u0001\u0000"+
		"\u0000\u0000\u0c2d\u0c2b\u0001\u0000\u0000\u0000\u0c2d\u0c2e\u0001\u0000"+
		"\u0000\u0000\u0c2e\u0c30\u0001\u0000\u0000\u0000\u0c2f\u0c2d\u0001\u0000"+
		"\u0000\u0000\u0c30\u0c31\u0005\u0003\u0000\u0000\u0c31\u0c3d\u0001\u0000"+
		"\u0000\u0000\u0c32\u0c33\u0005\u0002\u0000\u0000\u0c33\u0c34\u0003\u00c2"+
		"a\u0000\u0c34\u0c35\u0005\u0003\u0000\u0000\u0c35\u0c3d\u0001\u0000\u0000"+
		"\u0000\u0c36\u0c3d\u0005\f\u0000\u0000\u0c37\u0c3d\u0005\r\u0000\u0000"+
		"\u0c38\u0c39\u0005\u000e\u0000\u0000\u0c39\u0c3a\u0003\u00c2a\u0000\u0c3a"+
		"\u0c3b\u0005\u000f\u0000\u0000\u0c3b\u0c3d\u0001\u0000\u0000\u0000\u0c3c"+
		"\u0c23\u0001\u0000\u0000\u0000\u0c3c\u0c24\u0001\u0000\u0000\u0000\u0c3c"+
		"\u0c26\u0001\u0000\u0000\u0000\u0c3c\u0c32\u0001\u0000\u0000\u0000\u0c3c"+
		"\u0c36\u0001\u0000\u0000\u0000\u0c3c\u0c37\u0001\u0000\u0000\u0000\u0c3c"+
		"\u0c38\u0001\u0000\u0000\u0000\u0c3d\u00c5\u0001\u0000\u0000\u0000\u0c3e"+
		"\u0c40\u0005\u0140\u0000\u0000\u0c3f\u0c41\u0005\u0144\u0000\u0000\u0c40"+
		"\u0c3f\u0001\u0000\u0000\u0000\u0c40\u0c41\u0001\u0000\u0000\u0000\u0c41"+
		"\u0c5d\u0001\u0000\u0000\u0000\u0c42\u0c44\u0005\u013e\u0000\u0000\u0c43"+
		"\u0c45\u0005\u0144\u0000\u0000\u0c44\u0c43\u0001\u0000\u0000\u0000\u0c44"+
		"\u0c45\u0001\u0000\u0000\u0000\u0c45\u0c5d\u0001\u0000\u0000\u0000\u0c46"+
		"\u0c48\u0005\u0144\u0000\u0000\u0c47\u0c49\u0005\u0144\u0000\u0000\u0c48"+
		"\u0c47\u0001\u0000\u0000\u0000\u0c48\u0c49\u0001\u0000\u0000\u0000\u0c49"+
		"\u0c5d\u0001\u0000\u0000\u0000\u0c4a\u0c4b\u0005\u0010\u0000\u0000\u0c4b"+
		"\u0c4c\u0005\u0149\u0000\u0000\u0c4c\u0c4e\u0005\u0011\u0000\u0000\u0c4d"+
		"\u0c4f\u0005\u0144\u0000\u0000\u0c4e\u0c4d\u0001\u0000\u0000\u0000\u0c4e"+
		"\u0c4f\u0001\u0000\u0000\u0000\u0c4f\u0c5d\u0001\u0000\u0000\u0000\u0c50"+
		"\u0c52\u0005\u0010\u0000\u0000\u0c51\u0c53\u0005\u0149\u0000\u0000\u0c52"+
		"\u0c51\u0001\u0000\u0000\u0000\u0c52\u0c53\u0001\u0000\u0000\u0000\u0c53"+
		"\u0c54\u0001\u0000\u0000\u0000\u0c54\u0c56\u0005\u0004\u0000\u0000\u0c55"+
		"\u0c57\u0005\u0149\u0000\u0000\u0c56\u0c55\u0001\u0000\u0000\u0000\u0c56"+
		"\u0c57\u0001\u0000\u0000\u0000\u0c57\u0c58\u0001\u0000\u0000\u0000\u0c58"+
		"\u0c5a\u0005\u0011\u0000\u0000\u0c59\u0c5b\u0005\u0144\u0000\u0000\u0c5a"+
		"\u0c59\u0001\u0000\u0000\u0000\u0c5a\u0c5b\u0001\u0000\u0000\u0000\u0c5b"+
		"\u0c5d\u0001\u0000\u0000\u0000\u0c5c\u0c3e\u0001\u0000\u0000\u0000\u0c5c"+
		"\u0c42\u0001\u0000\u0000\u0000\u0c5c\u0c46\u0001\u0000\u0000\u0000\u0c5c"+
		"\u0c4a\u0001\u0000\u0000\u0000\u0c5c\u0c50\u0001\u0000\u0000\u0000\u0c5d"+
		"\u00c7\u0001\u0000\u0000\u0000\u0c5e\u0c5f\u0003\u0100\u0080\u0000\u0c5f"+
		"\u0c60\u0005\u0138\u0000\u0000\u0c60\u0c61\u0003~?\u0000\u0c61\u00c9\u0001"+
		"\u0000\u0000\u0000\u0c62\u0c63\u0005h\u0000\u0000\u0c63\u0c67\u0007\u001e"+
		"\u0000\u0000\u0c64\u0c65\u0005\u0114\u0000\u0000\u0c65\u0c67\u0007\u001f"+
		"\u0000\u0000\u0c66\u0c62\u0001\u0000\u0000\u0000\u0c66\u0c64\u0001\u0000"+
		"\u0000\u0000\u0c67\u00cb\u0001\u0000\u0000\u0000\u0c68\u0c69\u0005\u0086"+
		"\u0000\u0000\u0c69\u0c6a\u0005\u0099\u0000\u0000\u0c6a\u0c6e\u0003\u00ce"+
		"g\u0000\u0c6b\u0c6c\u0005\u00dc\u0000\u0000\u0c6c\u0c6e\u0007 \u0000\u0000"+
		"\u0c6d\u0c68\u0001\u0000\u0000\u0000\u0c6d\u0c6b\u0001\u0000\u0000\u0000"+
		"\u0c6e\u00cd\u0001\u0000\u0000\u0000\u0c6f\u0c70\u0005\u00dc\u0000\u0000"+
		"\u0c70\u0c77\u0005\u0117\u0000\u0000\u0c71\u0c72\u0005\u00dc\u0000\u0000"+
		"\u0c72\u0c77\u00050\u0000\u0000\u0c73\u0c74\u0005\u00e1\u0000\u0000\u0c74"+
		"\u0c77\u0005\u00dc\u0000\u0000\u0c75\u0c77\u0005\u00f9\u0000\u0000\u0c76"+
		"\u0c6f\u0001\u0000\u0000\u0000\u0c76\u0c71\u0001\u0000\u0000\u0000\u0c76"+
		"\u0c73\u0001\u0000\u0000\u0000\u0c76\u0c75\u0001\u0000\u0000\u0000\u0c77"+
		"\u00cf\u0001\u0000\u0000\u0000\u0c78\u0c7e\u0003~?\u0000\u0c79\u0c7a\u0003"+
		"\u0100\u0080\u0000\u0c7a\u0c7b\u0005\u0006\u0000\u0000\u0c7b\u0c7c\u0003"+
		"~?\u0000\u0c7c\u0c7e\u0001\u0000\u0000\u0000\u0c7d\u0c78\u0001\u0000\u0000"+
		"\u0000\u0c7d\u0c79\u0001\u0000\u0000\u0000\u0c7e\u00d1\u0001\u0000\u0000"+
		"\u0000\u0c7f\u0c80\u0003\u0100\u0080\u0000\u0c80\u0c81\u0005\u0001\u0000"+
		"\u0000\u0c81\u0c82\u0003\u0100\u0080\u0000\u0c82\u0c85\u0001\u0000\u0000"+
		"\u0000\u0c83\u0c85\u0003\u0100\u0080\u0000\u0c84\u0c7f\u0001\u0000\u0000"+
		"\u0000\u0c84\u0c83\u0001\u0000\u0000\u0000\u0c85\u00d3\u0001\u0000\u0000"+
		"\u0000\u0c86\u0c8b\u0003\u00d2i\u0000\u0c87\u0c88\u0005\u0004\u0000\u0000"+
		"\u0c88\u0c8a\u0003\u00d2i\u0000\u0c89\u0c87\u0001\u0000\u0000\u0000\u0c8a"+
		"\u0c8d\u0001\u0000\u0000\u0000\u0c8b\u0c89\u0001\u0000\u0000\u0000\u0c8b"+
		"\u0c8c\u0001\u0000\u0000\u0000\u0c8c\u00d5\u0001\u0000\u0000\u0000\u0c8d"+
		"\u0c8b\u0001\u0000\u0000\u0000\u0c8e\u0c8f\u0005k\u0000\u0000\u0c8f\u0c90"+
		"\u0003\u00d8l\u0000\u0c90\u0c94\u0003\u00dcn\u0000\u0c91\u0c93\u0003\u00de"+
		"o\u0000\u0c92\u0c91\u0001\u0000\u0000\u0000\u0c93\u0c96\u0001\u0000\u0000"+
		"\u0000\u0c94\u0c92\u0001\u0000\u0000\u0000\u0c94\u0c95\u0001\u0000\u0000"+
		"\u0000\u0c95\u0c97\u0001\u0000\u0000\u0000\u0c96\u0c94\u0001\u0000\u0000"+
		"\u0000\u0c97\u0c98\u0003\u00e0p\u0000\u0c98\u00d7\u0001\u0000\u0000\u0000"+
		"\u0c99\u0c9a\u0003\u00f2y\u0000\u0c9a\u0ca3\u0005\u0002\u0000\u0000\u0c9b"+
		"\u0ca0\u0003\u00dam\u0000\u0c9c\u0c9d\u0005\u0004\u0000\u0000\u0c9d\u0c9f"+
		"\u0003\u00dam\u0000\u0c9e\u0c9c\u0001\u0000\u0000\u0000\u0c9f\u0ca2\u0001"+
		"\u0000\u0000\u0000\u0ca0\u0c9e\u0001\u0000\u0000\u0000\u0ca0\u0ca1\u0001"+
		"\u0000\u0000\u0000\u0ca1\u0ca4\u0001\u0000\u0000\u0000\u0ca2\u0ca0\u0001"+
		"\u0000\u0000\u0000\u0ca3\u0c9b\u0001\u0000\u0000\u0000\u0ca3\u0ca4\u0001"+
		"\u0000\u0000\u0000\u0ca4\u0ca5\u0001\u0000\u0000\u0000\u0ca5\u0ca6\u0005"+
		"\u0003\u0000\u0000\u0ca6\u00d9\u0001\u0000\u0000\u0000\u0ca7\u0ca9\u0003"+
		"\u0100\u0080\u0000\u0ca8\u0ca7\u0001\u0000\u0000\u0000\u0ca8\u0ca9\u0001"+
		"\u0000\u0000\u0000\u0ca9\u0caa\u0001\u0000\u0000\u0000\u0caa\u0cab\u0003"+
		"\u00aeW\u0000\u0cab\u00db\u0001\u0000\u0000\u0000\u0cac\u0cad\u0005\u00e8"+
		"\u0000\u0000\u0cad\u0cae\u0003\u00aeW\u0000\u0cae\u00dd\u0001\u0000\u0000"+
		"\u0000\u0caf\u0cb0\u0005\u0093\u0000\u0000\u0cb0\u0cc3\u0003\u0100\u0080"+
		"\u0000\u0cb1\u0cb3\u0005\u00b6\u0000\u0000\u0cb2\u0cb1\u0001\u0000\u0000"+
		"\u0000\u0cb2\u0cb3\u0001\u0000\u0000\u0000\u0cb3\u0cb4\u0001\u0000\u0000"+
		"\u0000\u0cb4\u0cc3\u0005N\u0000\u0000\u0cb5\u0cb6\u0005\u00e8\u0000\u0000"+
		"\u0cb6\u0cb7\u0005\u00b7\u0000\u0000\u0cb7\u0cb8\u0005\u00be\u0000\u0000"+
		"\u0cb8\u0cb9\u0005\u00b7\u0000\u0000\u0cb9\u0cc3\u0005~\u0000\u0000\u0cba"+
		"\u0cbb\u0005&\u0000\u0000\u0cbb\u0cbc\u0005\u00be\u0000\u0000\u0cbc\u0cbd"+
		"\u0005\u00b7\u0000\u0000\u0cbd\u0cc3\u0005~\u0000\u0000\u0cbe\u0cbf\u0005"+
		"\u00f6\u0000\u0000\u0cbf\u0cc3\u0007\u0001\u0000\u0000\u0cc0\u0cc1\u0005"+
		".\u0000\u0000\u0cc1\u0cc3\u0003\u009eO\u0000\u0cc2\u0caf\u0001\u0000\u0000"+
		"\u0000\u0cc2\u0cb2\u0001\u0000\u0000\u0000\u0cc2\u0cb5\u0001\u0000\u0000"+
		"\u0000\u0cc2\u0cba\u0001\u0000\u0000\u0000\u0cc2\u0cbe\u0001\u0000\u0000"+
		"\u0000\u0cc2\u0cc0\u0001\u0000\u0000\u0000\u0cc3\u00df\u0001\u0000\u0000"+
		"\u0000\u0cc4\u0cc5\u0005\u00e6\u0000\u0000\u0cc5\u0d28\u0003\u0084B\u0000"+
		"\u0cc6\u0cc7\u0005\u00fb\u0000\u0000\u0cc7\u0cc8\u0003\u0100\u0080\u0000"+
		"\u0cc8\u0cc9\u0005\u0138\u0000\u0000\u0cc9\u0cca\u0003~?\u0000\u0cca\u0d28"+
		"\u0001\u0000\u0000\u0000\u0ccb\u0ccc\u0005(\u0000\u0000\u0ccc\u0cce\u0003"+
		"~?\u0000\u0ccd\u0ccf\u0003\u00e2q\u0000\u0cce\u0ccd\u0001\u0000\u0000"+
		"\u0000\u0ccf\u0cd0\u0001\u0000\u0000\u0000\u0cd0\u0cce\u0001\u0000\u0000"+
		"\u0000\u0cd0\u0cd1\u0001\u0000\u0000\u0000\u0cd1\u0cd3\u0001\u0000\u0000"+
		"\u0000\u0cd2\u0cd4\u0003\u00e6s\u0000\u0cd3\u0cd2\u0001\u0000\u0000\u0000"+
		"\u0cd3\u0cd4\u0001\u0000\u0000\u0000\u0cd4\u0cd5\u0001\u0000\u0000\u0000"+
		"\u0cd5\u0cd6\u0005X\u0000\u0000\u0cd6\u0cd7\u0005(\u0000\u0000\u0cd7\u0d28"+
		"\u0001\u0000\u0000\u0000\u0cd8\u0cda\u0005(\u0000\u0000\u0cd9\u0cdb\u0003"+
		"\u00e2q\u0000\u0cda\u0cd9\u0001\u0000\u0000\u0000\u0cdb\u0cdc\u0001\u0000"+
		"\u0000\u0000\u0cdc\u0cda\u0001\u0000\u0000\u0000\u0cdc\u0cdd\u0001\u0000"+
		"\u0000\u0000\u0cdd\u0cdf\u0001\u0000\u0000\u0000\u0cde\u0ce0\u0003\u00e6"+
		"s\u0000\u0cdf\u0cde\u0001\u0000\u0000\u0000\u0cdf\u0ce0\u0001\u0000\u0000"+
		"\u0000\u0ce0\u0ce1\u0001\u0000\u0000\u0000\u0ce1\u0ce2\u0005X\u0000\u0000"+
		"\u0ce2\u0ce3\u0005(\u0000\u0000\u0ce3\u0d28\u0001\u0000\u0000\u0000\u0ce4"+
		"\u0ce5\u0005w\u0000\u0000\u0ce5\u0ce6\u0003~?\u0000\u0ce6\u0ce7\u0005"+
		"\u0109\u0000\u0000\u0ce7\u0ceb\u0003\u00eau\u0000\u0ce8\u0cea\u0003\u00e4"+
		"r\u0000\u0ce9\u0ce8\u0001\u0000\u0000\u0000\u0cea\u0ced\u0001\u0000\u0000"+
		"\u0000\u0ceb\u0ce9\u0001\u0000\u0000\u0000\u0ceb\u0cec\u0001\u0000\u0000"+
		"\u0000\u0cec\u0cef\u0001\u0000\u0000\u0000\u0ced\u0ceb\u0001\u0000\u0000"+
		"\u0000\u0cee\u0cf0\u0003\u00e6s\u0000\u0cef\u0cee\u0001\u0000\u0000\u0000"+
		"\u0cef\u0cf0\u0001\u0000\u0000\u0000\u0cf0\u0cf1\u0001\u0000\u0000\u0000"+
		"\u0cf1\u0cf2\u0005X\u0000\u0000\u0cf2\u0cf3\u0005w\u0000\u0000\u0cf3\u0d28"+
		"\u0001\u0000\u0000\u0000\u0cf4\u0cf5\u0005\u0087\u0000\u0000\u0cf5\u0d28"+
		"\u0003\u0100\u0080\u0000\u0cf6\u0cf7\u0005\u0097\u0000\u0000\u0cf7\u0d28"+
		"\u0003\u0100\u0080\u0000\u0cf8\u0cfe\u0005 \u0000\u0000\u0cf9\u0cfa\u0003"+
		"\u00e8t\u0000\u0cfa\u0cfb\u0005\u0145\u0000\u0000\u0cfb\u0cfd\u0001\u0000"+
		"\u0000\u0000\u0cfc\u0cf9\u0001\u0000\u0000\u0000\u0cfd\u0d00\u0001\u0000"+
		"\u0000\u0000\u0cfe\u0cfc\u0001\u0000\u0000\u0000\u0cfe\u0cff\u0001\u0000"+
		"\u0000\u0000\u0cff\u0d02\u0001\u0000\u0000\u0000\u0d00\u0cfe\u0001\u0000"+
		"\u0000\u0000\u0d01\u0d03\u0003\u00eau\u0000\u0d02\u0d01\u0001\u0000\u0000"+
		"\u0000\u0d02\u0d03\u0001\u0000\u0000\u0000\u0d03\u0d04\u0001\u0000\u0000"+
		"\u0000\u0d04\u0d28\u0005X\u0000\u0000\u0d05\u0d06\u0003\u0100\u0080\u0000"+
		"\u0d06\u0d07\u0005\n\u0000\u0000\u0d07\u0d09\u0001\u0000\u0000\u0000\u0d08"+
		"\u0d05\u0001\u0000\u0000\u0000\u0d08\u0d09\u0001\u0000\u0000\u0000\u0d09"+
		"\u0d0a\u0001\u0000\u0000\u0000\u0d0a\u0d0b\u0005\u00a1\u0000\u0000\u0d0b"+
		"\u0d0c\u0003\u00eau\u0000\u0d0c\u0d0d\u0005X\u0000\u0000\u0d0d\u0d0e\u0005"+
		"\u00a1\u0000\u0000\u0d0e\u0d28\u0001\u0000\u0000\u0000\u0d0f\u0d10\u0003"+
		"\u0100\u0080\u0000\u0d10\u0d11\u0005\n\u0000\u0000\u0d11\u0d13\u0001\u0000"+
		"\u0000\u0000\u0d12\u0d0f\u0001\u0000\u0000\u0000\u0d12\u0d13\u0001\u0000"+
		"\u0000\u0000\u0d13\u0d14\u0001\u0000\u0000\u0000\u0d14\u0d15\u0005\u012e"+
		"\u0000\u0000\u0d15\u0d16\u0003~?\u0000\u0d16\u0d17\u0005Q\u0000\u0000"+
		"\u0d17\u0d18\u0003\u00eau\u0000\u0d18\u0d19\u0005X\u0000\u0000\u0d19\u0d1a"+
		"\u0005\u012e\u0000\u0000\u0d1a\u0d28\u0001\u0000\u0000\u0000\u0d1b\u0d1c"+
		"\u0003\u0100\u0080\u0000\u0d1c\u0d1d\u0005\n\u0000\u0000\u0d1d\u0d1f\u0001"+
		"\u0000\u0000\u0000\u0d1e\u0d1b\u0001\u0000\u0000\u0000\u0d1e\u0d1f\u0001"+
		"\u0000\u0000\u0000\u0d1f\u0d20\u0001\u0000\u0000\u0000\u0d20\u0d21\u0005"+
		"\u00e0\u0000\u0000\u0d21\u0d22\u0003\u00eau\u0000\u0d22\u0d23\u0005\u011e"+
		"\u0000\u0000\u0d23\u0d24\u0003~?\u0000\u0d24\u0d25\u0005X\u0000\u0000"+
		"\u0d25\u0d26\u0005\u00e0\u0000\u0000\u0d26\u0d28\u0001\u0000\u0000\u0000"+
		"\u0d27\u0cc4\u0001\u0000\u0000\u0000\u0d27\u0cc6\u0001\u0000\u0000\u0000"+
		"\u0d27\u0ccb\u0001\u0000\u0000\u0000\u0d27\u0cd8\u0001\u0000\u0000\u0000"+
		"\u0d27\u0ce4\u0001\u0000\u0000\u0000\u0d27\u0cf4\u0001\u0000\u0000\u0000"+
		"\u0d27\u0cf6\u0001\u0000\u0000\u0000\u0d27\u0cf8\u0001\u0000\u0000\u0000"+
		"\u0d27\u0d08\u0001\u0000\u0000\u0000\u0d27\u0d12\u0001\u0000\u0000\u0000"+
		"\u0d27\u0d1e\u0001\u0000\u0000\u0000\u0d28\u00e1\u0001\u0000\u0000\u0000"+
		"\u0d29\u0d2a\u0005\u012c\u0000\u0000\u0d2a\u0d2b\u0003~?\u0000\u0d2b\u0d2c"+
		"\u0005\u0109\u0000\u0000\u0d2c\u0d2d\u0003\u00eau\u0000\u0d2d\u00e3\u0001"+
		"\u0000\u0000\u0000\u0d2e\u0d2f\u0005V\u0000\u0000\u0d2f\u0d30\u0003~?"+
		"\u0000\u0d30\u0d31\u0005\u0109\u0000\u0000\u0d31\u0d32\u0003\u00eau\u0000"+
		"\u0d32\u00e5\u0001\u0000\u0000\u0000\u0d33\u0d34\u0005T\u0000\u0000\u0d34"+
		"\u0d35\u0003\u00eau\u0000\u0d35\u00e7\u0001\u0000\u0000\u0000\u0d36\u0d37"+
		"\u0005E\u0000\u0000\u0d37\u0d3c\u0003\u0100\u0080\u0000\u0d38\u0d39\u0005"+
		"\u0004\u0000\u0000\u0d39\u0d3b\u0003\u0100\u0080\u0000\u0d3a\u0d38\u0001"+
		"\u0000\u0000\u0000\u0d3b\u0d3e\u0001\u0000\u0000\u0000\u0d3c\u0d3a\u0001"+
		"\u0000\u0000\u0000\u0d3c\u0d3d\u0001\u0000\u0000\u0000\u0d3d\u0d3f\u0001"+
		"\u0000\u0000\u0000\u0d3e\u0d3c\u0001\u0000\u0000\u0000\u0d3f\u0d42\u0003"+
		"\u00aeW\u0000\u0d40\u0d41\u0005F\u0000\u0000\u0d41\u0d43\u0003\u0084B"+
		"\u0000\u0d42\u0d40\u0001\u0000\u0000\u0000\u0d42\u0d43\u0001\u0000\u0000"+
		"\u0000\u0d43\u00e9\u0001\u0000\u0000\u0000\u0d44\u0d45\u0003\u00e0p\u0000"+
		"\u0d45\u0d46\u0005\u0145\u0000\u0000\u0d46\u0d48\u0001\u0000\u0000\u0000"+
		"\u0d47\u0d44\u0001\u0000\u0000\u0000\u0d48\u0d49\u0001\u0000\u0000\u0000"+
		"\u0d49\u0d47\u0001\u0000\u0000\u0000\u0d49\u0d4a\u0001\u0000\u0000\u0000"+
		"\u0d4a\u00eb\u0001\u0000\u0000\u0000\u0d4b\u0d52\u00055\u0000\u0000\u0d4c"+
		"\u0d52\u0005\u00f8\u0000\u0000\u0d4d\u0d52\u0005I\u0000\u0000\u0d4e\u0d52"+
		"\u0005\u007f\u0000\u0000\u0d4f\u0d52\u0005\u011f\u0000\u0000\u0d50\u0d52"+
		"\u0003\u0100\u0080\u0000\u0d51\u0d4b\u0001\u0000\u0000\u0000\u0d51\u0d4c"+
		"\u0001\u0000\u0000\u0000\u0d51\u0d4d\u0001\u0000\u0000\u0000\u0d51\u0d4e"+
		"\u0001\u0000\u0000\u0000\u0d51\u0d4f\u0001\u0000\u0000\u0000\u0d51\u0d50"+
		"\u0001\u0000\u0000\u0000\u0d52\u00ed\u0001\u0000\u0000\u0000\u0d53\u0d57"+
		"\u0005\u0104\u0000\u0000\u0d54\u0d57\u0005\u00f3\u0000\u0000\u0d55\u0d57"+
		"\u0003\u0100\u0080\u0000\u0d56\u0d53\u0001\u0000\u0000\u0000\u0d56\u0d54"+
		"\u0001\u0000\u0000\u0000\u0d56\u0d55\u0001\u0000\u0000\u0000\u0d57\u00ef"+
		"\u0001\u0000\u0000\u0000\u0d58\u0d5a\u0003\u00eew\u0000\u0d59\u0d58\u0001"+
		"\u0000\u0000\u0000\u0d59\u0d5a\u0001\u0000\u0000\u0000\u0d5a\u0d5b\u0001"+
		"\u0000\u0000\u0000\u0d5b\u0d5c\u0003\u00f2y\u0000\u0d5c\u00f1\u0001\u0000"+
		"\u0000\u0000\u0d5d\u0d62\u0003\u0100\u0080\u0000\u0d5e\u0d5f\u0005\u0001"+
		"\u0000\u0000\u0d5f\u0d61\u0003\u0100\u0080\u0000\u0d60\u0d5e\u0001\u0000"+
		"\u0000\u0000\u0d61\u0d64\u0001\u0000\u0000\u0000\u0d62\u0d60\u0001\u0000"+
		"\u0000\u0000\u0d62\u0d63\u0001\u0000\u0000\u0000\u0d63\u00f3\u0001\u0000"+
		"\u0000\u0000\u0d64\u0d62\u0001\u0000\u0000\u0000\u0d65\u0d66\u0005g\u0000"+
		"\u0000\u0d66\u0d67\u0003\u00f6{\u0000\u0d67\u0d68\u0005\u001c\u0000\u0000"+
		"\u0d68\u0d69\u0005\u00bb\u0000\u0000\u0d69\u0d6a\u0003\u0084B\u0000\u0d6a"+
		"\u00f5\u0001\u0000\u0000\u0000\u0d6b\u0d6c\u0007!\u0000\u0000\u0d6c\u00f7"+
		"\u0001\u0000\u0000\u0000\u0d6d\u0d71\u0003\u00fa}\u0000\u0d6e\u0d71\u0005"+
		"@\u0000\u0000\u0d6f\u0d71\u0005<\u0000\u0000\u0d70\u0d6d\u0001\u0000\u0000"+
		"\u0000\u0d70\u0d6e\u0001\u0000\u0000\u0000\u0d70\u0d6f\u0001\u0000\u0000"+
		"\u0000\u0d71\u00f9\u0001\u0000\u0000\u0000\u0d72\u0d78\u0003\u0100\u0080"+
		"\u0000\u0d73\u0d74\u0005\u0121\u0000\u0000\u0d74\u0d78\u0003\u0100\u0080"+
		"\u0000\u0d75\u0d76\u0005\u00eb\u0000\u0000\u0d76\u0d78\u0003\u0100\u0080"+
		"\u0000\u0d77\u0d72\u0001\u0000\u0000\u0000\u0d77\u0d73\u0001\u0000\u0000"+
		"\u0000\u0d77\u0d75\u0001\u0000\u0000\u0000\u0d78\u00fb\u0001\u0000\u0000"+
		"\u0000\u0d79\u0d7e\u0003\u0100\u0080\u0000\u0d7a\u0d7b\u0005\u0004\u0000"+
		"\u0000\u0d7b\u0d7d\u0003\u0100\u0080\u0000\u0d7c\u0d7a\u0001\u0000\u0000"+
		"\u0000\u0d7d\u0d80\u0001\u0000\u0000\u0000\u0d7e\u0d7c\u0001\u0000\u0000"+
		"\u0000\u0d7e\u0d7f\u0001\u0000\u0000\u0000\u0d7f\u00fd\u0001\u0000\u0000"+
		"\u0000\u0d80\u0d7e\u0001\u0000\u0000\u0000\u0d81\u0d88\u00055\u0000\u0000"+
		"\u0d82\u0d88\u0005\u00f8\u0000\u0000\u0d83\u0d88\u0005I\u0000\u0000\u0d84"+
		"\u0d88\u0005\u007f\u0000\u0000\u0d85\u0d88\u0005\u011f\u0000\u0000\u0d86"+
		"\u0d88\u0003\u0100\u0080\u0000\u0d87\u0d81\u0001\u0000\u0000\u0000\u0d87"+
		"\u0d82\u0001\u0000\u0000\u0000\u0d87\u0d83\u0001\u0000\u0000\u0000\u0d87"+
		"\u0d84\u0001\u0000\u0000\u0000\u0d87\u0d85\u0001\u0000\u0000\u0000\u0d87"+
		"\u0d86\u0001\u0000\u0000\u0000\u0d88\u00ff\u0001\u0000\u0000\u0000\u0d89"+
		"\u0d8f\u0005\u014c\u0000\u0000\u0d8a\u0d8f\u0005\u014e\u0000\u0000\u0d8b"+
		"\u0d8f\u0003\u0106\u0083\u0000\u0d8c\u0d8f\u0005\u014f\u0000\u0000\u0d8d"+
		"\u0d8f\u0005\u014d\u0000\u0000\u0d8e\u0d89\u0001\u0000\u0000\u0000\u0d8e"+
		"\u0d8a\u0001\u0000\u0000\u0000\u0d8e\u0d8b\u0001\u0000\u0000\u0000\u0d8e"+
		"\u0d8c\u0001\u0000\u0000\u0000\u0d8e\u0d8d\u0001\u0000\u0000\u0000\u0d8f"+
		"\u0101\u0001\u0000\u0000\u0000\u0d90\u0d92\u0005\u013f\u0000\u0000\u0d91"+
		"\u0d90\u0001\u0000\u0000\u0000\u0d91\u0d92\u0001\u0000\u0000\u0000\u0d92"+
		"\u0d93\u0001\u0000\u0000\u0000\u0d93\u0d9d\u0005\u014a\u0000\u0000\u0d94"+
		"\u0d96\u0005\u013f\u0000\u0000\u0d95\u0d94\u0001\u0000\u0000\u0000\u0d95"+
		"\u0d96\u0001\u0000\u0000\u0000\u0d96\u0d97\u0001\u0000\u0000\u0000\u0d97"+
		"\u0d9d\u0005\u014b\u0000\u0000\u0d98\u0d9a\u0005\u013f\u0000\u0000\u0d99"+
		"\u0d98\u0001\u0000\u0000\u0000\u0d99\u0d9a\u0001\u0000\u0000\u0000\u0d9a"+
		"\u0d9b\u0001\u0000\u0000\u0000\u0d9b\u0d9d\u0005\u0149\u0000\u0000\u0d9c"+
		"\u0d91\u0001\u0000\u0000\u0000\u0d9c\u0d95\u0001\u0000\u0000\u0000\u0d9c"+
		"\u0d99\u0001\u0000\u0000\u0000\u0d9d\u0103\u0001\u0000\u0000\u0000\u0d9e"+
		"\u0da1\u0003\u0100\u0080\u0000\u0d9f\u0da1\u0003\u009eO\u0000\u0da0\u0d9e"+
		"\u0001\u0000\u0000\u0000\u0da0\u0d9f\u0001\u0000\u0000\u0000\u0da1\u0105"+
		"\u0001\u0000\u0000\u0000\u0da2\u0da3\u0007\"\u0000\u0000\u0da3\u0107\u0001"+
		"\u0000\u0000\u0000\u01ca\u0127\u012e\u0132\u0136\u013c\u0140\u0147\u014c"+
		"\u0150\u0156\u015a\u016d\u0173\u0177\u017b\u017f\u0187\u018b\u018e\u0193"+
		"\u0199\u01a2\u01a8\u01ac\u01b2\u01b9\u01c2\u01ce\u01d7\u01e0\u01e6\u01f1"+
		"\u01f9\u0201\u0208\u0212\u0219\u0221\u0230\u0253\u0256\u0259\u025d\u0263"+
		"\u0268\u026f\u0275\u0279\u027d\u0285\u028b\u028f\u029d\u02a5\u02b8\u02d1"+
		"\u02d4\u02db\u02e2\u02eb\u02ef\u02f6\u02fe\u0307\u030d\u0312\u0316\u031e"+
		"\u0323\u032c\u0332\u0339\u0342\u0348\u034c\u0352\u0359\u035e\u036b\u0370"+
		"\u037c\u0380\u0386\u038f\u0394\u039a\u03b2\u03b8\u03ba\u03c0\u03c6\u03c8"+
		"\u03d0\u03d2\u03dc\u03de\u03ed\u03f2\u03f9\u0403\u0409\u040b\u0413\u0415"+
		"\u042e\u0431\u0435\u0439\u044b\u044e\u0459\u045c\u046c\u0476\u047b\u0481"+
		"\u0484\u048d\u048f\u0492\u049c\u04a0\u04a6\u04ad\u04b2\u04b8\u04bc\u04c0"+
		"\u04c6\u04d1\u04da\u04e4\u04e7\u04ec\u04ee\u04f5\u04fb\u04fd\u0501\u050b"+
		"\u0511\u0514\u0516\u0522\u0529\u052d\u0531\u0535\u053c\u0545\u0548\u054c"+
		"\u0551\u0555\u055d\u0560\u0563\u056a\u0575\u0578\u0582\u0585\u0590\u0595"+
		"\u059d\u05a0\u05a4\u05ad\u05b6\u05b9\u05c2\u05c5\u05c8\u05cc\u05d7\u05da"+
		"\u05e1\u05e4\u05f7\u05fb\u05ff\u0603\u0607\u060b\u060d\u0618\u061d\u0626"+
		"\u062f\u0632\u0638\u0644\u0647\u0650\u0653\u065b\u065e\u0661\u0666\u0669"+
		"\u0675\u0678\u0680\u0685\u0689\u068b\u068d\u069c\u069e\u06a9\u06be\u06c8"+
		"\u06d3\u06d7\u06d9\u06e1\u06e8\u06f5\u06fb\u0715\u0724\u0729\u072d\u0737"+
		"\u073d\u0743\u074b\u0750\u0757\u0759\u075f\u0765\u0769\u076e\u0777\u077c"+
		"\u078a\u0794\u0797\u07a0\u07a5\u07aa\u07ac\u07b5\u07b8\u07c0\u07c3\u07ca"+
		"\u07cf\u07da\u07dd\u07e1\u07e3\u07eb\u07f5\u07fb\u07fd\u0804\u0808\u080a"+
		"\u0811\u0815\u0817\u0819\u0822\u082d\u0831\u083b\u0845\u0849\u0851\u0853"+
		"\u0860\u0868\u0871\u0877\u087f\u0885\u0889\u088e\u0893\u0899\u08a7\u08a9"+
		"\u08c7\u08d2\u08da\u08df\u08e4\u08f1\u08f7\u08fa\u0901\u0906\u0909\u090c"+
		"\u0911\u0918\u091b\u0924\u0927\u092b\u092e\u0931\u0940\u0943\u0956\u095a"+
		"\u0962\u0966\u097f\u0982\u098b\u0991\u0997\u099d\u09a6\u09a9\u09ac\u09bf"+
		"\u09c8\u09de\u09e1\u09eb\u09f4\u09fa\u0a00\u0a0b\u0a0d\u0a12\u0a19\u0a1b"+
		"\u0a21\u0a27\u0a32\u0a3b\u0a40\u0a45\u0a47\u0a49\u0a4f\u0a51\u0a5b\u0a64"+
		"\u0a66\u0a6c\u0a6e\u0a71\u0a7b\u0a7d\u0a85\u0a8d\u0a90\u0a95\u0a9a\u0aa6"+
		"\u0aaa\u0aae\u0ab1\u0ab3\u0abb\u0abe\u0ac8\u0ad0\u0ad6\u0ad8\u0ae0\u0aea"+
		"\u0af0\u0afe\u0b07\u0b0e\u0b13\u0b1a\u0b24\u0b29\u0b30\u0b4a\u0b4f\u0b51"+
		"\u0b58\u0b5c\u0b63\u0b67\u0b78\u0b87\u0b8e\u0b97\u0ba1\u0ba6\u0baf\u0bb4"+
		"\u0bbc\u0bc4\u0bc7\u0bcd\u0bd0\u0bd7\u0bdf\u0be2\u0bea\u0bed\u0c07\u0c12"+
		"\u0c17\u0c1e\u0c20\u0c2d\u0c3c\u0c40\u0c44\u0c48\u0c4e\u0c52\u0c56\u0c5a"+
		"\u0c5c\u0c66\u0c6d\u0c76\u0c7d\u0c84\u0c8b\u0c94\u0ca0\u0ca3\u0ca8\u0cb2"+
		"\u0cc2\u0cd0\u0cd3\u0cdc\u0cdf\u0ceb\u0cef\u0cfe\u0d02\u0d08\u0d12\u0d1e"+
		"\u0d27\u0d3c\u0d42\u0d49\u0d51\u0d56\u0d59\u0d62\u0d70\u0d77\u0d7e\u0d87"+
		"\u0d8e\u0d91\u0d95\u0d99\u0d9c\u0da0";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}