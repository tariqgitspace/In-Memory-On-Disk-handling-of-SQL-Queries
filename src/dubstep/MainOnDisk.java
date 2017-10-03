/*
CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),PRIMARY KEY (ORDERKEY,LINENUMBER),INDEX LINEITEM_shipdate (shipdate)); SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1998-08-10') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;
CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),PRIMARY KEY (ORDERKEY,LINENUMBER),INDEX LINEITEM_shipdate (shipdate));SELECT LINEITEM.EXTENDEDPRICE*1-LINEITEM.DISCOUNT from LINEITEM
SELECT IN_LINE.PARTKEY, IN_LINE.LINENUMBER FROM (SELECT (SUPPKEY+ORDERKEY) AS PARTKEY,(SUPPKEY*ORDERKEY) AS LINENUMBER FROM LINEITEM) IN_LINE

CREATE TABLE LINEITEM
(ORDERKEY INT,PARTKEY INT,SUPPKEY INT
,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL
,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1)
,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE
,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10)
,PRIMARY KEY (ORDERKEY,LINENUMBER)
,INDEX LINEITEM_shipdate (shipdate));


CREATE TABLE LINEITEM2
(ORDERKEY INT,PARTKEY INT,SUPPKEY INT
,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL
,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1)
,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE
,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10)
,PRIMARY KEY (ORDERKEY,LINENUMBER)
,INDEX LINEITEM_shipdate (shipdate));

SELECT
SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE >= DATE('1995-01-01')
AND LINEITEM.SHIPDATE < DATE ('1996-01-01')
AND LINEITEM.DISCOUNT > 0.08 AND LINEITEM.DISCOUNT < 0.1 
AND LINEITEM.QUANTITY < 25;




SELECT
 *
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE >= DATE('1995-01-01')
AND LINEITEM.SHIPDATE < DATE ('1996-01-01')
AND LINEITEM.DISCOUNT > 0.08 AND LINEITEM.DISCOUNT < 0.1 
AND LINEITEM.QUANTITY < 25
LIMIT 10;

CREATE TABLE LINEITEM (ORDERKEY INT,PARTKEY INT,SUPPKEY INT ,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL ,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1) ,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE ,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10) ,PRIMARY KEY (ORDERKEY,LINENUMBER) ,INDEX LINEITEM_shipdate (shipdate)); SELECT SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE FROM LINEITEM WHERE LINEITEM.SHIPDATE >= DATE('1994-01-01') AND LINEITEM.SHIPDATE < DATE ('1995-01-01') AND LINEITEM.DISCOUNT > 0.01 AND LINEITEM.DISCOUNT < 0.03 AND LINEITEM.QUANTITY < 25;

<shipdate,<orderkey+linenumber,rest of the cols>>
12345,1,2,jyoti1
12345,1,3,jyoti2
12345,2,4,jyoti3
12346,3,4

<12345,<1+2><1+3><2+4>>



SELECT
SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE >= DATE('1994-01-01')
AND LINEITEM.SHIPDATE < DATE ('1995-01-01')
AND LINEITEM.DISCOUNT > 0.01 AND LINEITEM.DISCOUNT < 0.03 
AND LINEITEM.QUANTITY < 25;

SELECT LINEITEM.EXTENDEDPRICE*1-LINEITEM.DISCOUNT 
from LINEITEM where LINEITEM.DISCOUNT>0.002;

SELECT
LINEITEM.RETURNFLAG,
LINEITEM.LINESTATUS,
SUM(LINEITEM.QUANTITY) AS SUM_QTY,
SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, 
SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, 
SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, 
AVG(LINEITEM.QUANTITY) AS AVG_QTY,
AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE,
AVG(LINEITEM.DISCOUNT) AS AVG_DISC,
COUNT(*) AS COUNT_ORDER
FROM
LINEITEM
WHERE
LINEITEM.SHIPDATE <= DATE('1998-09-03')
GROUP BY 
LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS 
ORDER BY
LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;

SELECT LINEITEM.RECEIPTDATE
, LINEITEM.PARTKEY
, LINEITEM.EXTENDEDPRICE FROM LINEITEM WHERE DATE('1997-01-01') < LINEITEM.SHIPDATE 
AND LINEITEM.SHIPDATE <= DATE('1998-01-01') 
LIMIT 10;

CREATE TABLE LINEITEM
(ORDERKEY INT,PARTKEY INT,SUPPKEY INT
,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL
,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1)
,LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE
,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10)
,PRIMARY KEY (ORDERKEY,LINENUMBER)
,INDEX LINEITEM_shipdate (shipdate));
SELECT LINEITEM.RECEIPTDATE
, LINEITEM.PARTKEY
, LINEITEM.EXTENDEDPRICE FROM LINEITEM WHERE DATE('1997-01-01') < LINEITEM.SHIPDATE 
AND LINEITEM.SHIPDATE <= DATE('1998-01-01') 
LIMIT 10;

 */
package dubstep;

import net.sf.jsqlparser.eval.Eval;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.dgc.Lease;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;

import javax.swing.text.StyleContext.SmallAttributeSet;

import dubstep.Aggregate.EvalLib;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;

import dubstep.Aggregate.EvalLib;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class MainOnDisk {

	private Map<String, Map<String, Map<String, Double>>> aggregateHistory = new HashMap<String, Map<String, Map<String, Double>>>();
	private static Scanner scan;
	final char delimiter = '|';
	private String[] rowData = new String[32];
	private BufferedReader br = null;
	//private String csvFile = "src\\dubstep\\data\\";
	private Runtime rt = Runtime.getRuntime();
	public static String csvFile = "data/";
	private String line = "";
	private Map<String, List<String>> indexNameToColumnsMap = new HashMap<String, List<String>>();
	private Statement statement;
	private PlainSelect plain;
	private Map<String, ArrayList<String>> columnDataTypes = new HashMap<String, ArrayList<String>>();
	private Map<String, Map<String, Integer>> columnNameToIndexMapping = new HashMap<String, Map<String, Integer>>();
	private Select select;
	private SelectBody body;
	private CCJSqlParser parser;
	// private Map<Integer, HashMap<Object, List<Object>>> indexToPrmryKeyMap =
	// new HashMap<Integer,HashMap<Object,List<Object>>>();

	private Table globaltable;
	private Map<String, TreeMap<Object, TreeSet<Object>>> indexToPrmryKeyMap = new HashMap<String, TreeMap<Object, TreeSet<Object>>>();
	private TreeMap<Object, Object> primaryKeyToDataMap = new TreeMap<Object, Object>();
	Map<String, String> aliases = new HashMap<String, String>();
	private String globaltableName;
	private List<Expression> allWhereClauses = new ArrayList<Expression>();
	private boolean nestedquery = false;
	private Integer nestedquery_loopnumber = 0;
	private Map<Integer, Boolean> validrows = new HashMap<Integer, Boolean>();
	private Map<Integer, Boolean> validrows_backup = new HashMap<Integer, Boolean>();
	private static String arg = null;
	private ArrayList<String[]> resultSet = null;
	private StringBuilder sb = null;
	Object StartKey = null;
	Object EndKey = null;
	Object StartKeyInserted = null;
	Object EndKeyInserted = null;
	boolean debug=false;

	public void readQueries(String temp) throws ParseException {
		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();
	}

	public void parseQueries() throws Exception {

		while (statement != null) {
			if (statement instanceof CreateTable) {
				primaryKeyToDataMap = null;
				indexNameToColumnsMap = null;
				indexToPrmryKeyMap = null;
				validrows = new HashMap<Integer, Boolean>();

				// System.out.println(arg);

				Runtime.getRuntime().freeMemory();
				Runtime.getRuntime().gc();
				getColumnDataTypesAndMapColumnNameToIndex();

			} else if (statement instanceof Select) {

				parseSelectStatement();
			} else {
				throw new Exception("I can't understand statement instanceof Select" + statement);
			}
			statement = parser.Statement();
		}
	}

	public void parseSelectStatement() throws Exception {

		select = (Select) statement;
		body = select.getSelectBody();

		if (body instanceof PlainSelect) {

			plain = (PlainSelect) body;
			Table table = null;
			if (plain.getFromItem() instanceof SubSelect) {
				nestedquery_loopnumber = 0;
				nestedquery = true;
				getAllNestedSubQueriesFrom(plain);
				nestedquery_loopnumber = 0;

				/*
				 * outerplain.setFromItem(globaltable); String xstring
				 * =outerplain.toString();
				 * 
				 * for(int i=0;i< allWhereClauses.size();i++) {
				 * if(xstring.contains(allWhereClauses.get(i).toString())) {
				 * continue; }
				 * xstring=xstring.concat(" and ").concat(allWhereClauses.get(i)
				 * .toString()); } System.out.println("OuterMost : "+xstring);
				 */
				// setSubSelectCalls((SubSelect) plain.getFromItem());
			} else {// if(plain.getFromItem() instanceof Table){
				globaltable = (Table) plain.getFromItem();
				globaltableName = globaltable.getName();
//				for (int i = 0; i < primaryKeyToDataMap.size(); i++) {
//					validrows.put(i, true);
//				}

				// Arrays.fill(validrows,true);
				getSelectedColumns(globaltableName, null, plain.getWhere());

			}
		}

		else {
			throw new Exception("I can't understand body instanceof PlainSelect " + statement);
		}
		/** Do something with the select operator **/

	}

	@SuppressWarnings("null")
	public void getAllNestedSubQueriesFrom(PlainSelect nestedplain) throws Exception {

		nestedquery_loopnumber++;
		Integer local_loop = nestedquery_loopnumber;

		if (local_loop != 1) // so that at starting whole select call itself
		{
			getAliases(nestedplain);
			if (nestedplain.getWhere() != null) {
				allWhereClauses.add((Expression) nestedplain.getWhere());
			}

			SubSelect x = (SubSelect) nestedplain.getFromItem();
			SelectBody nestedbody = x.getSelectBody();
			nestedplain = (PlainSelect) nestedbody;

		}
		if (nestedplain.getFromItem() instanceof SubSelect) {
			// System.out.println("FWD :"+nestedplain);
			// getSelectedColumns(globaltableName, outerplain.getWhere());
			getAllNestedSubQueriesFrom(nestedplain);
			// nestedplain.setFromItem(globaltable);
			// System.out.println("IF :"+outerplain.getSelectItems());
			if (local_loop == 1) // outermost loop
			{
				// System.out.println("REVERSE :"+nestedplain);
				nestedquery = false;
				getSelectedColumns(globaltableName, nestedplain, nestedplain.getWhere());
			} else {
				// System.out.println("REVERSE :"+nestedplain);
				getSelectedColumns(globaltableName, nestedplain, nestedplain.getWhere());
			}

		} else {
			// System.out.println("Else :"+nestedplain);
			// System.out.println(outerplain.getSelectItems());
			globaltable = (Table) nestedplain.getFromItem();
			globaltableName = globaltable.getName();
			for (int i = 0; i < primaryKeyToDataMap.size(); i++) {
				validrows.put(i, true);
			}
			getSelectedColumns(globaltableName, nestedplain, nestedplain.getWhere());
		}
	}

	public void getAliases(PlainSelect plain) {

		List<SelectItem> selectItems = plain.getSelectItems();
		int length = plain.getSelectItems().size();
		// System.out.println(length);
		for (int i = 0; i < length; i++) {
			if (plain.getSelectItems().get(i).toString().equals("*")) {
				continue;
			}
			// System.out.println("Error : " +plain.getSelectItems().get(i));
			String aliasname = ((SelectExpressionItem) plain.getSelectItems().get(i)).getAlias();
			// System.out.println("ALIAS name: " +aliasname);
			if (aliasname == null) {
				continue;
			}
			String actualName = ((SelectExpressionItem) selectItems.get(i)).getExpression().toString();
			// System.out.println(actualName);
			aliases.put(aliasname, actualName);
			// System.out.println("ALIAS : " +aliases.get(aliasname) + " "+
			// aliasname);
		}
	}

	public void createColumnIndexes(String tableName) {
		String csvFile_local_copy = csvFile + tableName + ".csv";
		String dataType = "";
		long usedMB = 0;
		try {
			indexNameToColumnsMap = new HashMap<String, List<String>>();

			indexToPrmryKeyMap = new HashMap<String, TreeMap<Object, TreeSet<Object>>>();
			primaryKeyToDataMap = new TreeMap<Object, Object>();
			TreeMap<Object, ArrayList<Object>> indexToDataMap = new TreeMap<Object, ArrayList<Object>>();
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));

			CreateTable create = (CreateTable) statement;
			// int[] indexColNum = new
			// int[create.getColumnDefinitions().size()];
			for (Index i : create.getIndexes()) {

				if (i.getName() == null && i.getColumnsNames().size() != 0) {
					indexNameToColumnsMap.put(i.getType(), i.getColumnsNames());
				} else {
					indexNameToColumnsMap.put(i.getType() + "_" + String.join("_", i.getColumnsNames()),
							i.getColumnsNames());
					// indexNameToColumnsMap.put(i.getColumnsNames().get(0),i.getColumnsNames());
				}
			}
			TreeMap<Object, FileWriter> indexToFileWriterMap = new TreeMap<Object, FileWriter>();
			if (indexNameToColumnsMap != null) {
				for (String s : indexNameToColumnsMap.keySet()) {

					if (indexNameToColumnsMap.keySet().contains("PRIMARY KEY") && s.contains("INDEX")) {
						// System.out.println("Building Indexes..");
						if (s.contains("INDEX") && (indexToPrmryKeyMap.get(indexNameToColumnsMap.get(s)) == null)) {
							indexToPrmryKeyMap.put(indexNameToColumnsMap.get(s).get(0),
									new TreeMap<Object, TreeSet<Object>>());
						}
					}
				}
			}

			if(debug)
			{
				return;
			}
			//System.out.println("Start On-disk");

			if (indexNameToColumnsMap != null) {

				int last_saved_line = 0;
				int file_number = 0;
				int line_number = 0;
				String indexName = "";
				Runtime.getRuntime().freeMemory();
				Runtime.getRuntime().gc();
				System.gc();
				File tempFile = null;
				FileWriter tempWriter = null;
				usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
				// System.out.println("Processing Indexes..");
				String fileName = "";
				StringBuilder sbOut = new StringBuilder();

				while ((line = br.readLine()) != null) {
					split(line);
					String[] r= new String[rowData.length];
					System.arraycopy(rowData, 0, r, 0, rowData.length);

					for (String s : indexNameToColumnsMap.keySet()) {
						// Dont change this
						int index = columnNameToIndexMapping.get(tableName)
								.get(indexNameToColumnsMap.get(s).get(0).toUpperCase());

						if (s.contains("INDEX")) {
							indexName = s;
							if (rowData[index].length() > 0) {

								if (indexToDataMap.get(rowData[index]) == null) {
									indexToDataMap.put(rowData[index], new ArrayList<Object>());
								}
								indexToDataMap.get(rowData[index]).add(r);
								line_number++;
								r = null;
								if (line_number != 0 && line_number % 20000 == 0) {
									usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

									if (usedMB > 80) {

//										System.out.println(line_number);
//										System.out.println(usedMB);
										System.gc();
										Runtime.getRuntime().freeMemory();
										Runtime.getRuntime().gc();
										usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

										for (Object key : indexToDataMap.keySet()) {
											fileName = csvFile + tableName + "_"
													+ indexNameToColumnsMap.get(s).get(0).toUpperCase() + "_"
													+ (String) key + ".csv";
											tempFile = new File(fileName);
											if (!tempFile.exists()) {
												indexToFileWriterMap.put(key, new FileWriter(fileName, true));
											}
											for (Object row : indexToDataMap.get(key)) {
												sbOut.append(String.join("|", (String[]) row)).append("\n");
											}

											indexToFileWriterMap.get(key).write(sbOut.toString());
											indexToFileWriterMap.get(key).flush();
											// tempWriter.close();

											// tempWriter = null;
											sbOut = null;
											sbOut = new StringBuilder();
										}
										// line_number = 0;
										indexToDataMap = null;
										indexToDataMap = new TreeMap<Object, ArrayList<Object>>();
										Runtime.getRuntime().freeMemory();
										Runtime.getRuntime().gc();
										// usedMB = (rt.totalMemory() -
										// rt.freeMemory()) / 1024 / 1024;

									}
								}

							}
						} else if (s.contains("PRIMARY")) {
							continue;
						}
					}
				}

				System.gc();
				//System.out.println(line_number);
				Runtime.getRuntime().freeMemory();
				Runtime.getRuntime().gc();
				usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
				//System.out.println(usedMB);
				for (Object key : indexToDataMap.keySet()) {
					fileName = csvFile + tableName + "_" + indexNameToColumnsMap.get(indexName).get(0).toUpperCase()
							+ "_" + (String) key + ".csv";
					tempFile = new File(fileName);
					if (!tempFile.exists()) {
						indexToFileWriterMap.put(key, new FileWriter(fileName, true));
					}
					for (Object row : indexToDataMap.get(key)) {
						sbOut.append(String.join("|", (String[]) row)).append("\n");
					}

					indexToFileWriterMap.get(key).write(sbOut.toString());
					indexToFileWriterMap.get(key).flush();
					// tempWriter.close();

					// tempWriter = null;
					sbOut = null;
					sbOut = new StringBuilder();
				}
				// line_number = 0;
				indexToDataMap = null;
				indexToDataMap = new TreeMap<Object, ArrayList<Object>>();
				Runtime.getRuntime().freeMemory();
				Runtime.getRuntime().gc();
				// usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

				//System.out.println("Done saving to disk");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(ObjectSizeFetcher.getObjectSize(list));
	}

	public void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException {

		Runtime.getRuntime().freeMemory();
		Runtime.getRuntime().gc();
		double heapSize = (double) Runtime.getRuntime().totalMemory() / (double) (1024 * 1024);
		double heapFreeSize = (double) Runtime.getRuntime().freeMemory() / (double) (1024 * 1024);
		CreateTable create = (CreateTable) statement;
		String tableName = create.getTable().getName();
		Map<String, Integer> columnNameToIndexMap = new HashMap<String, Integer>();
		List<ColumnDefinition> si = create.getColumnDefinitions();
		ListIterator<ColumnDefinition> it = si.listIterator();
		ArrayList<String> dataTypes = new ArrayList<String>();

		File dir = new File(csvFile);
		File[] foundFiles = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(tableName + "_");
			}
		});
		if(!debug)
		{
			 for (final File file : foundFiles) {
			 if (!file.delete()) {
			 System.err.println("Can't remove " + file.getAbsolutePath());
			 }
			 // Process file
			 // br = new BufferedReader(new FileReader(file));
			 }
		}

		int i = 0;
		while (it.hasNext()) {
			ColumnDefinition cd = it.next();
			dataTypes.add(cd.getColDataType().toString());
			columnNameToIndexMap.put(cd.getColumnName(), i++);
		}
		columnDataTypes.put(tableName, dataTypes);
		columnNameToIndexMapping.put(tableName, columnNameToIndexMap);
		List<Index> indexList = ((CreateTable) statement).getIndexes();
		if (indexList != null) {// can have duplicates
			
			createColumnIndexes(tableName);
		}
	}

	public void returnStartEnd(String tableName, Expression whereExpression) throws InvalidPrimitive, SQLException {

		boolean primaryIdxPresent = false;
		boolean indexPresent = false;
		String primaryLeftORRight = "";
		String indexLeftORRight = "";
		StartKey = null;
		EndKey = null;
		StartKeyInserted = null;
		EndKeyInserted = null;

		if (!(whereExpression instanceof AndExpression)) {
			return;
		}
		Expression a = ((AndExpression) whereExpression);
		EvalLib e = new EvalLib(tableName);

		// if(((AndExpression)whereExpression)!=null){

		while (((AndExpression) a).getLeftExpression() instanceof AndExpression) {
			a = (AndExpression) ((AndExpression) a).getLeftExpression();
		}
		// indexToPrmryKeyMap.keySet().toArray(a)

		// rowData = new String[columnNameToIndexMapping.get(tableName).size()];

		String final_s = null;
		for (String s : indexNameToColumnsMap.keySet()) {
			if (s.contains("INDEX")) {

				if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())
						&& a.toString().split(((AndExpression) a).getStringExpression())[1]
								.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
					// System.out.println("both expressions contain index
					// column");
					final_s = s;
					indexPresent = true;
					indexLeftORRight = "Both";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
					// System.out.println("left expression contain index
					// column");
					a = ((AndExpression) a).getLeftExpression();
					final_s = s;
					indexPresent = true;
					indexLeftORRight = "Left";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
						.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
					// System.out.println("right expression contain index
					// column");
					a = ((AndExpression) a).getRightExpression();
					final_s = s;
					indexPresent = true;
					indexLeftORRight = "Right";
					// break;
				}
			} else if (s.contains("PRIMARY")) {
				if (indexNameToColumnsMap.get(s).size() == 2) {
					break;
				}
				if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())
						&& a.toString().split(((AndExpression) a).getStringExpression())[1]
								.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
					// System.out.println("both expressions contain index
					// column");
					final_s = s;
					primaryIdxPresent = true;
					primaryLeftORRight = "Both";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
					// System.out.println("left expression contain index
					// column");
					a = ((AndExpression) a).getLeftExpression();
					final_s = s;
					primaryIdxPresent = true;
					primaryLeftORRight = "Left";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
						.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
					// System.out.println("right expression contain index
					// column");
					a = ((AndExpression) a).getRightExpression();
					final_s = s;
					primaryIdxPresent = true;
					primaryLeftORRight = "Right";
					// break;
				}
			}

			// Main Logic Start
			if (primaryIdxPresent == true) {
				if (primaryLeftORRight.equals("Both")) {
					// Left is always "greater than"
					// So, left will give start key

					Expression left = ((AndExpression) a).getLeftExpression();
					Expression right = ((AndExpression) a).getRightExpression();
					if (left instanceof GreaterThan) {
						if (primaryKeyToDataMap.get(((GreaterThan) left).getLeftExpression().toString()) == null) {
							primaryKeyToDataMap.put(((GreaterThan) left).getLeftExpression().toString(), null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThan) left).getLeftExpression().toString();

					}

					else if (left instanceof GreaterThanEquals) {
						if (primaryKeyToDataMap
								.get(((GreaterThanEquals) left).getLeftExpression().toString()) == null) {
							primaryKeyToDataMap.put(((GreaterThanEquals) left).getLeftExpression().toString(), null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThan) left).getLeftExpression().toString();

					}

					if (right instanceof MinorThan) {
						if (primaryKeyToDataMap.get(((MinorThan) right).getRightExpression().toString()) == null) {
							primaryKeyToDataMap.put(((MinorThan) right).getRightExpression().toString(), null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThan) right).getRightExpression().toString();
					} else if (right instanceof MinorThanEquals) {
						if (primaryKeyToDataMap
								.get(((MinorThanEquals) right).getRightExpression().toString()) == null) {
							primaryKeyToDataMap.put(((MinorThanEquals) right).getRightExpression().toString(), null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThanEquals) right).getRightExpression().toString();
					}
				} else if (primaryLeftORRight.equals("Left")) {
					// left is alwyas greater than
					if (a instanceof GreaterThan) {
						if (primaryKeyToDataMap.get(((GreaterThan) a).getLeftExpression().toString()) == null) {
							primaryKeyToDataMap.put(((GreaterThan) a).getLeftExpression().toString(), null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThan) a).getLeftExpression().toString();
					}

					else if (a instanceof GreaterThanEquals) {
						if (primaryKeyToDataMap.get(((GreaterThanEquals) a).getLeftExpression().toString()) == null) {
							primaryKeyToDataMap.put(((GreaterThanEquals) a).getLeftExpression().toString(), null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThanEquals) a).getLeftExpression().toString();
					}
				} else if (primaryLeftORRight.equals("Right")) {

					if (a instanceof MinorThan) {
						if (primaryKeyToDataMap.get(((MinorThan) a).getRightExpression().toString()) == null) {
							primaryKeyToDataMap.put(((MinorThan) a).getRightExpression().toString(), null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThan) a).getRightExpression().toString();
					}

					else if (a instanceof MinorThanEquals) {
						if (primaryKeyToDataMap.get(((MinorThanEquals) a).getRightExpression().toString()) == null) {
							primaryKeyToDataMap.put(((MinorThanEquals) a).getRightExpression().toString(), null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThanEquals) a).getRightExpression().toString();
						;
					}
				}
				break;
			}

			else if (indexPresent == true) {

				/*
				 * if(indexLeftORRight.equals("Both")) { //Left is always
				 * greater than //So, left will give start key
				 * 
				 * Expression left = ((AndExpression)a).getLeftExpression();
				 * Expression right = ((AndExpression)a).getRightExpression();
				 * if((left instanceof GreaterThan)||(left instanceof
				 * GreaterThan)) { if(primaryKeyToDataMap.get(((GreaterThan)
				 * left).getLeftExpression())==null) {
				 * primaryKeyToDataMap.put(((GreaterThan)
				 * left).getLeftExpression().toString(), null);
				 * IsStartkeyaddedValue=true; startkeyaddedValue=((GreaterThan)
				 * a).getLeftExpression().toString(); }
				 * 
				 * }
				 * 
				 * if((right instanceof MinorThanEquals)||(right instanceof
				 * MinorThan)) { if(primaryKeyToDataMap.get(((MinorThan)
				 * left).getLeftExpression())==null) {
				 * primaryKeyToDataMap.put(((MinorThan)
				 * left).getLeftExpression().toString(), null);
				 * IsEndkeyadded=true; endkeyaddedValue=((MinorThan)
				 * a).getLeftExpression().toString(); }
				 * 
				 * }
				 * 
				 * } else if(primaryLeftORRight.equals("Left")) { if
				 * (e.eval(whereExpression).toBool()) {
				 * 
				 * if((a instanceof GreaterThan)||(a instanceof GreaterThan)) {
				 * if(primaryKeyToDataMap.get(((GreaterThan)
				 * a).getLeftExpression())==null) {
				 * primaryKeyToDataMap.put(((GreaterThan)
				 * a).getLeftExpression().toString(), null);
				 * IsStartkeyaddedValue=true; startkeyaddedValue=((GreaterThan)
				 * a).getLeftExpression().toString(); }
				 * 
				 * }
				 * 
				 * else if((a instanceof MinorThanEquals)||(a instanceof
				 * MinorThan)) { if(primaryKeyToDataMap.get(((MinorThan)
				 * a).getLeftExpression())==null) {
				 * primaryKeyToDataMap.put(((MinorThan)
				 * a).getLeftExpression().toString(), null); IsEndkeyadded=true;
				 * endkeyaddedValue=((MinorThan)
				 * a).getLeftExpression().toString(); } }
				 * 
				 * 
				 * } } else if(primaryLeftORRight.equals("Both")) { if
				 * (e.eval(whereExpression).toBool()) {
				 * 
				 * }
				 * 
				 * }
				 */
				break;
			}

			// Main Logic End
		}

	}

	public void getSelectedColumns(String tableName, PlainSelect plain, Expression whereExpression)
			throws IOException, InvalidPrimitive, SQLException {

		long usedMB = 0;
		try {
			int line_number = 0;
			int condition_met_counter = 0;

			//// Fetching all files///
			File dir = new File(csvFile);

			ArrayList<String> indexValue = new ArrayList<String>();
			
			File[] foundFiles = dir.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return name.startsWith(tableName + "_");
				}
			});
			

			 for (File file : foundFiles) {
				 indexValue.add(file.getName().split("_")[2].split("\\.")[1]);
			 }

			/////////////////////////

			usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
			String csvFile_local_copy = csvFile + tableName + ".csv";
			resultSet = new ArrayList<String[]>();
			if (plain == null) // non-nested query case
			{
				plain = this.plain;
			}

			String[] row = null;
			Map<String, ArrayList<String>> groupByStringMap = new HashMap<String, ArrayList<String>>();
			Map<String, ArrayList<Double>> groupByMap = new HashMap<String, ArrayList<Double>>();
			Map<String, ArrayList<Integer>> groupByMapDenominators = new HashMap<String, ArrayList<Integer>>();
			EvalLib e = new EvalLib(tableName);
			List<Column> groupByColumns = plain.getGroupByColumnReferences();
			ArrayList<Expression> selectlist = new ArrayList<Expression>();
			List<OrderByElement> orderByElements = plain.getOrderByElements();
			double[] aggrMap = new double[10];
			String[] aggrStrMap = new String[10];
			int[] aggrDenomMap = new int[10];
			List<SelectItem> selectItems = plain.getSelectItems();
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			sb = new StringBuilder();
			double count_All = 0;
			int innerCount = 0;
			Function item = null;
			Expression operand = null;
			boolean isStarPresent = false;
			boolean isAggregate = false;
			validrows_backup = new HashMap<Integer, Boolean>();
			boolean count_star_present = false;

			ArrayList<Expression> originalSelectList = new ArrayList<Expression>();
			for (SelectItem select : selectItems) {
				if (select.toString().equals("*")) {
					isStarPresent = true;
					break;
				}

				Expression expression = ((SelectExpressionItem) select).getExpression();

				if (expression instanceof Function) {
					isAggregate = true;
					selectlist.add(((Function) expression));
				} else {
					selectlist.add(expression);
				}
				originalSelectList.add(((SelectExpressionItem) select).getExpression());
			}

			PrimitiveValue result = null;
			boolean whereclauseabsent = (plain.getWhere() == null) ? true : false;

			ArrayList<String> avoidDuplicates = new ArrayList<String>();
			ArrayList<Expression> l = new ArrayList<Expression>();
			ArrayList<Integer> indicesToRemove = new ArrayList<Integer>();
			// if (groupByColumns != null) {
			// for (int idx = 0; idx < originalSelectList.size(); idx++) {
			// // duplicateMap.put(selectlist.get(2).toString(),false);
			// String t = selectlist.get(idx).toString();
			// if (selectlist.get(idx) instanceof Function) {
			// if (originalSelectList.get(idx).toString().contains("AVG(") &&
			// originalSelectList.toString()
			// .contains(originalSelectList.get(idx).toString().replace("AVG(",
			// "SUM("))) {
			// indicesToRemove.add(idx);
			// } else if
			// (originalSelectList.get(idx).toString().contains("SUM(") &&
			// originalSelectList.toString()
			// .contains(originalSelectList.get(idx).toString().replace("SUM(",
			// "AVG("))) {
			// } else if
			// (originalSelectList.get(idx).toString().contains("SUM(") &&
			// originalSelectList.toString()
			// .contains(originalSelectList.get(idx).toString().replace("SUM(",
			// "COUNT("))) {
			// } else if
			// (originalSelectList.get(idx).toString().contains("COUNT(") &&
			// originalSelectList
			// .toString().contains(originalSelectList.get(idx).toString().replace("COUNT(",
			// "SUM("))) {
			// indicesToRemove.add(idx);
			// } else if
			// (originalSelectList.get(idx).toString().contains("COUNT(*)")) {
			// indicesToRemove.add(idx);
			// }
			// }
			// }
			// }

			Collections.sort(indicesToRemove, Collections.reverseOrder());
		//	for (int i : indicesToRemove) {
			//	selectlist.remove(i);
			//}
			List<String> selectElementsString = new ArrayList<String>();
			for (Expression s : selectlist) {
				if (s instanceof Column) {
					selectElementsString.add(((Column) s).getColumnName());
				}
			}

			if (orderByElements != null) {
				for (OrderByElement o : orderByElements) {
					if (o.getExpression() instanceof Column) {
						if (!selectElementsString.contains(((Column) o.getExpression()).getColumnName())) {
							selectlist.add(o.getExpression());
							// selectItems.add((SelectItem).getExpression());
						}
					}

				}
			}

			boolean alreadyPrinted = false;
			boolean simplePrint = false;
			int index = 0;
			int groupRowCount = 0;
			String temp = "";

			String groupKey = "";
			if (isStarPresent && whereclauseabsent) {

				if (plain.getLimit() == null) {
					if (nestedquery) {
						return;
					}
					sb.append(String.join(System.getProperty("line.separator"),
							Files.readAllLines(Paths.get(csvFile_local_copy))));
				}

				else {
					String line = null;
					line_number = 0;
					condition_met_counter = 0;
					if (orderByElements == null) {
						returnStartEnd(tableName, whereExpression);
						Object Start = "";
						if (StartKey == null) {
							Start = primaryKeyToDataMap.firstKey();

						} else {
							// System.out.println(StartKey);
							Start = StartKey;
						}
						for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMap).tailMap(Start).keySet()) {

							// for(Object key : primaryKeyToDataMap.keySet())
							// {
							if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {

								if (!nestedquery) {
									sb.append(String.join("|", (String[]) primaryKeyToDataMap.get(key)));
									sb.append("\n");
								}

								validrows_backup.put(line_number, true);
								line_number++;
								condition_met_counter++;
								if (condition_met_counter == plain.getLimit().getRowCount()) {
									break;
								}
							} else {
								line_number++;
							}

							if ((EndKey != null) && (key.toString().equals(EndKey))) {
								break;
							}

						}
						if (StartKeyInserted != null) {
							primaryKeyToDataMap.remove(StartKey);
						}
						if (EndKeyInserted != null) {
							primaryKeyToDataMap.remove(EndKey);
						}
					} else {

						returnStartEnd(tableName, whereExpression);
						Object Start = "";
						if (StartKey == null) {
							Start = primaryKeyToDataMap.firstKey();

						} else {
							Start = StartKey;
						}
						for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMap).tailMap(Start).keySet()) {

							// for(Object key : primaryKeyToDataMap.keySet())
							// {
							// resultSet.add(new ArrayList<Object>());
							// (String[])primaryKeyToDataMap.get(key)
							// sb.append(String.join("|",
							// (String[])primaryKeyToDataMap.get(key)));
							// sb.append("\n");
							if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
								if (!nestedquery) {
									resultSet.add((String[]) primaryKeyToDataMap.get(key));
								}

								validrows_backup.put(line_number, true);
								line_number++;
								condition_met_counter++;
								if (condition_met_counter == plain.getLimit().getRowCount()) {
									break;
								}
							} else {
								line_number++;
							}

							if ((EndKey != null) && (key.toString().equals(EndKey))) {
								break;
							}
						}
						if (StartKeyInserted != null) {
							primaryKeyToDataMap.remove(StartKey);
						}
						if (EndKeyInserted != null) {
							primaryKeyToDataMap.remove(EndKey);
						}
					}

					if (nestedquery) {
						validrows = validrows_backup;
						return;
					}
					// sb.setLength(sb.length() - 1);
				}
			} else if (whereclauseabsent && !isAggregate) {

				if (plain.getLimit() == null && nestedquery == true) {
					return;
				}

				line_number = 0;

				if (orderByElements == null) {
					returnStartEnd(tableName, whereExpression);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMap.firstKey();

					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMap).tailMap(Start).keySet()) {

						// for(Object key : primaryKeyToDataMap.keySet())
						// {
						if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
							validrows_backup.put(line_number, true);
						} else {
							line_number++;
							continue;
						}

						line_number++;
						rowData = (String[]) primaryKeyToDataMap.get(key);
						for (int i = 0; i < selectlist.size(); i++) {
							result = e.eval(((SelectExpressionItem) selectlist.get(i)).getExpression());
							sb.append(result.toRawString().concat("|"));
						}
						sb.setLength(sb.length() - 1);
						sb.append("\n");
						if (plain.getLimit() != null) {
							if (line_number == plain.getLimit().getRowCount()) {
								break;
							}
						}
						if ((EndKey != null) && (key.toString().equals(EndKey))) {
							break;
						}
					}
					if (StartKeyInserted != null) {
						primaryKeyToDataMap.remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMap.remove(EndKey);
					}
				} else {
					returnStartEnd(tableName, whereExpression);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMap.firstKey();
					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMap).tailMap(Start).keySet()) {

						// for(Object key : primaryKeyToDataMap.keySet())
						// {
						if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
							validrows_backup.put(line_number, true);
						} else {
							line_number++;
							continue;
						}

						line_number++;
						row = new String[columnNameToIndexMapping.get(tableName).size()];
						rowData = (String[]) primaryKeyToDataMap.get(key);
						for (Expression s : selectlist) {
							result = e.eval(s);
							// se.setExpression(s);
							row[columnNameToIndexMapping.get(tableName).get(((Column) s).getColumnName())] = result
									.toRawString();
							// result = e.eval(((SelectExpressionItem)
							// selectItems.get(i)).getExpression());
							// sb.append(result.toRawString().concat("|"));
						}
						resultSet.add(row);
						// sb.setLength(sb.length() - 1);
						// sb.append("\n");
						if (plain.getLimit() != null) {
							if (line_number == plain.getLimit().getRowCount()) {
								break;
							}
						}
						if ((EndKey != null) && (key.toString().equals(EndKey))) {
							break;
						}
					}
					if (StartKeyInserted != null) {
						primaryKeyToDataMap.remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMap.remove(EndKey);
					}
				}

				if (nestedquery) {
					validrows = validrows_backup;
					return;
				}
			} else if (isStarPresent && !whereclauseabsent) {

				line_number = 0;
				condition_met_counter = 0;

				if (orderByElements == null) {
					returnStartEnd(tableName, whereExpression);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMap.firstKey();

					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMap).tailMap(Start).keySet()) {

						// for(Object key : primaryKeyToDataMap.keySet())
						// {
						if (validrows.get(line_number) == null) {
							line_number++;
							continue;
						}

						rowData = (String[]) primaryKeyToDataMap.get(key);

						if (e.eval(whereExpression).toBool()) {

							condition_met_counter++;
							if (!nestedquery) {
								sb.append(String.join("|", (String[]) primaryKeyToDataMap.get(key)));
								sb.append("\n");
							}
							validrows_backup.put(line_number, true);
							line_number++;
							if (plain.getLimit() != null && condition_met_counter == plain.getLimit().getRowCount()) {
								break;
							}
						} else {
							line_number++;
						}

						if ((EndKey != null) && (key.toString().equals(EndKey))) {
							break;
						}
					}
					if (StartKeyInserted != null) {
						primaryKeyToDataMap.remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMap.remove(EndKey);
					}
				} else {

					returnStartEnd(tableName, whereExpression);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMap.firstKey();

					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMap).tailMap(Start).keySet()) {

						// for(Object key : primaryKeyToDataMap.keySet())
						// {
						if (validrows.get(line_number) == null) {
							line_number++;
							continue;
						}

						rowData = (String[]) primaryKeyToDataMap.get(key);

						if (e.eval(whereExpression).toBool()) {

							condition_met_counter++;
							if (!nestedquery) {
								resultSet.add(rowData);
								// sb.append(String.join("|",
								// (String[])primaryKeyToDataMap.get(key)));
								// sb.append("\n");
							}

							validrows_backup.put(line_number, true);
							line_number++;
							if (plain.getLimit() != null && condition_met_counter == plain.getLimit().getRowCount()) {
								break;
							}
						} else {
							line_number++;
						}

						if ((EndKey != null) && (key.toString().equals(EndKey))) {
							break;
						}
					}

					if (StartKeyInserted != null) {
						primaryKeyToDataMap.remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMap.remove(EndKey);
					}
				}

				if (nestedquery == true) {
					validrows = validrows_backup;
					return;
				}
			} 
			 
			else if (groupByColumns == null && !whereclauseabsent) {

				simplePrint = true;
				int lineNum = 0;
				String line32 = "";
				if (isAggregate) {
						for (int i = 0; i < originalSelectList.size(); i++) {
							String columname = "";
							// Foo
							if (originalSelectList.get(i) instanceof Function) {
								if (originalSelectList.get(i).toString().contains("(*)")) {
									count_star_present = true;
									columname = "COUNT_A";
								} else {
									columname = ((Function) originalSelectList.get(i)).getParameters().getExpressions()
											.get(0).toString();

								}
								item = (Function) originalSelectList.get(i);
								String aggregateFunction = item.getName();
								if (aggregateHistory.get(tableName) == null) {
									// aggregateHistory.get(tableName).putIfAbsent(columname,
									// null);
									Map<String, Double> agg = new HashMap<String, Double>();
									agg.put("SUM", 0.0);
									agg.put("MAX", Double.MIN_VALUE);
									agg.put("MIN", Double.MAX_VALUE);
									agg.put("AVG", 0.0);
									agg.put("COUNT", 0.0);
									Map<String, Map<String, Double>> col = new HashMap<String, Map<String, Double>>();
									col.put(columname, agg);
									aggregateHistory.put(tableName, col);

								} else if (aggregateHistory.get(tableName).get(columname) == null) {

									Map<String, Double> agg = new HashMap<String, Double>();
									agg.put("SUM", 0.0);
									agg.put("MAX", Double.MIN_VALUE);
									agg.put("MIN", Double.MAX_VALUE);
									agg.put("AVG", 0.0);
									agg.put("COUNT", 0.0);

									aggregateHistory.get(tableName).put(columname, agg);
								}

								else if (aggregateHistory.get(tableName).get(columname)
										.get(aggregateFunction) != null) {
									indicesToRemove.add(i);
								}

								if (aggregateHistory.get(tableName).get("COUNT_A") == null) {
									Map<String, Double> agg = new HashMap<String, Double>();
									agg.put("COUNT", 0.0);
									aggregateHistory.get(tableName).put("COUNT_A", agg);
								}

							} else {
								columname = ((Column) originalSelectList.get(i)).getColumnName();
							}

						}

						Collections.sort(indicesToRemove, Collections.reverseOrder());
				//		for (int i : indicesToRemove) {
					//		selectlist.remove(i);
						//}

						avoidDuplicates = new ArrayList<String>();
						l = new ArrayList<Expression>();

						// Remove Duplicates
						for (int idx = 0; idx < selectlist.size(); idx++) {
							if (avoidDuplicates.indexOf(selectlist.get(idx).toString()) == -1) {
								avoidDuplicates.add(selectlist.get(idx).toString());
								l.add(selectlist.get(idx));
							}
						}

						if (l.size() > 1) {
							for (int idx = 0; idx < l.size(); idx++) {
								if (l.get(idx).toString().contains("(*)")) {
									l.remove(idx);
									break;
								}
							}
						}
						selectlist = l;
					}

					for (int i = 0; i < selectlist.size(); i++) {

						if (selectlist.get(i) instanceof Function) {
							if (((Function) selectlist.get(i)).getName().equals("MAX")) {
								aggrMap[i] = Double.MIN_VALUE;
							}

							else if (((Function) selectlist.get(i)).getName().equals("MIN")) {
								aggrMap[i] = Double.MAX_VALUE;
							}
						}

					}

					if (whereExpression instanceof AndExpression) {
						if (((AndExpression) whereExpression) != null) {

							// int expCount =
							// whereExpression.toString().split("AND").length;
							Expression a = ((AndExpression) whereExpression);
							while (((AndExpression) a).getLeftExpression() instanceof AndExpression) {
								// if((AndExpression)a).getLeftExpression()
								// instanceof (AndExpression))
								a = (AndExpression) ((AndExpression) a).getLeftExpression();
							}
							// indexToPrmryKeyMap.keySet().toArray(a)

							//rowData = new String[columnNameToIndexMapping.get(tableName).size()];

							String final_s = null;
							
							for (String s : indexNameToColumnsMap.keySet()) {
								if (s.contains("INDEX")) {

									if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())
											&& a.toString().split(((AndExpression) a).getStringExpression())[1]
													.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
										// System.out.println("both expressions
										// contain index column");
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
										// System.out.println("left expression
										// contain index column");
										a = ((AndExpression) a).getLeftExpression();
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
											.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
										// System.out.println("right expression
										// contain index column");
										a = ((AndExpression) a).getRightExpression();
										final_s = s;
										break;
									}
								} else if (false)// (s.contains("PRIMARY"))
								{
									if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())
											&& a.toString().split(((AndExpression) a).getStringExpression())[1]
													.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
										// System.out.println("both expressions
										// contain index column");
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
										// System.out.println("left expression
										// contain index column");
										a = ((AndExpression) a).getLeftExpression();
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
											.contains(indexNameToColumnsMap.get(s).get(0).toUpperCase())) {
										// System.out.println("right expression
										// contain index column");
										a = ((AndExpression) a).getRightExpression();
										final_s = s;
										break;
									}
								}
							}

							
							
							// returnStartEnd(tableName,a,whereExpression);
							if ((final_s != null) && (a.toString()
									.contains(indexNameToColumnsMap.get(final_s).get(0).toUpperCase()))) {
								condition_met_counter = 0;
								boolean breakAll = false;
								String[] name = null; 
								
								ArrayList<String> validFiles = new ArrayList<String>();
								for (int i = 0; i < foundFiles.length; i++) {

									name = foundFiles[i].getName().split("_");
									rowData[columnNameToIndexMapping.get(tableName).get(name[1])] = name[2].split("\\.")[0];
									if (e.eval(a).toBool()) {
										validFiles.add(foundFiles[i].getPath());
									}
								}
								
								for(String fileName:validFiles)
								{
									br.close();
									br=new BufferedReader(new FileReader(new File(fileName)));
									while((line=br.readLine())!=null){
										split(line);
										if (e.eval(whereExpression).toBool()) {
											condition_met_counter++;
											// System.out.println(rowData);
											if (!isAggregate) // If there is
																// no
																// Aggregate
																// Statement
																// in Query
											{
												if (orderByElements != null) {
													row = new String[columnNameToIndexMapping.get(tableName)
															.size()];

													for (Expression s : selectlist) {
														// Instance of
														// column & not an
														// expression
														result = e.eval(s);
														row[columnNameToIndexMapping.get(tableName)
																.get(((Column) s).getColumnName())] = result
																		.toRawString();
													}

													resultSet.add(row);
													/*if ((plain.getLimit() != null)
															&& condition_met_counter == plain.getLimit().getRowCount()) {
														breakAll = true;
														break;
													}*/
												} else {
													for (int i = 0; i < selectItems.size() - 1; i++) {
														result = e.eval(((SelectExpressionItem) selectItems.get(i))
																.getExpression());
														sb.append(result.toRawString().concat("|"));
													}
													result = e.eval(((SelectExpressionItem) selectItems
															.get(selectItems.size() - 1)).getExpression());
													sb.append(result.toRawString() + "\n");
													if ((plain.getLimit() != null)
															&& condition_met_counter == plain.getLimit().getRowCount()) {
														breakAll = true;
														break;
													}
												}

											} else // If its aggregate Query
											{
												for (int i = 0; i < selectlist.size(); i++) {
													if (selectlist.get(i) instanceof Function) {
														item = (Function) selectlist.get(i);
														switch (item.getName().charAt(2)) {
														case 'X': // If Max
															operand = (Expression) item.getParameters()
																	.getExpressions().get(0);
															result = e.eval(operand);
															if (aggrMap[i] < result.toDouble()) {
																aggrMap[i] = result.toDouble();
															}

															break;
														case 'N': // If
																	// Minimum
															operand = (Expression) item.getParameters()
																	.getExpressions().get(0);
															result = e.eval(operand);
															if (result.toDouble() < aggrMap[i]) {
																aggrMap[i] = result.toDouble();
															}

															break;
														case 'M': // if Sum
															operand = (Expression) item.getParameters()
																	.getExpressions().get(0);
															result = e.eval(operand);
															// test.add(e.eval(operand).toDouble());
															// innerCount++;
															if (result != null) {
																aggrMap[i] += result.toDouble();
															}
															break;
														case 'U': // if
																	// Count
															if (!item.toString().toLowerCase()
																	.contains("count(*)")) {
																operand = (Expression) item.getParameters()
																		.getExpressions().get(0);
																result = e.eval(operand);
																if (result != null) {
																	aggrMap[i] += 1;
																}
															}
															break;
														case 'G': // If
																	// Average
															operand = (Expression) item.getParameters()
																	.getExpressions().get(0);
															result = e.eval(operand);
															if (result != null) {
																aggrMap[i] += result.toDouble();
																aggrDenomMap[i] += 1;
															}
															break;
														default:
															break;
														}
													} else {// If the Column
															// is not an
															// aggregate
															// column then
															// simply get
															// the value
														operand = selectlist.get(i);
														if (aggrStrMap[i] == null) {
															aggrStrMap[i] = e.eval(operand).toRawString();
														}
													}
												}
											}
										}
									}
									if(breakAll)
									{
										break;
									}
								}
								
								/*for (Object o : indexToPrmryKeyMap
										.get(indexNameToColumnsMap.get(final_s).get(0).toUpperCase()).keySet()) {

									rowData[columnNameToIndexMapping.get(tableName).get(
											indexNameToColumnsMap.get(final_s).get(0).toUpperCase())] = o.toString();

									if (e.eval(a).toBool()) {
										TreeSet<Object> li = indexToPrmryKeyMap
												.get(indexNameToColumnsMap.get(final_s).get(0).toUpperCase()).get(o);
										for (Object data : li) {
											count_All++;
											rowData = (String[]) primaryKeyToDataMap.get(data);
											
											
										}
									}
									if (breakAll) {
										break;
									}
								}*/

								// -----------------

								if (isAggregate && (condition_met_counter == 0) && (!count_star_present)) {
									return;
								} else if ((condition_met_counter == 0) && count_star_present) {
									for (int i = 0; i < originalSelectList.size(); i++) {
										item = (Function) originalSelectList.get(i);
										if (originalSelectList.get(i) instanceof Function) {
											String column = "COUNT_A";
											if (item.toString().toLowerCase().equals("count(*)")) {
												sb.append(aggregateHistory.get(tableName).get(column).get("COUNT")
														.longValue() + "|");
											} else {
												sb.append("|");
											}
										}
									}
									if (sb.length() >= 2) {

										sb.setLength(sb.length() - 1);
										sb.append("\n");
									}
									System.out.print(sb);
									return;
								}

								// -----------------------------------------

							} else if (isAggregate == true) // Where expression
															// doesnot contain
															// any Index columnn
							{
								//System.out.println("aggregate");
								
								/*simplePrint = false;
								alreadyPrinted = true;
								OnDiskAggregate agg = new OnDiskAggregate();
								agg.setColumnDataTypes(columnDataTypes);
								agg.setColumnNameToIndexMapping(columnNameToIndexMapping);
								agg.setPlain(plain);
								agg.getSelectedColumns(tableName, null, primaryKeyToDataMap.size(), whereExpression);*/
							} else if (whereExpression instanceof EqualsTo) {
								// Foobar
								returnStartEnd(tableName, whereExpression);
								return;
							} else {

								getSelectedColumnsOld(tableName, plain, whereExpression, selectItems);
								// System.out.println("Invalid query : No
								// aggregate and Where expression doesnot
								// contain any Index columnn");
							}
						}
					} else {// if it has no AND in where clause
						if (nestedquery) {
							getSelectedColumnsOld(tableName, plain, whereExpression, selectItems);
						} else {

							condition_met_counter = 0;
							boolean breakAll = false;
							String[] name = null; 
							//rowData = new String[columnNameToIndexMapping.get(tableName).size()];
							ArrayList<String> validFiles = new ArrayList<String>();
							for (int i = 0; i < foundFiles.length; i++) {

								name = foundFiles[i].getName().split("_");
								rowData[columnNameToIndexMapping.get(tableName).get(name[1])] = name[2].split("\\.")[0];
								if (e.eval(whereExpression).toBool()) {
									validFiles.add(foundFiles[i].getPath());
								}
							}
							
							for(String fileName:validFiles)
							{
								br.close();
								br=new BufferedReader(new FileReader(new File(fileName)));
								while((line=br.readLine())!=null){
									split(line);
									if (e.eval(whereExpression).toBool()) {
										
										condition_met_counter++;
										// System.out.println(rowData);
										if (!isAggregate) // If there is
															// no
															// Aggregate
															// Statement
															// in Query
										{
											if (orderByElements != null) {
												row = new String[columnNameToIndexMapping.get(tableName)
														.size()];

												for (Expression s : selectlist) {
													// Instance of
													// column & not an
													// expression
													result = e.eval(s);
													row[columnNameToIndexMapping.get(tableName)
															.get(((Column) s).getColumnName())] = result
																	.toRawString();
												}

												resultSet.add(row);
												if ((plain.getLimit() != null)
														&& condition_met_counter == plain.getLimit().getRowCount()) {
													breakAll = true;
													break;
												}
											} else {
												for (int i = 0; i < selectItems.size() - 1; i++) {
													result = e.eval(((SelectExpressionItem) selectItems.get(i))
															.getExpression());
													sb.append(result.toRawString().concat("|"));
												}
												result = e.eval(((SelectExpressionItem) selectItems
														.get(selectItems.size() - 1)).getExpression());
												sb.append(result.toRawString() + "\n");
												if ((plain.getLimit() != null)
														&& condition_met_counter == plain.getLimit().getRowCount()) {
													breakAll = true;
													break;
												}
											}

										} else // If its aggregate Query
										{
											for (int i = 0; i < selectlist.size(); i++) {
												if (selectlist.get(i) instanceof Function) {
													item = (Function) selectlist.get(i);
													switch (item.getName().charAt(2)) {
													case 'X': // If Max
														operand = (Expression) item.getParameters()
																.getExpressions().get(0);
														result = e.eval(operand);
														if (aggrMap[i] < result.toDouble()) {
															aggrMap[i] = result.toDouble();
														}

														break;
													case 'N': // If
																// Minimum
														operand = (Expression) item.getParameters()
																.getExpressions().get(0);
														result = e.eval(operand);
														if (result.toDouble() < aggrMap[i]) {
															aggrMap[i] = result.toDouble();
														}

														break;
													case 'M': // if Sum
														operand = (Expression) item.getParameters()
																.getExpressions().get(0);
														result = e.eval(operand);
														// test.add(e.eval(operand).toDouble());
														// innerCount++;
														if (result != null) {
															aggrMap[i] += result.toDouble();
														}
														break;
													case 'U': // if
																// Count
														if (!item.toString().toLowerCase()
																.contains("count(*)")) {
															operand = (Expression) item.getParameters()
																	.getExpressions().get(0);
															result = e.eval(operand);
															if (result != null) {
																aggrMap[i] += 1;
															}
														}
														break;
													case 'G': // If
																// Average
														operand = (Expression) item.getParameters()
																.getExpressions().get(0);
														result = e.eval(operand);
														if (result != null) {
															aggrMap[i] += result.toDouble();
															aggrDenomMap[i] += 1;
														}
														break;
													default:
														break;
													}
												} else {// If the Column
														// is not an
														// aggregate
														// column then
														// simply get
														// the value
													operand = selectlist.get(i);
													if (aggrStrMap[i] == null) {
														aggrStrMap[i] = e.eval(operand).toRawString();
													}
												}
											}
										}
									}
								}
								
							}
							
							/*for (Object o : indexToPrmryKeyMap
									.get(indexNameToColumnsMap.get(final_s).get(0).toUpperCase()).keySet()) {

								rowData[columnNameToIndexMapping.get(tableName).get(
										indexNameToColumnsMap.get(final_s).get(0).toUpperCase())] = o.toString();

								if (e.eval(a).toBool()) {
									TreeSet<Object> li = indexToPrmryKeyMap
											.get(indexNameToColumnsMap.get(final_s).get(0).toUpperCase()).get(o);
									for (Object data : li) {
										count_All++;
										rowData = (String[]) primaryKeyToDataMap.get(data);
										
										
									}
								}
								if (breakAll) {
									break;
								}
							}*/

							// -----------------

							if (isAggregate && (condition_met_counter == 0) && (!count_star_present)) {
								return;
							} else if ((condition_met_counter == 0) && count_star_present) {
								for (int i = 0; i < originalSelectList.size(); i++) {
									item = (Function) originalSelectList.get(i);
									if (originalSelectList.get(i) instanceof Function) {
										String column = "COUNT_A";
										if (item.toString().toLowerCase().equals("count(*)")) {
											sb.append(aggregateHistory.get(tableName).get(column).get("COUNT")
													.longValue() + "|");
										} else {
											sb.append("|");
										}
									}
								}
								if (sb.length() >= 2) {

									sb.setLength(sb.length() - 1);
									sb.append("\n");
								}
								System.out.print(sb);
								return;
							}

							// -----------------------------------------

						
						}
					}

//				} else {
//
//				}
				if (nestedquery) {
					//validrows = validrows_backup;
					return;
				}
			} else if (groupByColumns != null && !whereclauseabsent) {// if
																		// group
																		// by is
																		// present
																		// and
																		// where
																		// is
																		// present
				// Timer t = new Timer();
				Date d = new Date();

				// while ((line = br.readLine()) != null) {
				
				// File dir = new File(csvFile);
				// File[]
				foundFiles = dir.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						return name.startsWith(tableName + "_");
					}
				});

				//rowData = new String[columnNameToIndexMapping.get(tableName).size()];
				String[] name = null;

				ArrayList<String> validFiles = new ArrayList<String>();
				for (int i = 0; i < foundFiles.length; i++) {

					name = foundFiles[i].getName().split("_");
					rowData[columnNameToIndexMapping.get(tableName).get(name[1])] = name[2].split("\\.")[0];
					if (e.eval(whereExpression).toBool()) {
						validFiles.add(foundFiles[i].getPath());
					}
				}

				//returnStartEnd(tableName, whereExpression);
				Object Start = "";
//				if (StartKey == null) {
//					Start = primaryKeyToDataMap.firstKey();
//
//				} else {
//					Start = StartKey;
//				}
				// for (Object key : ((TreeMap<Object, Object>)
				// primaryKeyToDataMap).tailMap(Start).keySet()) {
				int fileNumber = 0;
				int rowCount = 0;
				for (String fileName : validFiles) {
					fileNumber++;
					
					br.close();
					br = new BufferedReader(new FileReader(new File(fileName)));
					
					while ((line = br.readLine()) != null) {
						rowCount++;
						split(line);

						/// for(Object key : primaryKeyToDataMap.keySet())
						// {
//						if (validrows.get(line_number) == null) {
//							line_number++;
//							continue;
//						}

						groupKey = "";
						if (e.eval(whereExpression).toBool()) {

							//validrows_backup.put(line_number, true);
							line_number++;
							condition_met_counter++;
							// System.out.println(lineCounter);
							if (groupByColumns != null) {
								for (int i = 0; i < groupByColumns.size() - 1; i++) {
									groupKey += e.eval(groupByColumns.get(i)).toRawString() + "|";
								}
								groupKey += e.eval(groupByColumns.get(groupByColumns.size() - 1)).toRawString();
								if (groupByMap.get(groupKey) == null) {
									groupByMapDenominators.put(groupKey, new ArrayList<Integer>());
									groupByMap.put(groupKey, new ArrayList<Double>());
									groupByStringMap.put(groupKey, new ArrayList<String>());
									for (int i = 0; i < selectlist.size(); i++) {
										groupByMap.get(groupKey).add(0.0);
										groupByMapDenominators.get(groupKey).add(0);
										groupByStringMap.get(groupKey).add("");
									}
								}
							}
							// Key Should be generated according to group order
							// by

							// if(e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression()))
							if (!isAggregate && (groupByColumns == null)) {
								for (int i = 0; i < selectItems.size(); i++) {

									result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
									sb.append(result.toRawString().concat("|"));
								}
								sb.setLength(sb.length() - 1);
								sb.append("\n");
								if (plain.getLimit() != null
										&& condition_met_counter == plain.getLimit().getRowCount()) {
									break;
								}
							} else {

								for (int i = 0; i < selectlist.size(); i++) {
									Function func = null;
									if (selectlist.get(i) instanceof Function) {
										func = (Function) selectlist.get(i);
										switch (func.getName().charAt(2)) {
										case 'G':// AVG
											result = e.eval(func.getParameters().getExpressions().get(0));
											if (result != null) {
												groupByMap.get(groupKey).set(i,
														groupByMap.get(groupKey).get(i) + result.toDouble());
												groupByMapDenominators.get(groupKey).set(i,
														groupByMapDenominators.get(groupKey).get(i) + 1);
											}
											break;
										case 'M':// SUM
											result = e.eval(func.getParameters().getExpressions().get(0));
											if (result != null) {
												groupByMap.get(groupKey).set(i,
														groupByMap.get(groupKey).get(i) + result.toDouble());
											}
											break;
										case 'U':// COUNT
											if (func.toString().contains("(*)")) {
												groupByMap.get(groupKey).set(i, groupByMap.get(groupKey).get(i) + 1);
											} else {
												operand = (Expression) func.getParameters().getExpressions().get(0);
												result = e.eval(operand);
												// index
												// =columnNameToIndexMapping.get(tableName).get(operand.toString());
												temp = result.toRawString();// record.get(index);//(rowData[index]);
												if (temp.trim().length() != 0) {
													// result = e.eval(operand);
													if (e.eval(operand) != null) {
														groupByMap.get(groupKey).set(i,
																groupByMap.get(groupKey).get(i) + 1);
													}
												}
											}
											break;
										case 'N':// MIN
											operand = (Expression) func.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											// index
											// =columnNameToIndexMapping.get(tableName).get(operand.toString());
											temp = result.toRawString();// record.get(index);//(rowData[index]);
											if (temp.trim().length() != 0) {
												// result = e.eval(operand);
												if (result.toDouble() < groupByMap.get(groupKey).get(i)) {
													groupByMap.get(groupKey).set(i, result.toDouble());
												}
											}
											break;
										case 'X':// MAX
											operand = (Expression) func.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											// index
											// =columnNameToIndexMapping.get(tableName).get(operand.toString());
											temp = result.toRawString();// record.get(index);//(rowData[index]);
											if (temp.trim().length() != 0) {
												// result = e.eval(operand);
												if (result.toDouble() > groupByMap.get(groupKey).get(i)) {
													groupByMap.get(groupKey).set(i, result.toDouble());
												}
											}
											break;
										default:
											break;
										}

									} else {// If the Column is not an aggregate
											// column
										// then simply get the value
										operand = selectlist.get(i);
										if (groupByStringMap.get(groupKey).get(i).trim().length() == 0) {
											groupByStringMap.get(groupKey).set(i, e.eval(operand).toRawString());
										}
									}
								}
							}

							// result =
							// e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
							// sb.append(result.toRawString()+"\n");

						} else {
							line_number++;
						}

//						if ((EndKey != null) && (key.toString().equals(EndKey))) {
//							break;
//						}
					}
				}

//				if (StartKeyInserted != null) {
//					primaryKeyToDataMap.remove(StartKey);
//				}
//				if (EndKeyInserted != null) {
//					primaryKeyToDataMap.remove(EndKey);
//				}

				if (nestedquery) {
					validrows = validrows_backup;
					return;
				}
				d = new Date();
				// System.out.println("time to execute gropu by = "+
				// (hrs1-hrs)+":"+(min1-min)+":"+(sec1-sec));
			} else {
				System.out.println("Invalid Query");
			}

			if (!alreadyPrinted && isAggregate && (groupByColumns == null)) {
				// Order by implementation//
				if (orderByElements != null && orderByElements.size() > 0) {
					LinkedHashMap<Integer, String> columnIndicesAndDirection = new LinkedHashMap<Integer, String>();
					for (OrderByElement colName : orderByElements) {
						columnIndicesAndDirection.put(
								columnNameToIndexMapping.get(tableName)
										.get(((Column) colName.getExpression()).getColumnName()),
								(colName.isAsc() ? "ASC" : "DESC"));
					}
					orderElements(tableName, columnIndicesAndDirection);
				}

				if (selectlist.size() != 0) {
					aggregateHistory.get(tableName).get("COUNT_A").replace("COUNT", (double)condition_met_counter);
				}
				// bar
				item = null;
				String columnName = "";
				for (int i = 0; i < selectlist.size(); i++) {
					if (selectlist.get(i) instanceof Function) {
						if (selectlist.get(i).toString().contains("(*)")) {
							count_star_present = true;
							columnName = "COUNT_A";
						} else {
							columnName = ((Function) selectlist.get(i)).getParameters().getExpressions().get(0)
									.toString();

						}
						item = (Function) selectlist.get(i);

						if ((item.getName().equals("SUM") || item.getName().equals("MIN")
								|| item.getName().equals("COUNT") || item.getName().equals("MAX"))
								&& !item.toString().toLowerCase().equals("count(*)")) {
							aggregateHistory.get(tableName).get(columnName).replace(item.getName(), aggrMap[i]);

							// System.out.println(aggregateHistory.get(tableName));
						} else if (item.getName().equals("AVG")) {
							aggregateHistory.get(tableName).get(columnName).replace(item.getName(),
									(aggrMap[i] / aggrDenomMap[i]));
							aggregateHistory.get(tableName).get(columnName).replace("SUM", aggrMap[i]);
							aggregateHistory.get(tableName).get(columnName).replace("COUNT", (double) aggrDenomMap[i]);
						}

					} else {// if its a Simple String Column or Simple Number
							// Column
							// Double elsecase =
							// Double.parseDouble(aggrStrMap[i]);
							// aggregateHistory.get(tableName).get(columnName).replace(item.getName(),
							// elsecase);
					}
				}

				if (nestedquery) {
					return;
				}

				// Read the Data from hashMap and Print the output on Console
				int local_counter = 0;

				if (aggregateHistory.get(tableName).get("COUNT_A").get("COUNT") < 1
						&& !originalSelectList.toString().contains("COUNT(*)")) {
					// if count is 0 and count(*) is not present then print
					// blank
					sb.append(" ");
				}

				else if (aggregateHistory.get(tableName).get("COUNT_A").get("COUNT") < 1
						&& originalSelectList.toString().contains("COUNT(*)")) {
					// if count is 0 and COUNT(*) is present then print
					// COUNT(*)|null|null
					for (int i = 0; i < originalSelectList.size(); i++) {

						columnName = "";
						if (originalSelectList.get(i) instanceof Function) {
							if (originalSelectList.get(i).toString().contains("(*)")) {
								count_star_present = true;
								columnName = "COUNT_A";
							} else {
								columnName = ((Function) originalSelectList.get(i)).getParameters().getExpressions()
										.get(0).toString();

							}
							item = (Function) originalSelectList.get(i);

							if (item.toString().toLowerCase().equals("count(*)")) {
								count_star_present = true;
								sb.append(
										aggregateHistory.get(tableName).get(columnName).get("COUNT").longValue() + "|");
							} else if (item.getName().equalsIgnoreCase("SUM") || item.getName().equalsIgnoreCase("MIN")
									|| item.getName().equalsIgnoreCase("COUNT")
									|| item.getName().equalsIgnoreCase("MAX")) {
								index = 0;
								if (columnNameToIndexMapping.get(tableName).get(columnName) == null) {
									index = -1;
									sb.append("|");
								} else {
									index = columnNameToIndexMapping.get(tableName).get(columnName);
									// String type =
									// columnDataTypes.get(tableName).get(index);
									// Double tempVal=
									// aggregateHistory.get(tableName).get(columnName).get(item.getName());
									sb.append("|");
								}

							} else if (item.getName().equalsIgnoreCase("AVG")) {
								sb.append("|");
							}

						} else {// if its a Simple String Column or Simple
								// Number Column

							for (int j = local_counter; j < aggrStrMap.length; j++) {
								if (aggrStrMap[j] != null) {
									local_counter = j;
									sb.append(aggrStrMap[local_counter] + "|");
									local_counter++;
									break;
								}
							}
						}
					}
				} else {
					// if count > 0 and count(*) is absent behave normally
					// count > 0 and count(*) is present behave normally
					for (int i = 0; i < originalSelectList.size(); i++) {

						columnName = "";
						if (originalSelectList.get(i) instanceof Function) {
							if (originalSelectList.get(i).toString().contains("(*)")) {
								count_star_present = true;
								columnName = "COUNT_A";
							} else {
								columnName = ((Function) originalSelectList.get(i)).getParameters().getExpressions()
										.get(0).toString();

							}
							item = (Function) originalSelectList.get(i);

							if (item.toString().toLowerCase().equals("count(*)")) {
								count_star_present = true;
								sb.append(
										aggregateHistory.get(tableName).get(columnName).get("COUNT").longValue() + "|");
							} else if (item.getName().equalsIgnoreCase("SUM") || item.getName().equalsIgnoreCase("MIN")
									|| item.getName().equalsIgnoreCase("COUNT")
									|| item.getName().equalsIgnoreCase("MAX")) {
								index = 0;
								if (columnNameToIndexMapping.get(tableName).get(columnName) == null) {
									index = -1;
									sb.append(
											aggregateHistory.get(tableName).get(columnName).get(item.getName()) + "|");
								} else {
									index = columnNameToIndexMapping.get(tableName).get(columnName);
									String type = columnDataTypes.get(tableName).get(index);
									Double tempVal = aggregateHistory.get(tableName).get(columnName)
											.get(item.getName());
									sb.append((type.equals("long") || type.equals("int")) ? tempVal.longValue() + "|"
											: temp + "|");
								}

							} else if (item.getName().equalsIgnoreCase("AVG")) {
								sb.append(aggregateHistory.get(tableName).get(columnName).get(item.getName()) + "|");
							}
						} else {// if its a Simple String Column or Simple
								// Number Column

							for (int j = local_counter; j < aggrStrMap.length; j++) {
								if (aggrStrMap[j] != null) {
									local_counter = j;
									sb.append(aggrStrMap[local_counter] + "|");
									local_counter++;
									break;
								}
							}
						}
					}
				}

				sb.setLength(sb.length() - 1);
				sb.append("\n");
			} else if (!alreadyPrinted && isAggregate && groupByColumns != null) {
				// for orderby implement logic // later

				List<String> keyList = new ArrayList<String>(groupByMap.keySet());
				Collections.sort(keyList);
				//
				for (String outputGroupKey : keyList) {

					for (int i = 0; i < selectlist.size(); i++) {
						item = null;
						if (selectlist.get(i) instanceof Function) {
							item = (Function) selectlist.get(i);
							switch (item.getName()) {
							case "SUM":
							case "MIN":
							case "MAX":
								sb.append((result instanceof LongValue)
										? ((groupByMap.get(outputGroupKey).get(i).longValue()) + "|")
										: groupByMap.get(outputGroupKey).get(i) + "|");
								break;
							case "count":
							case "COUNT":
								sb.append(groupByMap.get(outputGroupKey).get(i).longValue() + "|");
								break;
							case "AVG":
								sb.append((groupByMap.get(outputGroupKey).get(i)
										/ groupByMapDenominators.get(outputGroupKey).get(i)) + "|");
								break;
							default:
								break;
							}
						} else {// if its a Simple String Column or Simple
								// Number
							// Column
							sb.append(groupByStringMap.get(outputGroupKey).get(i) + "|");
						}
					}
					sb.setLength(sb.length() - 1);
					sb.append("\n");
				}
				if (sb.length() >= 2) {

					sb.setLength(sb.length() - 1);
					sb.append("\n");
				}
			} else {
				if (orderByElements != null && orderByElements.size() > 0) {
					LinkedHashMap<Integer, String> columnIndicesAndDirection = new LinkedHashMap<Integer, String>();
					for (OrderByElement colName : orderByElements) {
						columnIndicesAndDirection.put(
								columnNameToIndexMapping.get(tableName)
										.get(((Column) colName.getExpression()).getColumnName()),
								(colName.isAsc() ? "ASC" : "DESC"));
					}
					orderElements(tableName, columnIndicesAndDirection);

					if (!nestedquery) {
						int counter=0;
						for (String[] resultRow : resultSet) {
							counter++;
							if (originalSelectList.size() == 0) {
								sb.append(String.join("|", resultRow)).append("|");
							} else {
								for (Expression ex : originalSelectList) {
									if (ex instanceof Column) {
										sb.append(resultRow[columnNameToIndexMapping.get(tableName)
												.get(((Column) ex).getColumnName())]).append("|");
									}
								}
							}

							if(plain.getLimit() != null && counter==plain.getLimit().getRowCount())
							{
								sb.setLength(sb.length() - 1);
								sb.append("\n");
								break;
							}
							else
							{
							
							sb.setLength(sb.length() - 1);
							sb.append("\n");
							}
						}
					}
				}

			}
			// System.out.println(groupByMap);
			// sb.setLength(sb.length() - 1);
			System.out.print(sb.toString());// to print normal queries
			System.out.println();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	// This will sort max 2 columns simultaneously for now!
	public void orderElements(String tableName, LinkedHashMap<Integer, String> columnIndicesAndDirection) {
		List<String> dataType = new ArrayList<String>();
		String type = "";
		int colNumber = -1;// for multicolumn sorting;
		ArrayList<Integer> colIndexes = new ArrayList<Integer>();
		boolean isAsc = false;

		for (Integer index : columnIndicesAndDirection.keySet()) {
			colNumber++;
			type = columnDataTypes.get(tableName).get(index);
			isAsc = (columnIndicesAndDirection.get(index).equals("ASC")) ? true : false;
			colIndexes.add(index);
			switch (type) {
			case "INT":
			case "DOUBLE":
				if (colIndexes.size() == 1) {
					Collections.sort(resultSet, new Comparator<String[]>() {
						@Override
						public int compare(String[] o1, String[] o2) {
							// TODO Auto-generated method stub
							if (Double.compare(Double.parseDouble(o1[index]), Double.parseDouble(o2[index])) < 0) {
								if ((columnIndicesAndDirection.get(index).equals("ASC")))
									return -1;
								else
									return 1;
							} else if (Double.compare(Double.parseDouble(o1[index]),
									Double.parseDouble(o2[index])) > 0) {
								if ((columnIndicesAndDirection.get(index).equals("ASC")))
									return 1;
								else
									return -1;
							} else {
								return 0;
							}

						}
					});
				} else if (colIndexes.size() == 2) {

					Collections.sort(resultSet, new Comparator<String[]>() {
						@Override
						public int compare(String[] o1, String[] o2) {
							// TODO Auto-generated method stub
							int c = -1;
							if (Double.compare(Double.parseDouble(o1[colIndexes.get(0)]),
									Double.parseDouble(o2[colIndexes.get(0)])) < 0) {
								if ((columnIndicesAndDirection.get(colIndexes.get(0)).equals("ASC")))
									c = -1;
								else
									c = 1;
							} else if (Double.compare(Double.parseDouble(o1[colIndexes.get(0)]),
									Double.parseDouble(o2[colIndexes.get(0)])) > 0) {
								if ((columnIndicesAndDirection.get(colIndexes.get(0)).equals("ASC")))
									c = 1;
								else
									c = -1;
							} else {
								c = 0;
							}

							if (c == 0) {
								if (Double.compare(Double.parseDouble(o1[index]), Double.parseDouble(o2[index])) < 0) {
									if ((columnIndicesAndDirection.get(index).equals("ASC")))
										return -1;
									else
										return 1;
								} else if (Double.compare(Double.parseDouble(o1[index]),
										Double.parseDouble(o2[index])) > 0) {
									if ((columnIndicesAndDirection.get(index).equals("ASC")))
										return 1;
									else
										return -1;
								} else {
									return 0;
								}
							} else {
								return c;
							}

						}
					});
				}
				break;
			case "DATE":
				Collections.sort(resultSet, new Comparator<String[]>() {
					@SuppressWarnings("deprecation")
					@Override
					public int compare(String[] o1, String[] o2) {
						// TODO Auto-generated method stub
						if (colIndexes.size() == 1) {
							if (new Date((new DateValue(o1[index])).getYear(), (new DateValue(o1[index])).getMonth(),
									(new DateValue(o1[index])).getDate())
											.before(new Date((new DateValue(o2[index])).getYear(),
													(new DateValue(o2[index])).getMonth(),
													(new DateValue(o2[index])).getDate()))) {
								if ((columnIndicesAndDirection.get(index).equals("ASC")))
									return -1;
								else
									return 1;
							} else if (new Date((new DateValue(o1[index])).getYear(),
									(new DateValue(o1[index])).getMonth(), (new DateValue(o1[index])).getDate())
											.after(new Date((new DateValue(o2[index])).getYear(),
													(new DateValue(o2[index])).getMonth(),
													(new DateValue(o2[index])).getDate()))) {
								if ((columnIndicesAndDirection.get(index).equals("ASC")))
									return 1;
								else
									return -1;
							} else {
								return 0;
							}
						} else if (colIndexes.size() == 2) {
							int c = -1;
							if (new Date((new DateValue(o1[colIndexes.get(0)])).getYear(),
									(new DateValue(o1[colIndexes.get(0)])).getMonth(),
									(new DateValue(o1[colIndexes.get(0)])).getDate())
											.before(new Date((new DateValue(o2[colIndexes.get(0)])).getYear(),
													(new DateValue(o2[colIndexes.get(0)])).getMonth(),
													(new DateValue(o2[colIndexes.get(0)])).getDate()))) {
								if ((columnIndicesAndDirection.get(colIndexes.get(0)).equals("ASC")))
									c = -1;
								else
									c = 1;
							} else if (new Date((new DateValue(o1[colIndexes.get(0)])).getYear(),
									(new DateValue(o1[colIndexes.get(0)])).getMonth(),
									(new DateValue(o1[colIndexes.get(0)])).getDate())
											.after(new Date((new DateValue(o2[colIndexes.get(0)])).getYear(),
													(new DateValue(o2[colIndexes.get(0)])).getMonth(),
													(new DateValue(o2[colIndexes.get(0)])).getDate()))) {
								if ((columnIndicesAndDirection.get(colIndexes.get(0)).equals("ASC")))
									c = 1;
								else
									c = -1;
							} else {
								c = 0;
							}

							if (c == 0) {
								if (new Date((new DateValue(o1[index])).getYear(),
										(new DateValue(o1[index])).getMonth(), (new DateValue(o1[index])).getDate())
												.before(new Date((new DateValue(o2[index])).getYear(),
														(new DateValue(o2[index])).getMonth(),
														(new DateValue(o2[index])).getDate()))) {
									if ((columnIndicesAndDirection.get(index).equals("ASC")))
										return -1;
									else
										return 1;
								} else if (new Date((new DateValue(o1[index])).getYear(),
										(new DateValue(o1[index])).getMonth(), (new DateValue(o1[index])).getDate())
												.after(new Date((new DateValue(o2[index])).getYear(),
														(new DateValue(o2[index])).getMonth(),
														(new DateValue(o2[index])).getDate()))) {
									if ((columnIndicesAndDirection.get(index).equals("ASC")))
										return 1;
									else
										return -1;
								} else {
									return 0;
								}
							}
							return c;
						}

						return 0;
					}
				});
			}
			// if(columnIndicesAndDirection.get(index)=="DESC"){
			// Collections.reverseOrder();
			// }
		}
	}

	public void getSelectedColumnsOld(String tableName, PlainSelect plain, Expression whereExpression,
			List<SelectItem> selectItems) {
		try {

			
			if (plain == null) {
				plain = this.plain;
			}
			EvalLib e = new EvalLib(tableName);
			boolean whereclauseabsent = (plain.getWhere() == null) ? true : false;
			PrimitiveValue result = null;
			boolean is_aggregate = false; // hardcode

			boolean whereexpressionresult = true;
			int condition_met_counter = 0;
			int line_number = 0;
			for (Object key : primaryKeyToDataMap.keySet()) {
				rowData = (String[]) primaryKeyToDataMap.get(key);
				if (!whereclauseabsent && validrows.get(line_number) == false) {
					line_number++;
					continue;
				}

				if (whereclauseabsent && validrows.get(line_number) == true) {
					validrows_backup.put(line_number, true);
				}

				if (whereExpression != null) {
					whereexpressionresult = e.eval(whereExpression).toBool();
					if (whereexpressionresult == false) {
						line_number++;
						continue;
					} else {
						validrows_backup.put(line_number, true);
					}
				}

				line_number++;
				if (whereclauseabsent || whereexpressionresult) {
					condition_met_counter++;
					if (is_aggregate == false) // If there is no Aggregate
												// Statement in Query
					{
						for (int i = 0; i < selectItems.size() - 1; i++) {
							result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
							sb.append(result.toRawString().concat("|"));
						}
						result = e
								.eval(((SelectExpressionItem) selectItems.get(selectItems.size() - 1)).getExpression());
						sb.append(result.toRawString() + "\n");

						if ((plain.getLimit() != null) && (condition_met_counter == plain.getLimit().getRowCount())) {
							break;
						}
					} else // If its aggregate Query
					{
						System.out.println("Invalid Query");
					}
				}
			}

			if (nestedquery) {
				validrows = validrows_backup;
				return;
			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());

		}
	}


	private void split(String line) {
        int idxComma, idxToken = 0, fromIndex = 0;
        while ((idxComma = line.indexOf(delimiter, fromIndex)) != -1) {
            rowData[idxToken++] = line.substring(fromIndex, idxComma);
            fromIndex = idxComma + 1;
        }
        rowData[idxToken] = line.substring(fromIndex);
        
        //rowData = split(line);
    }
	public void scanLinesOnDisk() throws Exception {

		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		String query = "";
		while ((temp = scan.nextLine()) != null) {
			query += temp + " ";
			if (temp.indexOf(';') >= 0) {
				readQueries(query);
				parseQueries();
				System.out.print("$>");
				query = "";
			}

		}
		scan.close();

	}

	class EvalLib extends Eval {
		String tableName = "";
		boolean isIndexed = false;

		private EvalLib(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			if (!isIndexed) {
				int index = columnNameToIndexMapping.get(tableName).get(col.getColumnName());
				switch (columnDataTypes.get(tableName).get(index).toLowerCase()) {
				case "string":
				case "varchar":
				case "char":
					return new StringValue(rowData[index]);
				// return new StringValue(record.get(index));
				case "int":
					return new LongValue(rowData[index]);
				// return new LongValue(record.get(index));
				case "decimal":
					return new DoubleValue(rowData[index]);
				// return new DoubleValue(record.get(index));
				case "date":
					return new DateValue(rowData[index]);
				// return new DateValue(record.get(index));
				default:
					return new StringValue(rowData[index]);
				// return new StringValue(record.get(index));
				}
			} else {
				if (indexNameToColumnsMap.get("INDEX").get(0).contains(col.getColumnName())) {
					// indexToPrmryKeyMap.keySet().toArray(a);

				}
				return new StringValue("");
			}
		}
	}
}