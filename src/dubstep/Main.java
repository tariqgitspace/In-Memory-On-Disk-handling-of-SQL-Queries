package dubstep;

import net.sf.jsqlparser.eval.Eval; 
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.lang.reflect.Array;
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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.IntStream;
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
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.lang.instrument.Instrumentation;



public class Main {

	final char delimiter = '|';
	private Map<String, List<Expression>> whereClauseTree = null;
	boolean debug = false;
	private ArrayList<String> joinHistory = new ArrayList<String>();
	// String[] splitted = new String[64];
	private Map<String, Map<String, Map<String, Double>>> aggregateHistory = new HashMap<String, Map<String, Map<String, Double>>>();
	private static Scanner scan;
	private String[] rowData = new String[32];
	private BufferedReader br = null;
	private Runtime rt = Runtime.getRuntime();
	private static String csvFile = "src\\dubstep\\data\\";
	//public static String csvFile = "data/";
	private String line = "";
	private Map<String, HashMap<String, List<String>>> indexNameToColumnsMap = new HashMap<String,HashMap<String, List<String>>>();
	private Statement statement;
	private PlainSelect plain;
	private Map<String, ArrayList<String>> columnDataTypes = new HashMap<String, ArrayList<String>>();
	private Map<String, Map<String, Integer>> columnNameToIndexMapping = new HashMap<String, Map<String, Integer>>();
	private Select select;
	private SelectBody body;
	private CCJSqlParser parser;
	// private Map<Integer, HashMap<Object, List<Object>>> indexToPrmryKeyMapOfGivenTable =
	// new HashMap<Integer,HashMap<Object,List<Object>>>();

	private ArrayList<ArrayList<String>> FinalJoinedData = new ArrayList<ArrayList<String>>();
	private Table globaltable;
	private Map<String, TreeMap<Object, TreeSet<Object>>> indexToPrmryKeyMapOfGivenTable = new HashMap<String, TreeMap<Object, TreeSet<Object>>>();
	private Map<String, TreeMap<Object, Object>> primaryKeyToDataMapOfGivenTable = new HashMap<String, TreeMap<Object, Object>>();
	private Map<String, TreeMap<Object, byte[]>> primaryKeyToDataMapByte = new HashMap<String, TreeMap<Object, byte[]>>();
	private TreeMap<Object, Object> individualPrimaryKeyToDataMap = new TreeMap<Object, Object>();
	private TreeMap<String, byte[]> individualPrimaryKeyToDataMapBytes = new TreeMap<String, byte[]>();
	private TreeMap<String, String> aliases = new TreeMap<String, String>();
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

	private Map<String, ArrayList<String[]>> FilteredRows = null;

	public void readQueries(String temp) throws ParseException {
		StringReader input = new StringReader(temp);
		parser = new CCJSqlParser(input);
		statement = parser.Statement();
	}

	public void parseQueries() throws Exception {

		while (statement != null) {
			if (statement instanceof CreateTable) {
				// primaryKeyToDataMapOfGivenTable=null;
				individualPrimaryKeyToDataMap = null;
				//indexNameToColumnsMap = null;
				indexToPrmryKeyMapOfGivenTable = null;
				validrows = new HashMap<Integer, Boolean>();

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
			// System.out.println(plain);
			Table table = null;
			if (plain.getFromItem() instanceof SubSelect) {
				nestedquery_loopnumber = 0;
				nestedquery = true;
				getAllNestedSubQueriesFrom(plain);
				nestedquery_loopnumber = 0;

			} else {// if(plain.getFromItem() instanceof Table){
				globaltable = (Table) plain.getFromItem();
				globaltableName = globaltable.getName();
				/*for (int i = 0; i < primaryKeyToDataMapOfGivenTable.get(globaltable).size(); i++) {
					validrows.put(i, true);
				}*/

				// Arrays.fill(validrows,true);
				if (plain.getJoins() != null) {
					whereClauseTree = new HashMap<String, List<Expression>>();
					joinPreprocessing();
					getJoinOutput(plain);
					return;
					
				}
				//System.out.println("going to fetch select data");
				//getSelectedColumns(globaltableName, null, plain.getWhere());

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
				// System.out.println("outermost loop :"+nestedplain);
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
			for (int i = 0; i < primaryKeyToDataMapOfGivenTable.get(globaltableName).size(); i++) {
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

		try {
			
			indexToPrmryKeyMapOfGivenTable = new HashMap<String, TreeMap<Object, TreeSet<Object>>>();
			individualPrimaryKeyToDataMap = new TreeMap<Object, Object>();
			individualPrimaryKeyToDataMapBytes = new TreeMap<String, byte[]>();
			
			
			CreateTable create = (CreateTable) statement;
			// int[] indexColNum = new
			// int[create.getColumnDefinitions().size()];
			List<ColumnDefinition> colDefinitions = create.getColumnDefinitions();
			colDefinitions.get(0).getColumnSpecStrings();
			indexNameToColumnsMap.put(tableName,new HashMap<String, List<String>>());
			for (Index i : create.getIndexes()) {
				if (i.getName() == null && i.getColumnsNames().size() != 0) {
					
					indexNameToColumnsMap.get(tableName).put(i.getType(), i.getColumnsNames());
				} else {
					indexNameToColumnsMap.get(tableName).put(i.getType() + "_" + String.join("_", i.getColumnsNames()),
							i.getColumnsNames());
					// indexNameToColumnsMap.put(i.getColumnsNames().get(0),i.getColumnsNames());
				}
			}
			for (ColumnDefinition colDef : colDefinitions) {
				if (colDef.getColumnSpecStrings() != null) {

					indexNameToColumnsMap.get(tableName).put("INDEX_" + colDef.getColumnSpecStrings().get(1),
							new ArrayList<String>(Arrays.asList(colDef.getColumnName())));
				}
			}
			if (indexNameToColumnsMap != null) {
				Iterator it = indexNameToColumnsMap.get(tableName).keySet().iterator();
				String s = "";
				while (it.hasNext()) {
					s = (String)it.next();
					if (indexNameToColumnsMap.get(tableName).keySet().contains("PRIMARY KEY") && s.contains("INDEX")) {
						// System.out.println("Building Indexes..");
						if (s.contains("INDEX") && (indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s)) == null)) {
							indexToPrmryKeyMapOfGivenTable.put(indexNameToColumnsMap.get(tableName).get(s).get(0),
									new TreeMap<Object, TreeSet<Object>>());
						}
					}
				}
			}
			long usedMB = 0;
			Runtime.getRuntime().freeMemory();
			Runtime.getRuntime().gc();
			//System.out.println("Before starting the process!!");
			//usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
			//System.out.println(usedMB);
			int index = 0, pmKeyIndexCol1=0, pmKeyIndexCol2=0;
			if (!debug) {

				if (indexNameToColumnsMap != null) {
					br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
					//System.out.println("Processing Indexes..");
					usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
					//System.out.println(usedMB);
					while ((line = br.readLine()) != null) {
						split(line);
						String[] r = new String[columnNameToIndexMapping.get(tableName).size()];
						System.arraycopy(rowData, 0, r, 0, r.length);
						for (String s : indexNameToColumnsMap.get(tableName).keySet()) {
							// Dont change this

							index = columnNameToIndexMapping.get(tableName)
									.get(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase());
							pmKeyIndexCol1 = columnNameToIndexMapping.get(tableName)
									.get(indexNameToColumnsMap.get(tableName).get("PRIMARY KEY").get(0).toUpperCase());

							pmKeyIndexCol2 = 0;
							if (indexNameToColumnsMap.get(tableName).get("PRIMARY KEY").size() == 2) {
								pmKeyIndexCol2 = columnNameToIndexMapping.get(tableName)
										.get(indexNameToColumnsMap.get(tableName).get("PRIMARY KEY").get(1).toUpperCase());
							}

							if (s.contains("INDEX")) {
								//indexLogic
								if (rowData[index].length() > 0) {
									if (indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s).get(0))
											.get(rowData[index]) == null) {
										indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s).get(0)).put(rowData[index],
												new TreeSet<Object>());
										if (indexNameToColumnsMap.get(tableName).get("PRIMARY KEY").size() == 1) {
											indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s).get(0))
													.get(rowData[index]).add(rowData[pmKeyIndexCol1]);
										} else {
											indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s).get(0))
													.get(rowData[index])
													.add(rowData[pmKeyIndexCol1] + "," + rowData[pmKeyIndexCol2]);
										}
									} else {
										if (indexNameToColumnsMap.get(tableName).get("PRIMARY KEY").size() == 1) {
											indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s).get(0))
													.get(rowData[index]).add(rowData[pmKeyIndexCol1]);
										} else {
											indexToPrmryKeyMapOfGivenTable.get(indexNameToColumnsMap.get(tableName).get(s).get(0))
													.get(rowData[index])
													.add(rowData[pmKeyIndexCol1] + "," + rowData[pmKeyIndexCol2]);
										}
									}
								}
							} else if (s.contains("PRIMARY")) {

								if (indexNameToColumnsMap.get(tableName).get("PRIMARY KEY").size() == 1) {
									//individualPrimaryKeyToDataMap.put(rowData[pmKeyIndexCol1], r);
									individualPrimaryKeyToDataMapBytes.put(rowData[pmKeyIndexCol1], line.getBytes());
									

								} else {
									individualPrimaryKeyToDataMapBytes
											.put(rowData[pmKeyIndexCol1] + "," + rowData[pmKeyIndexCol2], line.getBytes());
								}

							}
							
						}
						r = null;
					}
				}
				br.close();
				usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
				//System.out.println(usedMB);
				//run gc here
				System.gc();
				Runtime.getRuntime().gc();
				///
//				byte[] tx = individualPrimaryKeyToDataMapBytes.get("1,1");
//				String rw = new String(tx);
				ObjectOutputStream output = new ObjectOutputStream(
						new FileOutputStream(csvFile + tableName + "_Primary.csv"));
				output.writeObject(individualPrimaryKeyToDataMapBytes);
				output.close();
				/*
				for (String idx : indexToPrmryKeyMapOfGivenTable.keySet()) {
					output = new ObjectOutputStream(
							new FileOutputStream(csvFile + tableName + "_INDEX_" + idx + ".csv"));
					output.writeObject(indexToPrmryKeyMapOfGivenTable.get(idx));
					output.close();
				}
				*/
				individualPrimaryKeyToDataMapBytes = null;
				indexToPrmryKeyMapOfGivenTable = null;
				individualPrimaryKeyToDataMap = null;
				
				
				System.gc();
				Runtime.getRuntime().gc();
				
			}
			//System.out.println("Index created on: " + tableName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(ObjectSizeFetcher.getObjectSize(list));
	}

	public void getColumnDataTypesAndMapColumnNameToIndex() throws SQLException {
		Runtime.getRuntime().freeMemory();
		Runtime.getRuntime().gc();
		
		CreateTable create = (CreateTable) statement;
		String tableName = create.getTable().getName();
		Map<String, Integer> columnNameToIndexMap = new HashMap<String, Integer>();
		List<ColumnDefinition> si = create.getColumnDefinitions();
		ListIterator<ColumnDefinition> it = si.listIterator();
		ArrayList<String> dataTypes = new ArrayList<String>();

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

	public void returnStartEnd(String tableName, Expression whereExpression, List<SelectItem> selectItems)
			throws InvalidPrimitive, SQLException {

		boolean primaryIdxPresent = false;
		boolean indexPresent = false;
		String primaryLeftORRight = "";
		String indexLeftORRight = "";
		StartKey = null;
		EndKey = null;
		StartKeyInserted = null;
		EndKeyInserted = null;
		EvalLib e = new EvalLib(tableName);
		PrimitiveValue result;

		if (whereExpression instanceof EqualsTo) {
			if (((EqualsTo) whereExpression).getLeftExpression() instanceof DateValue
					|| ((EqualsTo) whereExpression).getLeftExpression() instanceof StringValue
					|| ((EqualsTo) whereExpression).getLeftExpression() instanceof DoubleValue
					|| ((EqualsTo) whereExpression).getLeftExpression() instanceof LongValue) {
				if (primaryKeyToDataMapOfGivenTable.get(tableName)
						.get((((EqualsTo) whereExpression).getRightExpression()).toString()) != null) {
					// System.out.println("foo right");
					// alreadyPrinted = true;
					rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName)
							.get((((EqualsTo) whereExpression).getLeftExpression()).toString());
					for (int i = 0; i < selectItems.size(); i++) {
						result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
						sb.append(result.toRawString().concat("|"));
					}
					sb.setLength(sb.length() - 1);
					System.out.print(sb.toString());
					return;
					// System.out.println(String.join("|", );
				} else {
					// System.out.println("no foo right");
					sb.append(" ");
					sb.setLength(sb.length() - 1);
					System.out.print(sb.toString());
					return;
				}
			} else if (((EqualsTo) whereExpression).getRightExpression() instanceof DateValue
					|| ((EqualsTo) whereExpression).getRightExpression() instanceof StringValue
					|| ((EqualsTo) whereExpression).getRightExpression() instanceof DoubleValue
					|| ((EqualsTo) whereExpression).getRightExpression() instanceof LongValue) {// if
																								// primary
																								// key
																								// colun
																								// hass
																								// equal
																								// to
																								// data
																								// then
																								// print
																								// the
																								// row
				if (primaryKeyToDataMapOfGivenTable.get(tableName)
						.get((((EqualsTo) whereExpression).getRightExpression()).toString()) != null) {
					// System.out.println("foo right");
					// alreadyPrinted = true;
					rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName)
							.get((((EqualsTo) whereExpression).getRightExpression()).toString());
					for (int i = 0; i < selectItems.size(); i++) {
						result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
						sb.append(result.toRawString().concat("|"));
					}
					sb.setLength(sb.length() - 1);
					System.out.println(sb.toString());
					return;
					// System.out.println(String.join("|", );
				} else {// else print a blank
						// System.out.println("no foo right");
					sb.append(" ");
					sb.setLength(sb.length() - 1);
					System.out.println(sb.toString());
					return;
				}
			}
			// ((EqualsTo)whereExpression).getLeftExpression();
			return;
		}

		if (!(whereExpression instanceof AndExpression)) {
			return;
		}
		Expression a = ((AndExpression) whereExpression);
		e = new EvalLib(tableName);

		// if(((AndExpression)whereExpression)!=null){

		while (((AndExpression) a).getLeftExpression() instanceof AndExpression) {
			a = (AndExpression) ((AndExpression) a).getLeftExpression();
		}
		// indexToPrmryKeyMapOfGivenTable.keySet().toArray(a)

		// rowData = new String[columnNameToIndexMapping.get(tableName).size()];

		String final_s = null;
		for (String s : indexNameToColumnsMap.get(tableName).keySet()) {
			if (s.contains("INDEX")) {

				if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())
						&& a.toString().split(((AndExpression) a).getStringExpression())[1]
								.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
					// System.out.println("both expressions contain index
					// column");
					final_s = s;
					indexPresent = true;
					indexLeftORRight = "Both";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
					// System.out.println("left expression contain index
					// column");
					a = ((AndExpression) a).getLeftExpression();
					final_s = s;
					indexPresent = true;
					indexLeftORRight = "Left";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
						.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
					// System.out.println("right expression contain index
					// column");
					a = ((AndExpression) a).getRightExpression();
					final_s = s;
					indexPresent = true;
					indexLeftORRight = "Right";
					// break;
				}
			} else if (s.contains("PRIMARY")) {
				if (indexNameToColumnsMap.get(tableName).get(s).size() == 2) {
					break;
				}
				if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())
						&& a.toString().split(((AndExpression) a).getStringExpression())[1]
								.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
					// System.out.println("both expressions contain index
					// column");
					final_s = s;
					primaryIdxPresent = true;
					primaryLeftORRight = "Both";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
						.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
					// System.out.println("left expression contain index
					// column");
					a = ((AndExpression) a).getLeftExpression();
					final_s = s;
					primaryIdxPresent = true;
					primaryLeftORRight = "Left";
					// break;
				} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
						.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
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
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((GreaterThan) left).getLeftExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName).put(((GreaterThan) left).getLeftExpression().toString(),
									null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThan) left).getLeftExpression().toString();

					}

					else if (left instanceof GreaterThanEquals) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((GreaterThanEquals) left).getLeftExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName)
									.put(((GreaterThanEquals) left).getLeftExpression().toString(), null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThan) left).getLeftExpression().toString();

					}

					if (right instanceof MinorThan) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((MinorThan) right).getRightExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName).put(((MinorThan) right).getRightExpression().toString(),
									null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThan) right).getRightExpression().toString();
					} else if (right instanceof MinorThanEquals) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((MinorThanEquals) right).getRightExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName)
									.put(((MinorThanEquals) right).getRightExpression().toString(), null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThanEquals) right).getRightExpression().toString();
					}
				} else if (primaryLeftORRight.equals("Left")) {
					// left is alwyas greater than
					if (a instanceof GreaterThan) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((GreaterThan) a).getLeftExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName).put(((GreaterThan) a).getLeftExpression().toString(),
									null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThan) a).getLeftExpression().toString();
					}

					else if (a instanceof GreaterThanEquals) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((GreaterThanEquals) a).getLeftExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName)
									.put(((GreaterThanEquals) a).getLeftExpression().toString(), null);
							StartKeyInserted = true;
						}
						StartKey = ((GreaterThanEquals) a).getLeftExpression().toString();
					}
				} else if (primaryLeftORRight.equals("Right")) {

					if (a instanceof MinorThan) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((MinorThan) a).getRightExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName).put(((MinorThan) a).getRightExpression().toString(),
									null);
							EndKeyInserted = true;

						}
						EndKey = ((MinorThan) a).getRightExpression().toString();
					}

					else if (a instanceof MinorThanEquals) {
						if (primaryKeyToDataMapOfGivenTable.get(tableName)
								.get(((MinorThanEquals) a).getRightExpression().toString()) == null) {
							primaryKeyToDataMapOfGivenTable.get(tableName)
									.put(((MinorThanEquals) a).getRightExpression().toString(), null);
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
				 * GreaterThan)) {
				 * if(primaryKeyToDataMapOfGivenTable.get(tableName).get(((GreaterThan)
				 * left).getLeftExpression())==null) {
				 * primaryKeyToDataMapOfGivenTable.get(tableName).put(((GreaterThan)
				 * left).getLeftExpression().toString(), null);
				 * IsStartkeyaddedValue=true; startkeyaddedValue=((GreaterThan)
				 * a).getLeftExpression().toString(); }
				 * 
				 * }
				 * 
				 * if((right instanceof MinorThanEquals)||(right instanceof
				 * MinorThan)) {
				 * if(primaryKeyToDataMapOfGivenTable.get(tableName).get(((MinorThan)
				 * left).getLeftExpression())==null) {
				 * primaryKeyToDataMapOfGivenTable.get(tableName).put(((MinorThan)
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
				 * if(primaryKeyToDataMapOfGivenTable.get(tableName).get(((GreaterThan)
				 * a).getLeftExpression())==null) {
				 * primaryKeyToDataMapOfGivenTable.get(tableName).put(((GreaterThan)
				 * a).getLeftExpression().toString(), null);
				 * IsStartkeyaddedValue=true; startkeyaddedValue=((GreaterThan)
				 * a).getLeftExpression().toString(); }
				 * 
				 * }
				 * 
				 * else if((a instanceof MinorThanEquals)||(a instanceof
				 * MinorThan)) {
				 * if(primaryKeyToDataMapOfGivenTable.get(tableName).get(((MinorThan)
				 * a).getLeftExpression())==null) {
				 * primaryKeyToDataMapOfGivenTable.get(tableName).put(((MinorThan)
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

	// And expressions ek map me dalna hai
	/*
	 * LINEITEM.SHIPDATE >= DATE('1995-01-01') AND LINEITEM.SHIPDATE < DATE
	 * ('1996-01-01') AND LINEITEM.DISCOUNT > 0.08 AND LINEITEM.DISCOUNT < 0.1
	 * AND LINEITEM.QUANTITY < 25;
	 */
	List<Expression> t = null;

	private List<Expression> andClauses(Expression e) {
		if (e instanceof AndExpression) {
			AndExpression a = (AndExpression) e;
			t = andClauses(a.getLeftExpression());
			t.addAll(andClauses(a.getRightExpression()));
			return t;
		} else {
			List<Expression> l = new ArrayList<Expression>();
			l.add(e);
			return l;
		}
	}

	@SuppressWarnings("unchecked")
	private void joinPreprocessing() throws InvalidPrimitive, SQLException, IOException, ClassNotFoundException {
		primaryKeyToDataMapOfGivenTable = null;
		primaryKeyToDataMapOfGivenTable = new HashMap<String, TreeMap<Object, Object>>();
		primaryKeyToDataMapByte = null;
		primaryKeyToDataMapByte  = new HashMap<String, TreeMap<Object, byte[]>>();
		List<Expression> andClausesList = andClauses(plain.getWhere());
		plain.getSelectItems();
		whereClauseTree.put(plain.getFromItem().toString(), new ArrayList<Expression>());
		joinHistory = null;
		joinHistory = new ArrayList<String>();
		FinalJoinedData = null;
		FinalJoinedData = new ArrayList<ArrayList<String>>();
		
		
		
		//allTables.add(j.getRightItem().toString());
		//IndexToPrimaryKeyMapForAllTables -> When Select Statement: Created for all tables after reading data files
		Map<String, Map<String,TreeMap<Object, TreeSet<Object>>>> IndexToPrimaryKeyMapForAllTables = 
				new HashMap<String, Map<String,TreeMap<Object, TreeSet<Object>>>>();
		Map<String, Boolean> IsdataReadFromDiskForThisTable = null;
		IsdataReadFromDiskForThisTable = new HashMap<String, Boolean>();
		//ArrayList<String> allTables = new ArrayList<String>();
		
				
		if (plain.getJoins() != null) {
			List<Join> joins = plain.getJoins();
			for (Join j : joins) {
				// tableNames.add(j.getRightItem().toString());
				whereClauseTree.put(j.getRightItem().toString(), new ArrayList<Expression>());
				// allTables.add(j.getRightItem().toString());
			}
		}
		int tableCounter = 0;
		
		String tableNameKey = "";
		for (Expression ex : andClausesList) {
			for (String key : whereClauseTree.keySet()) {
				if (ex.toString().contains(key)) {
					tableCounter++;
					if (tableNameKey.length() != 0) {
						tableNameKey += "," + key;
					} else {
						tableNameKey = key;
					}
				}
			}
			if (whereClauseTree.get(tableNameKey) == null) {
				whereClauseTree.put(tableNameKey, new ArrayList<Expression>());
			}
			whereClauseTree.get(tableNameKey).add(ex);
			tableNameKey = "";
			tableCounter = 0;
		}

		int conditionNotMet = 0;
		int totalCounter = 0;
		boolean isRowFiltered = false;

		String pktable=null;
		String fktable=null;
		String fkcolumnName=null;
		
		
		for (String keyTable : whereClauseTree.keySet()) {
			conditionNotMet = 0;
			totalCounter = 0;
		
			if(whereClauseTree.get(keyTable).size()==0)
			{
				continue;
			}
			//If normal condition
			if (!keyTable.contains(",")) {
				EvalLib e = new EvalLib(keyTable);
				IsdataReadFromDiskForThisTable.put(keyTable, true);
				//1. read file from disk
				try {
					//System.out.println("Loading Data for: " + keyTable);
					ObjectInputStream input = new ObjectInputStream(
							new FileInputStream(csvFile + keyTable + "_Primary.csv"));
					primaryKeyToDataMapByte.put(keyTable, (TreeMap<Object, byte[]>)input.readObject());
					input.close();
				} catch (ClassNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				Iterator<Object> it = primaryKeyToDataMapByte.get(keyTable).keySet().iterator();
				Object temp=null;
				
				
				//2. filter rows
				while(it.hasNext()) {
					temp=it.next();
					split(new String(primaryKeyToDataMapByte.get(keyTable).get(temp)));
					totalCounter++;
					for (Expression andcondition : whereClauseTree.get(keyTable)) {
						if (!e.eval(andcondition).toBool()) {
							conditionNotMet ++;
							//isRowFiltered = true;
							it.remove();
							break;
						}
					}
				}
			//System.out.println("Number of rows for Table: " +keyTable+" reduced from totalCounter" +totalCounter+ "  to  "+primaryKeyToDataMapByte.get(keyTable).size());	
			}
		}
		
	
		//4. Load data for files which dont have normal conditions
		//create index for tables on which there is join
		for (String Table : whereClauseTree.keySet()) {
			if (Table.contains(",")) {	
				String[] tables = Table.split(",");
				if(IsdataReadFromDiskForThisTable.get(tables[0])==null)
				{
					try {

						//System.out.println("Loading Data for: " + tables[0]);
						ObjectInputStream input = new ObjectInputStream(
								new FileInputStream(csvFile + tables[0] + "_Primary.csv"));
						primaryKeyToDataMapByte.put(tables[0], (TreeMap<Object, byte[]>)input.readObject());
						input.close();
						IsdataReadFromDiskForThisTable.put(tables[0], true);
					} catch (ClassNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
				if(IsdataReadFromDiskForThisTable.get(tables[1])==null)
				{
					try {
						//System.out.println("Loading Data for: " + tables[1]);
						ObjectInputStream input = new ObjectInputStream(
								new FileInputStream(csvFile + tables[1] + "_Primary.csv"));
						primaryKeyToDataMapByte.put(tables[1], (TreeMap<Object, byte[]>)input.readObject());
						input.close();
						IsdataReadFromDiskForThisTable.put(tables[1], true);
					} catch (ClassNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}			
			}	
		}
		
		
		//3. Build Indexes
		
		HashMap<String, Boolean> IsalreadyIndexed = new HashMap<String, Boolean>();
		for (String keyTable : whereClauseTree.keySet()) {
			if (!keyTable.contains(",")) {
				for (String keyT : whereClauseTree.keySet()) {
					if (keyT.contains(",") && keyT.contains(keyTable) &&(IsalreadyIndexed.get(keyT)==null)) {
						IsalreadyIndexed.put(keyT,true);
						Expression left = null;
						Expression right = null;
						Expression eqEx = whereClauseTree.get(keyT).get(0);
						String[] tables = keyT.split(",");
						if (eqEx instanceof EqualsTo) {
							left = ((EqualsTo) eqEx).getLeftExpression();
							right = ((EqualsTo) eqEx).getRightExpression();
						}
						if (indexNameToColumnsMap.get(tables[0]).get("PRIMARY KEY").size() == 1) {
							if (indexNameToColumnsMap.get(tables[0]).get("PRIMARY KEY").get(0)
									.equals(((Column) left).getColumnName())) {
								
								pktable = tables[0];
								fktable = tables[1];
								fkcolumnName = ((Column) right).getColumnName();
							} else {
								pktable = tables[1];
								fktable = tables[0];
								fkcolumnName = ((Column) left).getColumnName();
							}
						}
						else{
							System.out.println("LineItem table!!");
						}

						int index = columnNameToIndexMapping.get(fktable).get(fkcolumnName);
						indexToPrmryKeyMapOfGivenTable = new HashMap<String, TreeMap<Object, TreeSet<Object>>>();
						if (indexToPrmryKeyMapOfGivenTable.get(fkcolumnName) == null) {
							indexToPrmryKeyMapOfGivenTable.put(fkcolumnName, new TreeMap<Object, TreeSet<Object>>());
						}
						for (Object key : primaryKeyToDataMapByte.get(fktable).keySet()) {
							split(new String(primaryKeyToDataMapByte.get(fktable).get(key)));
							if (indexToPrmryKeyMapOfGivenTable.get(fkcolumnName).get(rowData[index]) != null) {
								indexToPrmryKeyMapOfGivenTable.get(fkcolumnName).get(rowData[index]).add(key);
							} else {
								indexToPrmryKeyMapOfGivenTable.get(fkcolumnName).put(rowData[index],
										new TreeSet<Object>());
								indexToPrmryKeyMapOfGivenTable.get(fkcolumnName).get(rowData[index]).add(key);
							}
						}
						IndexToPrimaryKeyMapForAllTables.put(fktable, indexToPrmryKeyMapOfGivenTable);
					}
				}
			}
		}
		
		//Index building finished
		

		
		
		
		//TablesToJoin -> Find SortedOrder Of Joins
		Map<Integer,List<String>> TablesToJoin = new TreeMap<Integer,List<String>>();
		for (String keyTable : whereClauseTree.keySet()) {
			if (keyTable.contains(",")) {
				Expression left = null;
				Expression right = null;
				Expression eqEx = whereClauseTree.get(keyTable).get(0);			
				String[] tables = keyTable.split(",");
				if (eqEx instanceof EqualsTo) {
					left = ((EqualsTo) eqEx).getLeftExpression();
					right = ((EqualsTo) eqEx).getRightExpression();
				}
				if (indexNameToColumnsMap.get(tables[0]).get("PRIMARY KEY").size() == 1) {
					if (indexNameToColumnsMap.get(tables[0]).get("PRIMARY KEY").get(0)
							.equals(((Column) left).getColumnName())) {
						fkcolumnName = ((Column) right).getColumnName();
					} else {
						fkcolumnName = ((Column) left).getColumnName();
					}
				}

				left = null;
				right = null;
				eqEx = whereClauseTree.get(keyTable).get(0);
				if (eqEx instanceof EqualsTo) {
					left = ((EqualsTo) eqEx).getLeftExpression();
					right = ((EqualsTo) eqEx).getRightExpression();
				}
				tables = keyTable.split(",");
	
				if(indexNameToColumnsMap.get(tables[0]).get("PRIMARY KEY").size()==1)
				{		
					if(indexNameToColumnsMap.get(tables[0]).get("PRIMARY KEY").get(0).equals(((Column)left).getColumnName()))
					{
						pktable = tables[0];
						fktable = tables[1];
						fkcolumnName= ((Column)right).getColumnName();
					}
					else
					{
						fktable = tables[0];
						pktable = tables[1];
						fkcolumnName= ((Column)left).getColumnName();
					}
			
				}
				
				
				Integer joinsize =0;
				try{
				joinsize = primaryKeyToDataMapByte.get(pktable).keySet().size() * (IndexToPrimaryKeyMapForAllTables.get(fktable).get(fkcolumnName).keySet().size());
				}
				catch(Exception ex){
					System.out.println("Error " + pktable +" "+ fktable+ "  "+statement);
				}
				TablesToJoin.put(joinsize, new ArrayList<String>());
				TablesToJoin.get(joinsize).add(pktable);
				TablesToJoin.get(joinsize).add(fktable);
				TablesToJoin.get(joinsize).add(fkcolumnName);
			}
		}


		// Join the tables--begin
		ArrayList<ArrayList<String>> joinedData = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> PingPongjoinedData = new ArrayList<ArrayList<String>>();
		
		

		//Iterate from higher Join Size to Lower Join size
		
		try
		{
		for (Integer joinsize : TablesToJoin.keySet()) {
			ArrayList<String> pkfkName = (ArrayList<String>) TablesToJoin.get(joinsize);
			
			//If first Join
			if(joinedData.size()==0){
				int joinRowCount=0;
				joinHistory.add(pkfkName.get(0));
				joinHistory.add(pkfkName.get(1));
				
				//Iterate over PKtable
				
				for (Object data0 : primaryKeyToDataMapByte.get(pkfkName.get(0)).keySet()) {
					//Iterate Over "index to primary" Table with indexe column = fkcolumnName [pkfkName.get(2)]
					for (Object data1 : IndexToPrimaryKeyMapForAllTables.get(pkfkName.get(1)).get(pkfkName.get(2)).keySet()) {

						if (data0.equals(data1)) {

							//Add all primary keys for this foreign Key
							for (Object pkOfFK : IndexToPrimaryKeyMapForAllTables.get(pkfkName.get(1)).get(pkfkName.get(2)).get(data1)) {
								joinedData.add(new ArrayList<String>());
								joinedData.get(joinRowCount).add((String) data0);
								joinedData.get(joinRowCount).add((String) pkOfFK);
								joinRowCount += 1;
							}
							// System.out.println("emit row");
						}
					}
				}
				
				
				//System.out.println("first join-> pk: "+pkfkName.get(0)+", fk: "+ pkfkName.get(1));
			}
			else{  //Join for more than 2 tables
				int src_counter = 0,dest_counter=0;;
				
				if(joinHistory.contains(pkfkName.get(1)))				
				{
					pktable=pkfkName.get(0);
					System.out.println("Unhandled Case. Expect Error");
				}
				else if(joinHistory.contains(pkfkName.get(0))){
					//when joined data is pk and new table is fk
					//Iterate over joined data and lookup in hash of new data
					//ArrayList<String> data0 = new ArrayList<String>();
					pktable=pkfkName.get(0);
					fktable=pkfkName.get(1);
					joinHistory.add(fktable);

					ListIterator<ArrayList<String>> iter = joinedData.listIterator();
					dest_counter=0;;
					PingPongjoinedData = new ArrayList<ArrayList<String>>();
					int i=0;
					//Iterate Over Intermediate result
					while (iter.hasNext()) {
						ArrayList<String> temp = new ArrayList<String>();
						temp.addAll(iter.next());
						if((joinHistory.size()-1) != temp.size())
						{
							continue;
						}
						//Iterate Over FK of new Table with indexed column =  pkfkName.get(2)
						//if(IndexToPrimaryKeyMapForAllTables.get(pkfkName.get(1)).get(pkfkName.get(2)).keySet().contains(temp.get(joinHistory.indexOf(pktable)))==false)
						if(IndexToPrimaryKeyMapForAllTables.get(pkfkName.get(1)).get(pkfkName.get(2)).get(temp.get(joinHistory.indexOf(pktable)))==null)
						{
							continue;
						}
						//for (Object data1 : IndexToPrimaryKeyMapForAllTables.get(pkfkName.get(1)).get(pkfkName.get(2)).keySet()) 
						{
							
							//if (temp.get(joinHistory.indexOf(pktable)).equals(data1)) 
							{
								for (Object pkOfFK : IndexToPrimaryKeyMapForAllTables.get(pkfkName.get(1)).get(pkfkName.get(2)).get(temp.get(joinHistory.indexOf(pktable)))) 
								{
									ArrayList<String> localTemp = new ArrayList<String>();
									//copy all the previous primary keys
									for(i=0;i<temp.size();i++){
										localTemp.add(temp.get(i));
									}
									localTemp.add((String) pkOfFK);	
									PingPongjoinedData.add(dest_counter, localTemp);
									dest_counter++;
								}
							}
						}
						temp = null;
					} //while
					
					joinedData=null;
					joinedData = new ArrayList<ArrayList<String>>();		
					joinedData = PingPongjoinedData;
					PingPongjoinedData = null;
				}
				//System.out.println("next join-> pk: "+pkfkName.get(0)+", fk: "+ pkfkName.get(1));
				//System.out.println("Intermediate Size:  " + joinedData.size());

			}
			
			
		}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		
		FinalJoinedData = joinedData;
		//System.out.println("Complete!");
		//Finalize answer
		/*
		ListIterator<ArrayList<String>> iter1 = joinedData.listIterator();
		ArrayList<ArrayList<String>>  FinalResult = null;
		FinalResult = new ArrayList<ArrayList<String>>();
		
		int final_count=0;
		int i=0;
		while (iter1.hasNext()) {
			ArrayList<String> temp = new ArrayList<String>();
			
			temp.addAll(iter1.next());
			ArrayList<String> localTemp = new ArrayList<String>();
			//copy all the previous primary keys
			for(i=0;i<temp.size();i++){
				localTemp.add(temp.get(i));
			}
		
			if(joinHistory.size()== temp.size())
			{
				FinalResult.add(final_count,localTemp);
			}
			else
			{
				System.out.println("Error");
			}
			temp=null;		
			final_count++;
		}
		

		System.out.println("Final Answer : "+FinalResult.size() + "   "+final_count);
		 */
	}

	
	public void getJoinOutput(PlainSelect plain){
		
		
		LinkedHashMap<Integer, String> columnIndicesAndDirection = new LinkedHashMap<Integer, String>();
		LinkedList<String> columnNamesAndDirection = new LinkedList<String>();
		List<OrderByElement> orderByElements = plain.getOrderByElements();
		
		
		
		Map<String, ArrayList<String>> groupByStringMap = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<Double>> groupByMap = new HashMap<String, ArrayList<Double>>();
		Map<String, ArrayList<Integer>> groupByMapDenominators = new HashMap<String, ArrayList<Integer>>();
		
		//1. get select-list
		ArrayList<Expression> originalSelectList = new ArrayList<Expression>();
		ArrayList<Expression> selectlist = new ArrayList<Expression>();
		boolean isAggregate = false,isStarPresent = false;
		List<SelectItem> selectItems = plain.getSelectItems();
		List<String> tableInSelectItems = new ArrayList<String>();
		for (SelectItem select : selectItems) {
			if (select.toString().equals("*")) {
				isStarPresent = true;
				break;
			}
			
			Expression expression = ((SelectExpressionItem) select).getExpression();
			
			if (expression instanceof Function) {
				isAggregate = true;
				selectlist.add(((Function) expression));
				tableInSelectItems.add(((Function) expression).getParameters().getExpressions().get(0).toString().split("\\.")[0]);
			} else {
				selectlist.add(expression);
				tableInSelectItems.add(expression.toString().split("\\.")[0]);
			}
			originalSelectList.add(((SelectExpressionItem) select).getExpression());
		}
		//EvalLib e = new EvalLib(tableName);
		String groupKey = "";
		List<Column> groupByColumns = plain.getGroupByColumnReferences();
		Map<String,EvalLib> evalMap = new HashMap<String,EvalLib>();
		for(int i=0;i<joinHistory.size();i++){
			evalMap.put(joinHistory.get(i),new EvalLib(joinHistory.get(i)));
		}
		
		
		int[] indexOffset = new int[joinHistory.size()];
		indexOffset[0] = columnNameToIndexMapping.get(joinHistory.get(0)).size();
		for(int i=1;i<joinHistory.size();i++){
			indexOffset[i] = indexOffset[i-1]+columnNameToIndexMapping.get(joinHistory.get(i)).size();				
		}
		if (groupByColumns != null) {
			//2. generate group by key and calculate
			try {
				Expression operand = null;
				String temp = "";
				PrimitiveValue result;
				Iterator<ArrayList<String>> itJoinRowPK = FinalJoinedData.iterator();
				while(itJoinRowPK.hasNext()){
					ArrayList<String> PKs= itJoinRowPK.next();
					//for(int idx=0;idx<joinHistory.size();idx++){
					
					try
					{
						split(new String(primaryKeyToDataMapByte.get(groupByColumns.get(0).getTable().getName()).get(PKs.get(joinHistory.indexOf(groupByColumns.get(0).getTable().getName())))));
					}
					catch(Exception Ex)
					{
						System.out.println("Crashed "+ groupByColumns.get(0).getTable().getName()+ "  "+plain);
					}
						//split(new String(primaryKeyToDataMapByte.get(groupByColumns.get(0).getTable().getName()).get(PKs.get(joinHistory.indexOf(groupByColumns.get(0).getTable().getName())))));
						groupKey = evalMap.get(groupByColumns.get(0).getTable().getName()).eval(groupByColumns.get(0)).toRawString();
						for (int i = 1; i < groupByColumns.size(); i++) {
							split(new String(primaryKeyToDataMapByte.get(groupByColumns.get(i).getTable().getName()).get(PKs.get(joinHistory.indexOf(groupByColumns.get(i).getTable().getName())))));
							groupKey += "|"+evalMap.get(groupByColumns.get(i).getTable().getName()).eval(groupByColumns.get(i)).toRawString();
						}
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
						for (int i = 0; i < selectlist.size(); i++){
							Function func = null;
							if (selectlist.get(i) instanceof Function) {
								func = (Function) selectlist.get(i);
								split(new String(primaryKeyToDataMapByte.get(tableInSelectItems.get(i)).get(PKs.get(joinHistory.indexOf(tableInSelectItems.get(i))))));
								switch (func.getName().charAt(2)) {
								case 'G':// AVG
									result = evalMap.get(tableInSelectItems.get(i)).eval(func.getParameters().getExpressions().get(0));
									if (result != null) {
										groupByMap.get(groupKey).set(i,
												groupByMap.get(groupKey).get(i) + result.toDouble());
										groupByMapDenominators.get(groupKey).set(i,
												groupByMapDenominators.get(groupKey).get(i) + 1);
									}
									break;
								case 'M':// SUM
									result = evalMap.get(tableInSelectItems.get(i)).eval(func.getParameters().getExpressions().get(0));
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
										result = evalMap.get(tableInSelectItems.get(i)).eval(operand);
										temp = result.toRawString();// record.get(index);//(rowData[index]);
										if (temp.trim().length() != 0) {
											// result = e.eval(operand);
											if (evalMap.get(tableInSelectItems.get(i)).eval(operand) != null) {
												groupByMap.get(groupKey).set(i,
														groupByMap.get(groupKey).get(i) + 1);
											}
										}
									}
									break;
								case 'N':// MIN
									operand = (Expression) func.getParameters().getExpressions().get(0);
									result = evalMap.get(tableInSelectItems.get(i)).eval(operand);
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
									result = evalMap.get(tableInSelectItems.get(i)).eval(operand);
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
								split(new String(primaryKeyToDataMapByte.get(tableInSelectItems.get(i)).get(PKs.get(joinHistory.indexOf(tableInSelectItems.get(i))))));
								if (groupByStringMap.get(groupKey).get(i).trim().length() == 0) {
									groupByStringMap.get(groupKey).set(i, evalMap.get(tableInSelectItems.get(i)).eval(operand).toRawString());
								}
							}
						}
					//}
				}
				//System.out.println("GrpBy Done");
				resultSet = new ArrayList<String[]>();
				Function item = null;
				for(String outputGroupKey : groupByMap.keySet()){
					String[] row = new String[selectlist.size()];
					for (int i = 0; i < selectlist.size(); i++) {
						item = null;
						if (selectlist.get(i) instanceof Function) {
							item = (Function) selectlist.get(i);
							switch (item.getName()) {
							case "SUM":
							case "MIN":
							case "MAX":
							case "count":
							case "COUNT":
								row[i] = groupByMap.get(outputGroupKey).get(i).toString();
								break;
							case "AVG":
								row[i] = ""+((groupByMap.get(outputGroupKey).get(i)
										/ groupByMapDenominators.get(outputGroupKey).get(i)));
								break;
							default:
								break;
							}
						} else {
							row[i] = groupByStringMap.get(outputGroupKey).get(i);
						}
					}
					resultSet.add(row);
				}
//				/System.out.println("resultset created!");
				///4. order elements
				if (orderByElements != null && orderByElements.size() > 0) {
					
					for (OrderByElement colName : orderByElements) {
						for(int i=0;i<plain.getSelectItems().size();i++){
							if(plain.getSelectItems().get(i).toString().contains(colName.getExpression().toString())){
								columnIndicesAndDirection.put(i,(colName.isAsc() ? "ASC" : "DESC"));
								columnNamesAndDirection.add(colName.getExpression().toString());
							}
						}
					}
					
					orderElements("",columnIndicesAndDirection, columnNamesAndDirection);
				}
				StringBuilder tb = new StringBuilder();
				int condition_met_counter=0;
				for(String[] r: resultSet){
					
					tb.append(String.join("|", r)).append("\n");
					condition_met_counter++;
					if (condition_met_counter == plain.getLimit().getRowCount())
							{
								break;
							}
					
				}
				System.out.println(tb.toString());
				//System.out.println("--ordered--");
				////
				
				///group by calculation complete
				///3. print group by
				/*List<String> keyList = new ArrayList<String>(groupByMap.keySet());
				Collections.sort(keyList);
				//
				Function item=null;
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
				return;
				*/
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Error:"+ e.getMessage());
			}
			
			
			//generate group by key
			
			
		}
	}
	@SuppressWarnings("unused")
	public void getSelectedColumns(String tableName, PlainSelect plain, Expression whereExpression)
			throws IOException, InvalidPrimitive, SQLException {
		try {

			
			String csvFile_local_copy = csvFile + tableName + ".csv";
			resultSet = new ArrayList<String[]>();
			if (plain == null) // non-nested query case
			{
				plain = this.plain;
			}
			/*
			if (plain.getJoins() != null) {
				getJoinOutput(plain);
				return;
				
			}
			*/

			String[] row = null;
			Map<String, ArrayList<String>> groupByStringMap = new HashMap<String, ArrayList<String>>();
			Map<String, ArrayList<Double>> groupByMap = new HashMap<String, ArrayList<Double>>();
			Map<String, ArrayList<Integer>> groupByMapDenominators = new HashMap<String, ArrayList<Integer>>();
			EvalLib e = new EvalLib(tableName);
			List<Column> groupByColumns = plain.getGroupByColumnReferences();
			
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
			ArrayList<Expression> selectlist = new ArrayList<Expression>();
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

			Collections.sort(indicesToRemove, Collections.reverseOrder());
			for (int i : indicesToRemove) {
				selectlist.remove(i);
			}
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
					int line_number = 0;
					int condition_met_counter = 0;
					if (orderByElements == null) {
						returnStartEnd(tableName, whereExpression, selectItems);
						Object Start = "";
						if (StartKey == null) {
							Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

						} else {
							// System.out.println(StartKey);
							Start = StartKey;
						}
						for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
								.keySet()) {

							// for(Object key :
							// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
							// {
							if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {

								if (!nestedquery) {
									sb.append(String.join("|", (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key)));
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
							primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
						}
						if (EndKeyInserted != null) {
							primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
						}
					} else {

						returnStartEnd(tableName, whereExpression, selectItems);
						Object Start = "";
						if (StartKey == null) {
							Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

						} else {
							Start = StartKey;
						}
						for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
								.keySet()) {

							// for(Object key :
							// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
							// {
							// resultSet.add(new ArrayList<Object>());
							// (String[])primaryKeyToDataMapOfGivenTable.get(tableName).get(key)
							// sb.append(String.join("|",
							// (String[])primaryKeyToDataMapOfGivenTable.get(tableName).get(key)));
							// sb.append("\n");
							if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
								if (!nestedquery) {
									resultSet.add((String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key));
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
							primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
						}
						if (EndKeyInserted != null) {
							primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
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

				int line_number = 0;

				if (orderByElements == null) {
					returnStartEnd(tableName, whereExpression, selectItems);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
							.keySet()) {

						// for(Object key :
						// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
						// {
						if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
							validrows_backup.put(line_number, true);
						} else {
							line_number++;
							continue;
						}

						line_number++;
						rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key);
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
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
					}
				} else {
					returnStartEnd(tableName, whereExpression, selectItems);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();
					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
							.keySet()) {

						// for(Object key :
						// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
						// {
						if ((validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
							validrows_backup.put(line_number, true);
						} else {
							line_number++;
							continue;
						}

						line_number++;
						row = new String[columnNameToIndexMapping.get(tableName).size()];
						rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key);
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
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
					}
				}

				if (nestedquery) {
					validrows = validrows_backup;
					return;
				}
			} else if (isStarPresent && !whereclauseabsent) {

				int line_number = 0;
				int condition_met_counter = 0;

				if (orderByElements == null) {
					returnStartEnd(tableName, whereExpression, selectItems);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
							.keySet()) {

						// for(Object key :
						// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
						// {
						if (validrows.get(line_number) == null) {
							line_number++;
							continue;
						}

						rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key);

						if (e.eval(whereExpression).toBool()) {

							condition_met_counter++;
							if (!nestedquery) {
								sb.append(String.join("|", (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key)));
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
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
					}
				} else {

					returnStartEnd(tableName, whereExpression, selectItems);
					Object Start = "";
					if (StartKey == null) {
						Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

					} else {
						Start = StartKey;
					}
					for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
							.keySet()) {

						// for(Object key :
						// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
						// {
						if (validrows.get(line_number) == null) {
							line_number++;
							continue;
						}

						rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key);

						if (e.eval(whereExpression).toBool()) {

							condition_met_counter++;
							if (!nestedquery) {
								resultSet.add(rowData);
								// sb.append(String.join("|",
								// (String[])primaryKeyToDataMapOfGivenTable.get(tableName).get(key)));
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
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
					}
					if (EndKeyInserted != null) {
						primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
					}
				}

				if (nestedquery == true) {
					validrows = validrows_backup;
					return;
				}
			} else if (isAggregate && (groupByColumns == null)
					&& !(primaryKeyToDataMapOfGivenTable.get(tableName).size() > 0 || indexToPrmryKeyMapOfGivenTable.size() > 0)) {
				Aggregate a = new Aggregate();
				a.setColumnDataTypes(columnDataTypes);
				a.setColumnNameToIndexMapping(columnNameToIndexMapping);
				a.setPlain(plain);
				a.getSelectedColumns(tableName, null, primaryKeyToDataMapOfGivenTable.get(tableName).size(), whereExpression);
				simplePrint = true;
				return;
			} else if ((primaryKeyToDataMapOfGivenTable.get(tableName).size() > 0 || indexToPrmryKeyMapOfGivenTable.size() > 0)
					&& (groupByColumns == null)) {

				simplePrint = true;
				int lineNum = 0;
				String line32 = "";
				if (whereclauseabsent) {
					simplePrint = false;
					alreadyPrinted = true;
					Aggregate a = new Aggregate();
					a.setColumnDataTypes(columnDataTypes);
					a.setColumnNameToIndexMapping(columnNameToIndexMapping);
					a.setPlain(plain);
					a.getSelectedColumns(tableName, null, primaryKeyToDataMapOfGivenTable.get(tableName).size(), whereExpression);
					// Aggregate.getSelectedColumns(tableName+"2",
					// whereExpression);
				} else if (indexToPrmryKeyMapOfGivenTable.size() > 0) {
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
						for (int i : indicesToRemove) {
							selectlist.remove(i);
						}

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
							// indexToPrmryKeyMapOfGivenTable.keySet().toArray(a)

							rowData = new String[columnNameToIndexMapping.get(tableName).size()];

							String final_s = null;
							for (String s : indexNameToColumnsMap.keySet()) {
								if (s.contains("INDEX")) {

									if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())
											&& a.toString().split(((AndExpression) a).getStringExpression())[1]
													.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
										// System.out.println("both expressions
										// contain index column");
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
										// System.out.println("left expression
										// contain index column");
										a = ((AndExpression) a).getLeftExpression();
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
											.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
										// System.out.println("right expression
										// contain index column");
										a = ((AndExpression) a).getRightExpression();
										final_s = s;
										break;
									}
								} else if (false)// (s.contains("PRIMARY"))
								{
									if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())
											&& a.toString().split(((AndExpression) a).getStringExpression())[1]
													.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
										// System.out.println("both expressions
										// contain index column");
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[0]
											.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
										// System.out.println("left expression
										// contain index column");
										a = ((AndExpression) a).getLeftExpression();
										final_s = s;
										break;
									} else if (a.toString().split(((AndExpression) a).getStringExpression())[1]
											.contains(indexNameToColumnsMap.get(tableName).get(s).get(0).toUpperCase())) {
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
									.contains(indexNameToColumnsMap.get(tableName).get(final_s).get(0).toUpperCase()))) {
								int conditionMet = 0;
								boolean breakAll = false;
								for (Object o : indexToPrmryKeyMapOfGivenTable
										.get(indexNameToColumnsMap.get(tableName).get(final_s).get(0).toUpperCase()).keySet()) {

									rowData[columnNameToIndexMapping.get(tableName).get(
											indexNameToColumnsMap.get(tableName).get(final_s).get(0).toUpperCase())] = o.toString();

									if (e.eval(a).toBool()) {
										TreeSet<Object> li = indexToPrmryKeyMapOfGivenTable
												.get(indexNameToColumnsMap.get(tableName).get(final_s).get(0).toUpperCase()).get(o);
										for (Object data : li) {
											count_All++;
											rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(data);
											// try (Stream<String> lines =
											// Files.lines(Paths.get(csvFile_local_copy)))
											// {
											// rowData =
											// lines.skip(lineNum).findFirst().get().split("\\|",-1);
											// //System.out.println(line32);
											// }
											if (e.eval(whereExpression).toBool()) {
												conditionMet++;
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
																&& conditionMet == plain.getLimit().getRowCount()) {
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
																&& conditionMet == plain.getLimit().getRowCount()) {
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
									if (breakAll) {
										break;
									}
								}

								// -----------------

								if (isAggregate && (conditionMet == 0) && (!count_star_present)) {
									return;
								} else if ((conditionMet == 0) && count_star_present) {
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
								simplePrint = false;
								alreadyPrinted = true;
								Aggregate agg = new Aggregate();
								agg.setColumnDataTypes(columnDataTypes);
								agg.setColumnNameToIndexMapping(columnNameToIndexMapping);
								agg.setPlain(plain);
								agg.getSelectedColumns(tableName, null, primaryKeyToDataMapOfGivenTable.get(tableName).size(),
										whereExpression);
							} else {

								getSelectedColumnsOld(tableName, plain, whereExpression, selectItems);
								// System.out.println("Invalid query : No
								// aggregate and Where expression doesnot
								// contain any Index columnn");
							}
						}
					} else if (whereExpression instanceof EqualsTo) {
						// Foobar
						returnStartEnd(tableName, whereExpression, selectItems);
						return;
					} else {// if it has no AND in where clause
						if (nestedquery) {
							getSelectedColumnsOld(tableName, plain, whereExpression, selectItems);
						} else {
							simplePrint = false;
							alreadyPrinted = true;
							Aggregate agg = new Aggregate();
							agg.setColumnDataTypes(columnDataTypes);
							agg.setColumnNameToIndexMapping(columnNameToIndexMapping);
							agg.setPlain(plain);
							agg.getSelectedColumns(tableName, null, primaryKeyToDataMapOfGivenTable.get(tableName).size(),
									whereExpression);
						}
					}

				} else {

				}
				if (nestedquery) {
					validrows = validrows_backup;
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
				int line_number = 0;
				int condition_met_counter = 0;

				returnStartEnd(tableName, whereExpression, selectItems);
				Object Start = "";
				if (StartKey == null) {
					Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

				} else {
					Start = StartKey;
				}
				for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start)
						.keySet()) {

					/// for(Object key :
					/// primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
					// {
					if (validrows.get(line_number) == null) {
						line_number++;
						continue;
					}

					rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key);

					groupKey = "";
					if (e.eval(whereExpression).toBool()) {

						validrows_backup.put(line_number, true);
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
						// Key Should be generated according to group order by

						// if(e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression()))
						if (!isAggregate && (groupByColumns == null)) {
							for (int i = 0; i < selectItems.size(); i++) {

								result = e.eval(((SelectExpressionItem) selectItems.get(i)).getExpression());
								sb.append(result.toRawString().concat("|"));
							}
							sb.setLength(sb.length() - 1);
							sb.append("\n");
							if (plain.getLimit() != null && condition_met_counter == plain.getLimit().getRowCount()) {
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

					if ((EndKey != null) && (key.toString().equals(EndKey))) {
						break;
					}
				}

				if (StartKeyInserted != null) {
					primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
				}
				if (EndKeyInserted != null) {
					primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
				}

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
					orderElements(tableName, columnIndicesAndDirection, null);
				}

				if (selectlist.size() != 0) {
					aggregateHistory.get(tableName).get("COUNT_A").replace("COUNT", count_All);
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
					orderElements(tableName, columnIndicesAndDirection, null);

					if (!nestedquery) {
						for (String[] resultRow : resultSet) {
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

							sb.setLength(sb.length() - 1);
							sb.append("\n");
						}
					}
				}

			}
			// System.out.println(groupByMap);
			// sb.setLength(sb.length() - 1);
			System.out.print(sb.toString());// to print normal queries
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	// This will sort max 2 columns simultaneously for now!
	public void orderElements(String tableName, LinkedHashMap<Integer, String> columnIndicesAndDirection, LinkedList<String> columnNamesAndDirection) {
		List<String> dataType = new ArrayList<String>();
		String type = "";
		int colNumber = -1;// for multicolumn sorting;
		ArrayList<Integer> colIndexes = new ArrayList<Integer>();
		boolean isAsc = false;
	
		for (Integer index : columnIndicesAndDirection.keySet()) {
			colNumber++;
			if(columnNamesAndDirection!=null){
				if(columnNamesAndDirection.get(colNumber).equals("RETURNFLAG")){
					type = "CHAR";
				}
				else if(columnNamesAndDirection.get(colNumber).equals("LINESTATUS")){
					type = "CHAR";
				}
				else if(columnNamesAndDirection.get(colNumber).equals("REVENUE")){
					type = "DECIMAL";
				}
				else if(columnNamesAndDirection.get(colNumber).contains("DATE")){
					type = "DATE";
				}
				else if(columnNamesAndDirection.get(colNumber).equals("SHIPMODE")){
					type = "CHAR";
				}
			}
			else{
				columnDataTypes.get(tableName).get(index);
			}
			isAsc = (columnIndicesAndDirection.get(index).equals("ASC")) ? true : false;
			colIndexes.add(index);
			switch (type) {
			case "INT":
			case "int":
			case "DOUBLE":
			case "DECIMAL":
			case "double":
			case "decimal":
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
			returnStartEnd(tableName, whereExpression, selectItems);
			Object Start = "";
			if (StartKey == null) {
				Start = primaryKeyToDataMapOfGivenTable.get(tableName).firstKey();

			} else {
				Start = StartKey;
			}
			for (Object key : ((TreeMap<Object, Object>) primaryKeyToDataMapOfGivenTable.get(tableName)).tailMap(Start).keySet()) {

				// for(Object key : primaryKeyToDataMapOfGivenTable.get(tableName).keySet())
				// {
				rowData = (String[]) primaryKeyToDataMapOfGivenTable.get(tableName).get(key);
				if (!whereclauseabsent && validrows.get(line_number) == null) {
					line_number++;
					continue;
				}

				if (whereclauseabsent && (validrows.get(line_number) != null) && (validrows.get(line_number) == true)) {
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
				if ((EndKey != null) && (key.toString().equals(EndKey))) {
					break;
				}
			}

			if (StartKeyInserted != null) {
				primaryKeyToDataMapOfGivenTable.get(tableName).remove(StartKey);
			}
			if (EndKeyInserted != null) {
				primaryKeyToDataMapOfGivenTable.get(tableName).remove(EndKey);
			}
			if (nestedquery) {
				validrows = validrows_backup;
				return;
			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());

		}
	}

	void Csvreader_SpeedTest(String filename) throws Throwable {
		File file = new File(filename);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		long t0 = System.currentTimeMillis();
		while ((line = reader.readLine()) != null) {
			split(line);
		}
		long t1 = System.currentTimeMillis();
		reader.close();
		//System.out.println("read " + file.length() + " bytes in " + (t1 - t0) + " ms");
	}

	private void split(String line) {
		int idxComma, idxToken = 0, fromIndex = 0;
		while ((idxComma = line.indexOf(delimiter, fromIndex)) != -1) {
			rowData[idxToken++] = line.substring(fromIndex, idxComma);
			fromIndex = idxComma + 1;
		}
		rowData[idxToken] = line.substring(fromIndex);

		// rowData = line.split("\\|",-1);
	}

	public static void main(String[] args) throws Throwable {

		Runtime.getRuntime().freeMemory();
		Runtime.getRuntime().gc();
		// Main m = new Main();
		// m.Csvreader_SpeedTest(csvFile+"LINEITEM.csv");
		arg = "in-mem";
		//arg="on-disk";

		//arg=args[1];
		// System.out.println(args[0]);
		// System.out.println(args[1]);
		if (arg.contains("in-mem")) {
			Main InMemoryMain = new Main();
			InMemoryMain.scanLinesInMemory();
		}

		if (arg.contains("on-disk")) {
			MainOnDisk OnDiskMain = new MainOnDisk();
			OnDiskMain.scanLinesOnDisk();
		}

	}

	public void scanLinesInMemory() throws Exception {

		// long startTime = System.nanoTime();
		System.out.print("$>");
		scan = new Scanner(System.in);
		String temp;
		String query = "";
		while ((temp = scan.nextLine()) != null) {
			Instant start = Instant.now();
			query += temp + " ";
			if (temp.indexOf(';') >= 0) {
				readQueries(query);
				parseQueries();
				// Instant end = Instant.now();
				// System.out.println(Duration.between(start, end)); // prints
				// PT1M3.553S
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
				if (indexNameToColumnsMap.get(tableName).get("INDEX").get(0).contains(col.getColumnName())) {
					// indexToPrmryKeyMapOfGivenTable.keySet().toArray(a);

				}
				return new StringValue("");
			}
		}
	}
}

//////// logic////
/*
 * 1. Read file o create and save 2 hashmaps pk->data, fk->pk 
 * 2. Each hashmap -> * disk 
 * 3. Onselect - >
 *  3. a read where-> individual table->apply normal where
 * -> set NULL if condition not met -> save data in mem 
 *   3. b join 2 tables ->
 * create AL( each item->pk->everytable) 4. c join 3 or more -> append the
 *
 */