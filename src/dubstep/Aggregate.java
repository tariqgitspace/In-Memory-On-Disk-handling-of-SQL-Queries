//create table R(A int, B String, C String, D int );Select SUM(A),B, SUM(D) From R
//create table R(A int, B String, C String, D int );Select SUM(A),Count(*), SUM(D),MIN(A),MAX(D),AVG(D),Count(C) From R
//create table R(A int, B String, C String, D int );Select SUM(A), SUM(D) From R;
//create table R(A int, B String, C String, D int ); select A,B,C,D from R
//create table R(A int, B String, C String, D int ); select A,B from R where A=1 and B=1;select * from R
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (B=1 AND D =9)
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1 AND D =9)
//create table R(A int, B String, C String, D int ); select A,B from R where (A=1 and B=2) OR (C>4 AND B=1) AND (B>2 OR D =9)

package dubstep;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.eval.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;


public class Aggregate{


	private String[] rowData = null;

	//aggregateHistory is HashMap <Table ,<Aggregate Value of each column in this table>> 
	private  Map<String,Map<String,Map<String,Double>>> aggregateHistory=new HashMap<String,Map<String,Map<String,Double>>> ();
	//public enum columndDataTypess  {String,varchar,Char,Int,decimal,date}; 
	private  BufferedReader br = null;

	private  String csvFile = "src\\dubstep\\data\\";
	//public static String csvFile = "data/";
	private  String line = "";
	private  Statement statement;
	private static Scanner scan;
	private  CCJSqlParser parser;
	private  PlainSelect plain;
	private  Map<String,ArrayList <String>> columnDataTypes = new HashMap<String,ArrayList <String>>();
	private  Map<String,Map<String,Integer>> columnNameToIndexMapping = new HashMap<String,Map<String,Integer>>();
	private  Select select;
	private  SelectBody body;
	private  String[] columnDataTypeArray = null;
	private  String[] columnIndexArray = null;
	private  List<String> columnIndexList = null;
	boolean[] validrows;

	public static void main(String[] args) throws Exception
	{}






	/*******************************************
	1> Read Select Statement one by one.
	2> Check if  This is Plain or Aggrregate query
	3> If its Plain query, fetch the Select value using Evallib
	4> If its Aggregate Query, then perform the operation as per aggregate function
	 */
	public  void getSelectedColumns(String tableName, PlainSelect plain,int row_size,Expression whereExpression) throws IOException
	{
		try{

			if(plain==null)
			{
				plain=this.plain;
			}
			//Initialize Variable
			columnIndexArray = new String[columnNameToIndexMapping.get(tableName).keySet().size()];
			columnDataTypeArray = new String[columnDataTypes.get(tableName).size()];
			columnIndexList = Arrays.asList(columnIndexArray);
			Map<String, Integer> q = columnNameToIndexMapping.get(tableName);//.keySet()..toArray(columnIndexArray);
			columnDataTypes.get(tableName).toArray(columnDataTypeArray);
			for(String k: q.keySet()){
				columnIndexList.set(q.get(k),k);
			}
			String csvFile_local_copy = csvFile+tableName+".csv";
			br = new BufferedReader(new FileReader(String.format(csvFile_local_copy)));
			StringBuilder sb = new StringBuilder();
			EvalLib e = new EvalLib(tableName);
			PrimitiveValue pv = null;
			double[] aggrMap = new double[10];

			String[] aggrStrMap = new String[10];
			int[] aggrDenomMap = new int[10];
			ArrayList <Expression> originalSelectList = new ArrayList<Expression>();
			List<SelectItem> selectItems = plain.getSelectItems();
			ArrayList <Expression> selectlist = new ArrayList<Expression>();
			boolean whereclauseabsent = (plain.getWhere()==null)?true:false;
			boolean isStarPresent = false;
			boolean is_aggregate=false;
			PrimitiveValue result=null;
			boolean count_star_present=false;

			//Extract individual Select Statements in selectlist
			for(SelectItem select: selectItems)
			{
				if(select.toString().equals("*")){ isStarPresent = true; break; }
				Expression expression = ((SelectExpressionItem)select).getExpression();
				if(expression instanceof Function)
				{
					is_aggregate = true;
					selectlist.add((Function) expression);
				}
				else{
					selectlist.add(expression);
				}
				originalSelectList.add(((SelectExpressionItem)select).getExpression());
			}


			//System.out.println(Integer.MAX_VALUE);
			List<Double> test = new ArrayList<Double>();
			//History of Aggregate Function is saved
			//If Already Aggregate Computation exists(from last query which was executed), delete this entry from to-Compute list
			//Also, delete duplicate Selects in a single query.			
			ArrayList<Integer> indicesToRemove = new ArrayList<Integer>();
			if(is_aggregate){

				Function item=null;
				String columnName ="";
				for(int i =0; i<originalSelectList.size();i++)
				{
					String columname ="";
					//Foo
					if(originalSelectList.get(i) instanceof Function)
					{
						if(originalSelectList.get(i).toString().contains("(*)"))
						{
							columname = "COUNT_A";
							count_star_present=true;
						}
						else
						{
							columname = ((Function) originalSelectList.get(i)).getParameters().getExpressions().get(0).toString();

						}
						item = (Function) originalSelectList.get(i);
						String aggregateFunction = item.getName(); 
						if(aggregateHistory.get(tableName)==null)
						{
							//aggregateHistory.get(tableName).putIfAbsent(columname, null);
							Map<String,Double> agg= new HashMap<String,Double>();
							agg.put("SUM",0.0);
							agg.put("MAX",Double.MIN_VALUE);
							agg.put("MIN",Double.MAX_VALUE);
							agg.put("AVG",0.0);
							agg.put("COUNT",0.0);
							Map<String,Map<String,Double>> col = new HashMap<String,Map<String,Double>>();
							col.put(columname, agg);
							aggregateHistory.put(tableName, col);

						}
						else if(aggregateHistory.get(tableName).get(columname)==null)
						{

							Map<String,Double> agg= new HashMap<String,Double>();
							agg.put("SUM",0.0);
							agg.put("MAX",Double.MIN_VALUE);
							agg.put("MIN",Double.MAX_VALUE);
							agg.put("AVG",0.0);
							agg.put("COUNT",0.0);

							aggregateHistory.get(tableName).put(columname, agg);
						}


						else if(aggregateHistory.get(tableName).get(columname).get(aggregateFunction)!=null)
						{
							indicesToRemove.add(i);
						}

						if(aggregateHistory.get(tableName).get("COUNT_A")==null)
						{
							Map<String,Double> agg= new HashMap<String,Double>();
							agg.put("COUNT",0.0);					
							aggregateHistory.get(tableName).put("COUNT_A", agg);
						}

					}
					else
					{
						columname = ((Column) originalSelectList.get(i)).getColumnName();
					}

				}


				Collections.sort(indicesToRemove, Collections.reverseOrder());
				for (int i : indicesToRemove)
				{
					selectlist.remove(i);
				}



				ArrayList<String> avoidDuplicates = new ArrayList<String> ();
				ArrayList<Expression> l = new ArrayList<Expression>();

				//Remove Duplicates
				for(int idx = 0 ; idx<selectlist.size();idx++)
				{
					if(avoidDuplicates.indexOf(selectlist.get(idx).toString())==-1)
					{
						avoidDuplicates.add(selectlist.get(idx).toString());
						l.add(selectlist.get(idx));
					}
				}

				if(l.size()>1){
					for(int idx = 0 ; idx<l.size();idx++)
					{
						if(l.get(idx).toString().contains("(*)")){
							l.remove(idx);
							break;
						}
					}
				}
				selectlist = l;
			}

			for(int i =0;i<selectlist.size();i++){

				if(selectlist.get(i) instanceof Function)
				{
					if(((Function) selectlist.get(i)).getName().equals("MAX"))
					{
						aggrMap[i]=Double.MIN_VALUE;
					}

					else if(((Function) selectlist.get(i)).getName().equals("MIN"))
					{
						aggrMap[i]=Double.MAX_VALUE;
					}
				}

			}

			int count_All=0;
			int innerCount = 0;
			Function  item= null;
			Expression operand = null;
			if(isStarPresent){ // If a star is present then read the file at once to avoid i/o costs
				sb.append(String
						.join(
								System.getProperty("line.separator")
								,Files.readAllLines(Paths.get(csvFile_local_copy))
								)
						);

			}
			else{


				if((is_aggregate==true) && selectlist.size()==0)
				{
					//If All the needed information already exists in Memory, Dont do any Computation 
				}
				else{


					boolean whereexpressionresult = true;
					validrows = new boolean[row_size];
					Arrays.fill(validrows,true);
					int line_number=0;
					int condition_met_counter = 0;
					while ((line = br.readLine()) != null) {
						//System.out.println(tableName);

						rowData = line.split("\\|",-1);
						if(whereExpression!=null)
						{
							whereexpressionresult = e.eval(whereExpression).toBool();
							if(whereexpressionresult==false)
							{
								validrows[line_number]=false;
								line_number++;
								continue;
							}
						}
						line_number++;

						if(whereclauseabsent || whereexpressionresult)
						{

							condition_met_counter++;
							count_All++;
							if (is_aggregate == false) //If there is no Aggregate Statement in Query
							{
								for(int i=0;i<selectItems.size()-1;i++)
								{
									result = e.eval(((SelectExpressionItem)selectItems.get(i)).getExpression());
									sb.append(result.toRawString().concat("|"));
								}
								result = e.eval(((SelectExpressionItem)selectItems.get(selectItems.size()-1)).getExpression());
								sb.append(result.toRawString()+"\n");

								if((plain.getLimit()!=null)&&(condition_met_counter == plain.getLimit().getRowCount()))
								{
									break;
								}
							}
							else    //If its aggregate Query
							{
								for(int i =0; i<selectlist.size();i++)
								{									
									if(selectlist.get(i) instanceof Function )
									{
										item = (Function) selectlist.get(i);
										switch (item.getName().charAt(2)){
										case 'X':    //If Max
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(aggrMap[i] < result.toDouble())
											{
												aggrMap[i]=result.toDouble();
											}

											break;
										case 'N':    //If Minimum
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(result.toDouble() < aggrMap[i])
											{
												aggrMap[i] = result.toDouble();
											}

											break;
										case 'M':      //if Sum
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											//test.add(e.eval(operand).toDouble());
											//innerCount++;
											if(result !=null)
											{
												aggrMap[i]+=result.toDouble();
											}
											break;																				
										case 'U':      //if Count
											if(!item.toString().toLowerCase().contains("count(*)"))
											{
												operand = (Expression) item.getParameters().getExpressions().get(0);
												result = e.eval(operand);
												if(result!=null)
												{
													aggrMap[i]+=1;
												}
											}
											break;
										case 'G':	   //If Average
											operand = (Expression) item.getParameters().getExpressions().get(0);
											result = e.eval(operand);
											if(result!=null)
											{
												aggrMap[i]+=result.toDouble();
												aggrDenomMap[i]+=1;
											}
											break;
										default:
											break;
										}
									}else{// If the Column is not an aggregate column then simply get the value
										operand = selectlist.get(i);
										if(aggrStrMap[i]==null){
											aggrStrMap[i] =  e.eval(operand).toRawString();
										}
									}
								}
							}
						}
					}
				}		
				//double [] t = test.stream().mapToDouble((i->i)).toArray();
				//System.out.println(Arrays.stream(t).sum());
				//Update the Values to hasTable


				if(is_aggregate==true)
				{
					if(selectlist.size()!=0){
						aggregateHistory.get(tableName).get("COUNT_A").replace("COUNT",(double)count_All);
					}
					item=null;
					for(int i =0; i<selectlist.size();i++)
					{
						item = (Function) selectlist.get(i);
						String columnName ="";
						if(item.toString().contains("(*)"))
						{
							columnName = "COUNT_A";
						}
						else
						{
							columnName= ((Function) selectlist.get(i)).getParameters().getExpressions().get(0).toString();
						}						

						if(selectlist.get(i) instanceof Function)
						{
							if ((item.getName().equals("SUM")
									||item.getName().equals("MIN")
									||item.getName().equals("COUNT")
									||item.getName().equals("MAX")) && !item.toString().toLowerCase().equals("count(*)"))
							{
								aggregateHistory.get(tableName).get(columnName).replace(item.getName(),aggrMap[i]);

								//System.out.println(aggregateHistory.get(tableName));
							}
							else if(item.getName().equals("AVG"))
							{
								aggregateHistory.get(tableName).get(columnName).replace(item.getName(),
										(aggrMap[i]/aggrDenomMap[i]));
								aggregateHistory.get(tableName).get(columnName).replace("SUM",aggrMap[i]);							
								aggregateHistory.get(tableName).get(columnName).replace("COUNT",(double) aggrDenomMap[i]);
							}						
						}
						else
						{// if its a Simple String Column or Simple Number Column
							//Double elsecase = Double.parseDouble(aggrStrMap[i]);
							//aggregateHistory.get(tableName).get(columnName).replace(item.getName(), elsecase);
						}
					}

					//-----------------
					if((count_All==0)&&(!count_star_present))
					{
							return;
						
					}
					else if((count_All==0)&&count_star_present)
					{
						for(int i =0; i<originalSelectList.size();i++)
						{
							item = (Function) originalSelectList.get(i);
							if(originalSelectList.get(i) instanceof Function){
								String columnName = "COUNT_A";
								if(item.toString().toLowerCase().equals("count(*)"))
								{
									sb.append(aggregateHistory.get(tableName).get(columnName).get("COUNT").longValue()+"|");
								}
								else
								{
									sb.append("|");
								}
							}
						}
						if(sb.length() >=2)
						{

							sb.setLength(sb.length() - 1);
							sb.append("\n");
						}
						System.out.print(sb);
						return;
					}

					//-----------------------------------------
					
					int local_counter=0;
					//Read the Data from hashMap and Print the output on Console
					for(int i =0; i<originalSelectList.size();i++)
					{
						String columnName ="";
						item = (Function) originalSelectList.get(i);
						if(item.toString().contains("(*)"))
						{
							columnName = "COUNT_A";
						}
						else
						{
							columnName = ((Function) originalSelectList.get(i)).getParameters().getExpressions().get(0).toString();
						}
						if(originalSelectList.get(i) instanceof Function){

							if(item.toString().toLowerCase().equals("count(*)"))
							{
								sb.append(aggregateHistory.get(tableName).get(columnName).get("COUNT").longValue()+"|");
							}
							else if(item.getName().equalsIgnoreCase("SUM")
									||item.getName().equalsIgnoreCase("MIN")
									||item.getName().equalsIgnoreCase("COUNT")
									||item.getName().equalsIgnoreCase("MAX"))
							{
								int index = 0 ;
								if(columnNameToIndexMapping.get(tableName).get(columnName)==null){
									index = -1;
									sb.append( aggregateHistory.get(tableName).get(columnName).get(item.getName())+"|");
								}
								else {
									index = columnNameToIndexMapping.get(tableName).get(columnName);
									String type = columnDataTypes.get(tableName).get(index);
									Double temp= aggregateHistory.get(tableName).get(columnName).get(item.getName());
									sb.append((type.equals("long")||type.equals("int"))? temp.longValue()+"|":temp+"|");
								}

							}
							else if(item.getName().equalsIgnoreCase("AVG")){
								sb.append(aggregateHistory.get(tableName).get(columnName).get(item.getName())+"|");
							}
						}
						else{// if its a Simple String Column or Simple Number Column
							for(int j=local_counter;j<aggrStrMap.length;j++)
							{
								if(aggrStrMap[j]!=null)
								{
									local_counter=j;
									sb.append(aggrStrMap[local_counter]+"|");
									local_counter++;
									break;
								}
							}
						}
					}
				}
				if(sb.length() >=2)
				{

					sb.setLength(sb.length() - 1);
					sb.append("\n");
				}
				System.out.print(sb);
			}
		}
		catch(SQLException e){
			System.out.println(e.getMessage());

		}
	}
	static String[] colName = null;

	/*******************************************
	1> From Table Schema extract ColumnNames and ColumnDataTypes
	2> From File Corresponding to Table,read all the lines and Extract Common Statistics such as Min,Max and Average
	3> Save above extracted Values into corresponding HashMap
	 */


	class EvalLib extends Eval{
		String tableName = "";
		int index = 0;

		private EvalLib(String tableName){
			this.tableName = tableName;
		}
		@Override
		public PrimitiveValue eval(Column col) throws SQLException {
			index = columnIndexList.indexOf(col.getColumnName());
			switch( columnDataTypeArray[index].toLowerCase())
			{
			case "int": 
				return new LongValue(rowData[index]);
				//return new LongValue(record.get(index));
			case "decimal":
				return new DoubleValue(rowData[index]);
				//return new DoubleValue(record.get(index));
			case "string":
			case "varchar":
			case "char":
				return new StringValue(rowData[index]);
				//return new StringValue(record.get(index));

			case "date":
				return new DateValue(rowData[index]);
				//return new DateValue(record.get(index));
			default:
				return new StringValue(rowData[index]);
				//return new StringValue(record.get(index));
			}
		}
	}

	public void setPlain(PlainSelect plain) {
		this.plain = plain;
	}


	public void setColumnDataTypes(Map<String, ArrayList<String>> columnDataTypes) {
		this.columnDataTypes = columnDataTypes;
	}


	public void setColumnNameToIndexMapping(Map<String, Map<String, Integer>> columnNameToIndexMapping) {
		this.columnNameToIndexMapping = columnNameToIndexMapping;
	}



}