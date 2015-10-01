package com.onbalancetech.cooql.builder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.onbalancetech.cooql.COOQL;
import com.onbalancetech.cooql.COOQLResultSet;
import com.onbalancetech.cooql.COOQLResultSetFuture;
import com.onbalancetech.cooql.COOQLRow;
import com.onbalancetech.cooql.COOQLUDTValue;
import com.onbalancetech.cooql.From;
import com.onbalancetech.cooql.InsertInto;
import com.onbalancetech.cooql.InsertValues;
import com.onbalancetech.cooql.Operator;
import com.onbalancetech.cooql.OrderBy;
import com.onbalancetech.cooql.SelectLimit;
import com.onbalancetech.cooql.SelectOrderBy;
import com.onbalancetech.cooql.SelectOrderByDirection;
import com.onbalancetech.cooql.SelectOrderByOperator;
import com.onbalancetech.cooql.SelectValues;
import com.onbalancetech.cooql.TypeConverter;
import com.onbalancetech.cooql.UpdateValues;
import com.onbalancetech.cooql.UpdateWhere;

public class COOQLBuilder
{
	private static final String PACKAGE = "";
	private static final String PRIVATE = "private";
	private static final String PUBLIC = "public";

	private static String indentation;
	private static StringBuilder blockBuffer = new StringBuilder();

	public static final void buildAPI( InetAddress hostAddress, String keyspaceName )
	{
		String packageName = "com.onbalancetech.cooql.api";
		Cluster cluster = Cluster.builder().addContactPoint( hostAddress.getHostAddress() ).build();
		KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace( keyspaceName );
		if (metadata == null)
		{
			throw new IllegalArgumentException( "No keyspace named '"+keyspaceName+"' was found." );
		}

		StringBuilder workingBuffer = new StringBuilder();

		COOQLBuilder.buildCOOQLEntryPoint( workingBuffer, keyspaceName, packageName );

		COOQLBuilder.buildDelete( metadata, workingBuffer, packageName );
		COOQLBuilder.buildInsert( metadata, workingBuffer, packageName );
		COOQLBuilder.buildSelect( metadata, workingBuffer, packageName );
		COOQLBuilder.buildUpdate( metadata, workingBuffer, packageName );
		COOQLBuilder.buildUserInterfaceTypes( metadata, workingBuffer, packageName );
		COOQLBuilder.buildUserTypes( metadata, workingBuffer, packageName );

		Collection<TableMetadata> tableCollection = metadata.getTables();
		Iterator<TableMetadata> tableIterator = tableCollection.iterator();
		TableMetadata tableMetadata;

		// Iterate over each table/column family in the keyspace 
		while (tableIterator.hasNext())
		{
			tableMetadata = tableIterator.next();

			COOQLBuilder.buildCOOQLResultSet( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildCOOQLResultSetFuture( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildCOOQLRow( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildDeleteFrom( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildDeleteOperators( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildDeleteWheres( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildInsert( metadata, workingBuffer, packageName );
			COOQLBuilder.buildInsertValues( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildInsertInto( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectFrom( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectLimit( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectOperators( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectOrderBy( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectOrderByDirection( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectOrderByOperators( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectValues( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildSelectWheres( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildUpdateOperators( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildUpdateValues( tableMetadata, workingBuffer, packageName );
			COOQLBuilder.buildUpdateWheres( tableMetadata, workingBuffer, packageName );
		}
	}

	static final void buildCOOQLEntryPoint( StringBuilder workingBuffer, String keyspaceName, String packageName )
	{
		String classType = "COOQL";

		workingBuffer.setLength( 0 );

		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "com.datastax.driver.core.Session" );
		addCOOQLImport( workingBuffer );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, keyspaceName );

		setIndentation( 1 );

		addMethodHeader( workingBuffer, "public static", classType+"_"+keyspaceName, "getInstance", "Session session" );
		addMethodContent( workingBuffer, "return new ", classType, "_", keyspaceName, "( session );" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addConstructorHeader( workingBuffer, "protected", classType+"_"+keyspaceName, "Session session" );
		addMethodContent( workingBuffer, "super( session );" );
		addMethodFooter( workingBuffer );

		String[] commandList = { "Delete", "Insert", "Select", "Update" };
		String command;
		for (int c=0; c<commandList.length; c++)
		{
			command = commandList[ c ];
			addBlankLine( workingBuffer );
			addMethodHeader( workingBuffer, PUBLIC, command, command.toUpperCase(), null );
			addMethodContent( workingBuffer, "resetCommand();" );
			addMethodContent( workingBuffer, command+" ", command.toLowerCase(), " = new ", command, "( this );" );
			addMethodContent( workingBuffer, "getDataHolder().getBuffer().append( ", command.toLowerCase(), ".toString() );" );
			addMethodContent( workingBuffer, "return ", command.toLowerCase(), ";" );
			addMethodFooter( workingBuffer );
		}

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, keyspaceName, workingBuffer.toString() );
	}

	static final void buildCOOQLResultSet( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = COOQLResultSet.class.getSimpleName();
		String tableName = tableMetadata.getName();
		
		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.util.Iterator" );
		addImport( workingBuffer, "java.util.Set" );
		addBlankLine( workingBuffer );
		addImport( workingBuffer, "com.datastax.driver.core.ResultSet" );
		addImport( workingBuffer, "com.datastax.driver.core.Row" );
		addClassTypeImport( workingBuffer, COOQLResultSet.class.getSimpleName() );
		addClassTypeImport( workingBuffer, COOQLRow.class.getSimpleName() );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, "RowIterator" );
		addBlankLine( workingBuffer );
		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );
		addClassHeader( workingBuffer, PRIVATE, "RowIterator", tableName );
		addMethodContent( workingBuffer, "private Iterator<Row> iterator;" );
		addMethodContent( workingBuffer, "private ", COOQLResultSet.class.getSimpleName(), "_", tableName,
				" resultSet;" );
		addBlankLine( workingBuffer );
		setIndentation( 2 );
		addConstructorHeader( workingBuffer, PACKAGE, "RowIterator_"+tableName,
				COOQLResultSet.class.getSimpleName()+"_"+tableName+" resultSet" );
		addMethodContent( workingBuffer, "this.resultSet = resultSet;" );
		addMethodContent( workingBuffer, "iterator = this.resultSet.resultSet.iterator();" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, "boolean", "hasNext", null );
		addMethodContent( workingBuffer, "return iterator.hasNext();" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, COOQLRow.class.getSimpleName()+"_"+tableName, "next", null );
		addMethodContent( workingBuffer, "return new ", COOQLRow.class.getSimpleName(), "_", tableName, "( resultSet, iterator.next() );" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, "void", "remove", null );
		addMethodContent( workingBuffer, "iterator.remove();" );
		addMethodFooter( workingBuffer );
		workingBuffer.append( "\t}\r\n" );

		setIndentation( 1 );

		addBlankLine( workingBuffer );
		addConstructorHeader( workingBuffer, PACKAGE, COOQLResultSet.class.getSimpleName()+"_"+tableName,
				"ResultSet resultSet, DataHolder dataHolder" );
		addMethodContent( workingBuffer, "super( resultSet, dataHolder );" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, "protected", COOQLRow.class.getSimpleName(), "XcreateRow", "Row row" );
		addMethodContent( workingBuffer, "return new ", COOQLRow.class.getSimpleName(), "_", tableName, "( this, row );" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, "RowIterator_"+tableName, "iterator", null );
		addMethodContent( workingBuffer, "return new RowIterator_", tableName, "( this );" );
		addMethodFooter( workingBuffer );
		
		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildCOOQLResultSetFuture( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = COOQLResultSetFuture.class.getSimpleName();
		String tableName = tableMetadata.getName();
		
		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.util.concurrent.TimeUnit" );
		addImport( workingBuffer, "java.util.concurrent.TimeoutException" );
		addImport( workingBuffer, "java.util.Set" );
		addBlankLine( workingBuffer );
		addImport( workingBuffer, "com.datastax.driver.core.ResultSetFuture" );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, COOQLResultSetFuture.class.getSimpleName() );
		addBlankLine( workingBuffer );
		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructorHeader( workingBuffer, PUBLIC, classType+"_"+tableName,
				"ResultSetFuture resultSetFuture, DataHolder dataHolder" );
		addMethodContent( workingBuffer, "super( resultSetFuture, dataHolder );" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, COOQLResultSet.class.getSimpleName()+"_"+tableName, "getUninterruptibly", null );
		addMethodContent( workingBuffer, "return new ", COOQLResultSet.class.getSimpleName()+"_"+tableName,
				"( resultSetFuture.getUninterruptibly(), dataHolder );" );
		addMethodFooter( workingBuffer );
		
		addBlankLine( workingBuffer );
		workingBuffer.append( indentation );
		workingBuffer.append( PUBLIC ).append( " " );
		workingBuffer.append( COOQLResultSet.class.getSimpleName()+"_"+tableName ).append( " " ).append( "getUninterruptibly" ).append( "(" );
		workingBuffer.append( " " ).	append( "long timeout, TimeUnit unit" ).append( " " );
		workingBuffer.append( ") throws TimeoutException\r\n" ).append( indentation ).append( "{\r\n" );
		addMethodContent( workingBuffer, "return new ", COOQLResultSet.class.getSimpleName()+"_"+tableName,
				"( resultSetFuture.getUninterruptibly( timeout, unit ), dataHolder );" );
		addMethodFooter( workingBuffer );
		
		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildCOOQLRow( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = COOQLRow.class.getSimpleName();
		String tableName = tableMetadata.getName();

		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.nio.ByteBuffer" );
		addImport( workingBuffer, "java.util.Date" );
		addImport( workingBuffer, "java.util.List" );
		addBlankLine( workingBuffer );
		addImport( workingBuffer, "com.datastax.driver.core.Row" );
		addImport( workingBuffer, "com.datastax.driver.core.UDTValue" );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, COOQLResultSet.class.getSimpleName() );
		addClassTypeImport( workingBuffer, COOQLRow.class.getSimpleName() );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, TypeConverter.class.getSimpleName() );
		addBlankLine( workingBuffer );
		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructorHeader( workingBuffer, PACKAGE, classType+"_"+tableName,
				COOQLResultSet.class.getSimpleName()+" resultSet, Row row" );
		addMethodContent( workingBuffer, "super( resultSet, row );" );
		addMethodFooter( workingBuffer );

		// Add column inclusions
		String columnName, accessorMethod = null, returnType, returnType2;
		Iterator<ColumnMetadata> columnIterator = tableMetadata.getColumns().iterator();
		ColumnMetadata columnMetadata;

		while (columnIterator.hasNext())
		{
			columnMetadata = columnIterator.next();
			columnName = columnMetadata.getName();
			addBlankLine( workingBuffer );

			returnType = getTypeFromValidator( columnMetadata.getType(), true );
			addMethodHeader( workingBuffer, PUBLIC, returnType, "get"+capitalize( columnName ), null );
			if ((returnType.startsWith( "List" )) && (returnType.indexOf("UDTValue") != -1))
			{
				addMethodContent( workingBuffer, "List returnList = wrapUDTValueList( row.getList( \"\\\"", columnName, "\\\"\", UDTValue.class ), " );
				addMethodContent( workingBuffer, "\t\t", returnType.substring( returnType.indexOf( '<' ) +1, returnType.indexOf( '_' ) ), "Impl",
						returnType.substring( returnType.indexOf( "_" ), returnType.indexOf( '>' ) ), ".class ); " ); 
				addMethodContent( workingBuffer, "return returnList;" );
			}
			else
			{
				accessorMethod = getAccessorMethodByType( columnName, returnType, true );
				addMethodContent( workingBuffer, "return row.", accessorMethod );
			}
			addMethodFooter( workingBuffer );

			addBlankLine( workingBuffer );
			returnType2 = getTypeFromValidator( columnMetadata.getType(), false );
			addMethodHeader( workingBuffer, PUBLIC, "Object", "getConverted"+capitalize( columnName ), null );
			if ((returnType.startsWith( "List" )) && (returnType.indexOf("UDTValue") != -1))
			{
				addMethodContent( workingBuffer, "List returnList = wrapUDTValueList( row.getList( \"\\\"", columnName, "\\\"\", UDTValue.class ), " );
				addMethodContent( workingBuffer, "\t\t", returnType.substring( returnType.indexOf( '<' ) +1, returnType.indexOf( '_' ) ), "Impl",
						returnType.substring( returnType.indexOf( "_" ), returnType.indexOf( '>' ) ), ".class ); " ); 
				addMethodContent( workingBuffer, "return returnList;" );
			}
			else
			{
				accessorMethod = getAccessorMethodByType( columnName, returnType, true );
				addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
				addMethodContent( workingBuffer, "TypeConverter tc =  dh.getConverter( \"", tableName, "\", \"", columnName, "\" );" );
				addMethodContent( workingBuffer, returnType+" sourceObject = row.", accessorMethod );
				addMethodContent( workingBuffer, "return tc.convertFromStorageFormat( sourceObject );" );
			}
			addMethodFooter( workingBuffer );
		}

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildUserInterfaceTypes( KeyspaceMetadata metadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = COOQLUDTValue.class.getSimpleName();

		Collection<UserType> userTypeList = metadata.getUserTypes();
		Iterator<UserType> userTypeIterator = userTypeList.iterator();
		String userTypeName;
		UserType userType;
		Iterator<String> fieldNameIterator;
		String fieldName, fieldType;

		while (userTypeIterator.hasNext())
		{
			userType = userTypeIterator.next();
			userTypeName = userType.getTypeName();

			workingBuffer.setLength( 0 );
			addPackage( workingBuffer, packageName );
			addImport( workingBuffer, "java.util.Date" );
			addBlankLine( workingBuffer );
			addClassTypeImport( workingBuffer, classType );
			addBlankLine( workingBuffer );

			setIndentation( 0 );
			workingBuffer.append( indentation ).append( PUBLIC ).append( " " ).append( "interface " )
					.append( classType ).append( "_" ).append( userTypeName ).append( " extends " )
					.append( classType ).append( "\r\n" ).append( indentation ).append( "{" );

			setIndentation( 1 );

			fieldNameIterator = userType.getFieldNames().iterator();
			while (fieldNameIterator.hasNext())
			{
				fieldName = fieldNameIterator.next();
				fieldType = getTypeFromValidator( userType.getFieldType( "\""+fieldName+"\"" ), true );
				addBlankLine( workingBuffer );
				workingBuffer.append( indentation ).append( PUBLIC ).append( " " ).append( fieldType ).append( " " )
						.append( "get" ).append( capitalize( fieldName ) ).append( "();\r\n" );
			}

			fieldNameIterator = userType.getFieldNames().iterator();
			while (fieldNameIterator.hasNext())
			{
				fieldName = fieldNameIterator.next();
				fieldType = getTypeFromValidator( userType.getFieldType( "\""+fieldName+"\"" ), true );
				addBlankLine( workingBuffer );
				workingBuffer.append( indentation ).append( "void " ).append( "set" ).append( capitalize( fieldName ) ).append( "(" )
						.append( fieldType+" "+fieldName ).append( " );\r\n" );

				if (fieldType.equals( "Date" ))
				{
					addBlankLine( workingBuffer );
					workingBuffer.append( indentation ).append( "void " ).append( "set" ).append( capitalize( fieldName ) ).append( "(" )
							.append( "long "+fieldName ).append( " );\r\n" );
				}
			}

			addClassFooter( workingBuffer );

			writeClassFile( packageName, classType, userTypeName, workingBuffer.toString() );
		}
	}

	static final void buildUserTypes( KeyspaceMetadata metadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = COOQLUDTValue.class.getSimpleName()+"Impl";

		Collection<UserType> userTypeList = metadata.getUserTypes();
		Iterator<UserType> userTypeIterator = userTypeList.iterator();
		String userTypeName;
		UserType userType;
		Iterator<String> fieldNameIterator;
		String fieldName, fieldType;

		while (userTypeIterator.hasNext())
		{
			userType = userTypeIterator.next();
			userTypeName = userType.getTypeName();

			workingBuffer.setLength( 0 );
			addPackage( workingBuffer, packageName );
			addImport( workingBuffer, "java.util.Date" );
			addBlankLine( workingBuffer );
			addImport( workingBuffer, "com.datastax.driver.core.UDTValue" );
			addBlankLine( workingBuffer );
			addClassTypeImport( workingBuffer, "COOQLUDTValueImpl" );
			addBlankLine( workingBuffer );

			setIndentation( 0 );
			workingBuffer.append( indentation ).append( PUBLIC ).append( " " ).append( "class " )
					.append( classType ).append( "_" ).append( userTypeName ).append( " extends " ).append( classType )
					.append( " implements " ).append( COOQLUDTValue.class.getSimpleName() ).append( "_" )
					.append( userTypeName ).append( "\r\n" ).append( indentation ).append( "{\r\n" );

			setIndentation( 1 );

			fieldNameIterator = userType.getFieldNames().iterator();
			while (fieldNameIterator.hasNext())
			{
				fieldName = fieldNameIterator.next();
				fieldType = getTypeFromValidator( userType.getFieldType( "\""+fieldName+"\"" ), true );
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, fieldType, "get"+capitalize( fieldName ), null );
				addMethodContent( workingBuffer, "return udtValue.", getAccessorMethodByType( fieldName, fieldType, true ) );
				addMethodFooter( workingBuffer );
			}

			fieldNameIterator = userType.getFieldNames().iterator();
			while (fieldNameIterator.hasNext())
			{
				fieldName = fieldNameIterator.next();
				fieldType = getTypeFromValidator( userType.getFieldType( "\""+fieldName+"\"" ), true );
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, "void", "set"+capitalize( fieldName ), fieldType+" "+fieldName );
				addMethodContent( workingBuffer, "udtValue.", getAccessorMethodByType( fieldName, fieldType, false ) );
				addMethodFooter( workingBuffer );

				if (fieldType.equals( "Date" ))
				{
					addBlankLine( workingBuffer );
					addMethodHeader( workingBuffer, PUBLIC, "void", "set"+capitalize( fieldName ), "long "+fieldName );
					addMethodContent( workingBuffer, "udtValue.setDate( \"\\\"", fieldName, "\\\"\", new Date( ", fieldName , " ) );" );
					addMethodFooter( workingBuffer );
				}
			}

			addClassFooter( workingBuffer );

			writeClassFile( packageName, classType, userTypeName, workingBuffer.toString() );
		}
	}

	static final void buildDelete( KeyspaceMetadata metadata, StringBuilder workingBuffer, String packageName )
	{
		buildCommand( workingBuffer, metadata, "Delete", "DeleteFrom", "FROM", true, "DELETE []", packageName );
	}

	static final void buildDeleteFrom( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildFrom( tableMetadata, workingBuffer, packageName, "Delete" );
	}

	static final void buildDeleteOperators( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildOperators( tableMetadata, workingBuffer, packageName, "Delete" );
	}

	static final void buildDeleteWheres( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildWheres( tableMetadata, workingBuffer, packageName, "Delete" );
	}

	static final void buildInsert( KeyspaceMetadata metadata, StringBuilder workingBuffer, String packageName )
	{
		buildCommand( workingBuffer, metadata, "Insert", "InsertInto", "INTO", true, "INSERT", packageName );
	}

	static final void buildInsertInto( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = InsertInto.class.getSimpleName();
		String tableName = tableMetadata.getName();
		String insertValuesType = buildClassName( InsertValues.class.getSimpleName(), tableName );
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();

		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.util.Date" );
		addBlankLine( workingBuffer );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, classType );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructor( workingBuffer, classType, tableName );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, insertValuesType, "VALUES",
				getParameterDeclarationList( partitionKeyComponentList, false )
						+ ((clusteringKeyComponentList.size() != 0)?(", "+getParameterDeclarationList( clusteringKeyComponentList, false )):""));
		addMethodContent( workingBuffer, "return new ", insertValuesType, "( XgetCOOQL(), ", 
				getParameterList( partitionKeyComponentList )+((clusteringKeyComponentList.size() != 0)?(", "+getParameterList( clusteringKeyComponentList )):""),
				" );" );
		addMethodFooter( workingBuffer );

		if ((requiresDateAlternate( partitionKeyComponentList )) || (requiresDateAlternate( clusteringKeyComponentList )))
		{
			addBlankLine( workingBuffer );
			addMethodHeader( workingBuffer, PUBLIC, insertValuesType, "VALUES",
					getParameterDeclarationList( partitionKeyComponentList, true )
							+ ((clusteringKeyComponentList.size() != 0)?(", "+getParameterDeclarationList( clusteringKeyComponentList, true )):""));
			addMethodContent( workingBuffer, "return new ", insertValuesType, "( XgetCOOQL(), ", 
					getParameterList( partitionKeyComponentList )+((clusteringKeyComponentList.size() != 0)?(", "+getParameterList( clusteringKeyComponentList )):""),
					" );" );
			addMethodFooter( workingBuffer );
		}

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildInsertValues( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = InsertValues.class.getSimpleName();
		String tableName = tableMetadata.getName();
		String clusteringKeyComponent, partitionKeyComponent;
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();
		
		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.nio.ByteBuffer" );
		addImport( workingBuffer, "java.util.Date" );
		addImport( workingBuffer, "java.util.List" );
		addImport( workingBuffer, "java.util.Map" );
		addBlankLine( workingBuffer );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, TypeConverter.class.getSimpleName() );
		addClassTypeImport( workingBuffer, classType );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );
		addConstructor( workingBuffer, classType, tableName );

		// Append index constructor
		addBlankLine( workingBuffer );
		addConstructorHeader( workingBuffer, PUBLIC, classType+"_"+tableName,
				COOQL.class.getSimpleName()+" cooql, "+getParameterDeclarationList( partitionKeyComponentList, false )
						+ ((clusteringKeyComponentList.size() > 0)?(", "+getParameterDeclarationList( clusteringKeyComponentList, false )):"" ) );
		addMethodContent( workingBuffer, "super( cooql );" );
		addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
		for (int p=0; p<partitionKeyComponentList.size(); p++)
		{
			partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", partitionKeyComponent,
					"\\\"\" )+\",\" );" );
			addMethodContent( workingBuffer, "dh.getParameterList().add( ", partitionKeyComponent, " );" );
		}
		for (int c=0; c<clusteringKeyComponentList.size(); c++)
		{
			clusteringKeyComponent = clusteringKeyComponentList.get( c ).getName();
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", clusteringKeyComponent,
					"\\\"\" )+\",\" );" );
			addMethodContent( workingBuffer, "dh.getParameterList().add( ", clusteringKeyComponent, " );" );
		}
		workingBuffer.append( "\t}\r\n" );

		if ((requiresDateAlternate( partitionKeyComponentList )) || (requiresDateAlternate( clusteringKeyComponentList )))
		{
			addBlankLine( workingBuffer );
			addConstructorHeader( workingBuffer, PUBLIC, classType+"_"+tableName,
					COOQL.class.getSimpleName()+" cooql, "+getParameterDeclarationList( partitionKeyComponentList, true )
							+ ((clusteringKeyComponentList.size() > 0)?(", "+getParameterDeclarationList( clusteringKeyComponentList, true )):"" ) );
			blockBuffer.setLength( 0 );
			for (int p=0; p<partitionKeyComponentList.size(); p++)
			{
				partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
				if (partitionKeyComponentList.get( p ).getType().getName().name().equals( "TIMESTAMP" )) 
				{
					blockBuffer.append( "new Date( " ).append( partitionKeyComponent ).append( " )" );
				}
				else blockBuffer.append( partitionKeyComponent );
				if (p < partitionKeyComponentList.size() -1) blockBuffer.append( ", " );
			}
			if (clusteringKeyComponentList.size() > 0) blockBuffer.append( ", " );
			for (int c=0; c<clusteringKeyComponentList.size(); c++)
			{
				clusteringKeyComponent = clusteringKeyComponentList.get( c ).getName();
				if (clusteringKeyComponentList.get( c ).getType().getName().name().equals( "TIMESTAMP" )) 
				{
					blockBuffer.append( "new Date( " ).append( clusteringKeyComponent ).append( " )" );
				}
				else 	blockBuffer.append( clusteringKeyComponent );
				if (c < clusteringKeyComponentList.size() -1) blockBuffer.append( ", " );
			}
			addMethodContent( workingBuffer, "this( cooql, ", blockBuffer.toString(), " );"  );
			addMethodFooter( workingBuffer );
		}

		addColumnSetters( workingBuffer, tableMetadata, classType, tableName, partitionKeyComponentList,
				clusteringKeyComponentList, "," );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildSelect( KeyspaceMetadata metadata, StringBuilder workingBuffer, String packageName )
	{
		buildCommand( workingBuffer, metadata, "Select", "SelectFrom", "FROM", true, "SELECT []", packageName );
	}

	static final void buildSelectFrom( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildFrom( tableMetadata, workingBuffer, packageName, "Select" );
	}

	static final void buildSelectLimit( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = SelectLimit.class.getSimpleName();
		String tableName = tableMetadata.getName();
		
		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, classType );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructor( workingBuffer, classType, tableName );

		addExecuteQuery( workingBuffer, tableName );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildSelectOperators( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildOperators( tableMetadata, workingBuffer, packageName, "Select" );
	}

	static final void buildSelectOrderBy( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		if (clusteringKeyComponentList.size() == 0) return; // No clustering column to order by so exit

		String tableName = tableMetadata.getName();
		String scopedTableName;
		String clusteringKeyComponent;
		String classType = SelectOrderBy.class.getSimpleName();
		
		int cc = 1;
		do
		{
			String suffix = "_BY";
			for (int c=0; c<cc; c++)
			{
				suffix += "_";
				suffix += clusteringKeyComponentList.get( c ).getName();
			}
			scopedTableName = tableName+suffix;
			String selectOrderByOperatorType = buildClassName( SelectOrderByOperator.class.getSimpleName(), scopedTableName );

			workingBuffer.setLength( 0 );
			addPackage( workingBuffer, packageName );
			addCOOQLImport( workingBuffer );
			addClassTypeImport( workingBuffer, classType );
			addBlankLine( workingBuffer );

			setIndentation( 0 );
			addClassHeader( workingBuffer, PUBLIC, classType, scopedTableName );

			setIndentation( 1 );

			addConstructor( workingBuffer, classType, scopedTableName );

			if (cc <= clusteringKeyComponentList.size())
			{
				clusteringKeyComponent = clusteringKeyComponentList.get( cc -1 ).getName();
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, selectOrderByOperatorType, "_"+clusteringKeyComponent, null );
				addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \""+((cc -1 > 0)?",":"")+" \\\"", clusteringKeyComponent, "\\\"\" );" );
				addMethodContent( workingBuffer, "return new ", selectOrderByOperatorType, "( XgetCOOQL() );" );
				addMethodFooter( workingBuffer );
			}

			addLimit( workingBuffer, tableName );

			addClassFooter( workingBuffer );

			writeClassFile( packageName, classType, scopedTableName, workingBuffer.toString() );
		}
		while (cc++ < clusteringKeyComponentList.size());
	}

	static final void buildSelectOrderByOperators( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		String classType = SelectOrderByOperator.class.getSimpleName();
		String tableName = tableMetadata.getName();
		String selectOrderByDirectionType = buildClassName( SelectOrderByDirection.class.getSimpleName(), tableName );
		String clusteringKeyComponent;
		String scopedTableName;
		
		int cc = Math.min( 1, clusteringKeyComponentList.size() );
		do
		{
			String suffix = "_BY";
			for (int c=0; c<cc; c++)
			{
				suffix += "_";
				suffix += clusteringKeyComponentList.get( c ).getName();
			}
			scopedTableName = tableName+suffix;

			workingBuffer.setLength( 0 );
			addPackage( workingBuffer, packageName );
			addBlankLine( workingBuffer );
			addCOOQLImport( workingBuffer );
			addBlankLine( workingBuffer );
			addClassTypeImport( workingBuffer, classType );
			addBlankLine( workingBuffer );
	
			setIndentation( 0 );
			addClassHeader( workingBuffer, PUBLIC, classType, scopedTableName );
	
			setIndentation( 1 );
	
			addConstructor( workingBuffer, classType, scopedTableName );
			addBlankLine( workingBuffer );
	
			if (cc < clusteringKeyComponentList.size())
			{
				String selectOrderByOperatorType = buildClassName( SelectOrderByOperator.class.getSimpleName(), scopedTableName );
				clusteringKeyComponent = clusteringKeyComponentList.get( cc ).getName();
				addMethodHeader( workingBuffer, PUBLIC, selectOrderByOperatorType, "_"+clusteringKeyComponent, null );
				addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \""+((cc > 0)?",":"")+" \\\"", clusteringKeyComponent, "\\\"\" );" );
				addMethodContent( workingBuffer, "return new ", selectOrderByOperatorType, "( XgetCOOQL() );" );
				addMethodFooter( workingBuffer );
				addBlankLine( workingBuffer );
			}
	
			addMethodHeader( workingBuffer, PUBLIC, selectOrderByDirectionType, "ASC", null );
			addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \" ASC\" );" );
			addMethodContent( workingBuffer, "return new ", selectOrderByDirectionType, "( XgetCOOQL() );" );
			addMethodFooter( workingBuffer );
			addBlankLine( workingBuffer );
	
			addMethodHeader( workingBuffer, PUBLIC, selectOrderByDirectionType, "DESC", null );
			addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \" DESC\" );" );
			addMethodContent( workingBuffer, "return new ", selectOrderByDirectionType, "( XgetCOOQL() );" );
			addMethodFooter( workingBuffer );
	
			addExecuteQuery( workingBuffer, tableName );
	
			addClassFooter( workingBuffer );
	
			writeClassFile( packageName, classType, scopedTableName, workingBuffer.toString() );
		}
		while (cc++ < clusteringKeyComponentList.size());
	}

	static final void buildSelectOrderByDirection( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = SelectOrderByDirection.class.getSimpleName();
		String tableName = tableMetadata.getName();
		
		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addBlankLine( workingBuffer );
		addCOOQLImport( workingBuffer );
		addBlankLine( workingBuffer );
		addClassTypeImport( workingBuffer, classType );
		addClassTypeImport( workingBuffer, SelectLimit.class.getSimpleName() );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructor( workingBuffer, classType, tableName );

		addLimit( workingBuffer, tableName );

		addExecuteQuery( workingBuffer, tableName );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildSelectValues( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = SelectValues.class.getSimpleName();
		String tableName = tableMetadata.getName();
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();

		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.nio.ByteBuffer" );
		addImport( workingBuffer, "java.util.Date" );
		addImport( workingBuffer, "java.util.List" );
		addBlankLine( workingBuffer );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, classType );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructor( workingBuffer, classType, tableName );

		addBlankLine( workingBuffer );
		addConstructorHeader( workingBuffer, PACKAGE, classType+"_"+tableName,
				COOQL.class.getSimpleName()+" cooql, "+getParameterDeclarationList( partitionKeyComponentList, false ) );
		addMethodContent( workingBuffer, "super( cooql );" );
		addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
		String partitionKeyComponent;
		for (int p=0; p<partitionKeyComponentList.size(); p++)
		{
			partitionKeyComponent = partitionKeyComponentList.get( p  ).getName();
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", partitionKeyComponent, "\\\"\" )+\",\" );" );
			addMethodContent( workingBuffer, "dh.getParameterList().add( ", partitionKeyComponent, " );" );
		}
		addMethodFooter( workingBuffer );

		if (requiresDateAlternate( partitionKeyComponentList ))
		{
			addBlankLine( workingBuffer );
			addConstructorHeader( workingBuffer, PACKAGE, classType+"_"+tableName,
					COOQL.class.getSimpleName()+" cooql, "+getParameterDeclarationList( partitionKeyComponentList, true ) );
			addMethodContent( workingBuffer, "super( cooql );" );
			addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
			for (int p=0; p<partitionKeyComponentList.size(); p++)
			{
				partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
				addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", partitionKeyComponent, "\\\"\" )+\",\" );" );
				addMethodContent( workingBuffer, "dh.getParameterList().add( ", partitionKeyComponent, " );" );
			}
			addMethodFooter( workingBuffer );
		}

		String columnName;
		Iterator<ColumnMetadata> columnIterator = tableMetadata.getColumns().iterator();
		ColumnMetadata columnMetadata;

		while (columnIterator.hasNext())
		{
			columnMetadata = columnIterator.next();
			columnName = columnMetadata.getName();
			if (isKeyColumn( columnName, partitionKeyComponentList )) continue;
			addBlankLine( workingBuffer );
			addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName,
					getTypeFromValidator( columnMetadata.getType(), true )+" "+columnName );
			addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", columnName, "\\\"\" )+\",\" );" );
			addMethodContent( workingBuffer, "dh.getParameterList().add( ", columnName, " );" );
			addMethodContent( workingBuffer, "return this;" );
			addMethodFooter( workingBuffer );

			if (getTypeFromValidator( columnMetadata.getType(), true ).equals( "Date" ))
			{
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName,
						"long "+columnName );
				addMethodContent( workingBuffer, "return _", columnName, "( new Date( ", columnName, " ) );" );
				addMethodFooter( workingBuffer );
			}
		}

		addExecuteQuery( workingBuffer, tableName );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildSelectWheres( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildWheres( tableMetadata, workingBuffer, packageName, "Select" );
	}

	static final void buildUpdate( KeyspaceMetadata metadata, StringBuilder workingBuffer, String packageName )
	{
		buildCommand( workingBuffer, metadata, "Update", "UpdateValues", "SET", false, "UPDATE ", packageName );
	}

	static final void buildUpdateOperators( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildOperators( tableMetadata, workingBuffer, packageName, "Update" );
	}

	static final void buildUpdateValues( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		String classType = UpdateValues.class.getSimpleName();
		String tableName = tableMetadata.getName();
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();
		String suffix = buildPartitionSuffix( partitionKeyComponentList );
		String updateWhereType = buildClassName( UpdateWhere.class.getSimpleName(), tableName+suffix );

		String partitionKeyComponent;

		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.nio.ByteBuffer" );
		addImport( workingBuffer, "java.util.Date" );
		addImport( workingBuffer, "java.util.List" );
		addImport( workingBuffer, "java.util.Map" );
		addBlankLine( workingBuffer );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, TypeConverter.class.getSimpleName() );
		addClassTypeImport( workingBuffer, classType );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructor( workingBuffer, classType, tableName );

		addColumnSetters( workingBuffer, tableMetadata, classType, tableName, partitionKeyComponentList,
				clusteringKeyComponentList, "=?," );

		// Add where method
		addBlankLine( workingBuffer );
		blockBuffer.setLength( 0 );
		blockBuffer.append( "WHERE" );
		for (int p = 0; p<partitionKeyComponentList.size(); p++)
		{
			partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
			blockBuffer.append( "_" ).append( partitionKeyComponent );
			if (p<partitionKeyComponentList.size() -1) blockBuffer.append( "_AND" );
		}
		blockBuffer.append( "_EQ" );
		addMethodHeader( workingBuffer, PUBLIC, updateWhereType, blockBuffer.toString(),
				getParameterDeclarationList( partitionKeyComponentList, false ) );
		addMethodContent( workingBuffer, updateWhereType, " updateWhere" );
		addMethodContent( workingBuffer, "\t\t= new ", updateWhereType, "( XgetCOOQL(), ",
				getParameterList( partitionKeyComponentList ), " );" );
		addMethodContent( workingBuffer, "return updateWhere;" );
		addMethodFooter( workingBuffer );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	static final void buildUpdateWheres( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName )
	{
		buildWheres( tableMetadata, workingBuffer, packageName, "Update" );
	}

	private static void addBlankLine( StringBuilder workingBuffer )
	{
		workingBuffer.append( "\r\n" );
	}

	private static void addClassFooter( StringBuilder workingBuffer )
	{
		workingBuffer.append( "}\r\n" );
	}

	private static void addClassHeader( StringBuilder workingBuffer, String permissions, String classType,
			String tableName )
	{
		workingBuffer.append( indentation );
		if ( permissions != null ) workingBuffer.append( permissions ).append( " " );
		workingBuffer.append( "class " ).append( classType ).append( "_" ).append( tableName )
				.append( " extends " ).append( classType ).append( "\r\n" ).append( indentation ).append( "{\r\n" );
	}

	private static void addClassTypeImport( StringBuilder workingBuffer, String classType )
	{
		workingBuffer.append( "import com.onbalancetech.cooql." ).append( classType ).append( ";\r\n" );
	}

	private static void addCOOQLImport( StringBuilder workingBuffer )
	{
		workingBuffer.append( "import ").append( COOQL.class.getCanonicalName() ).append( ";\r\n" );
	}

	private static void addColumnSetters( StringBuilder workingBuffer, TableMetadata tableMetadata, String classType,
			String tableName, List<ColumnMetadata> partitionKeyComponentList,
			List<ColumnMetadata> clusteringKeyComponentList, String operationSuffix )
	{
		String columnName, dataType, dataType2, parameterListEntry;

		Iterator<ColumnMetadata> columnIterator = tableMetadata.getColumns().iterator(); // TODO restore once bug is fixed
		ColumnMetadata columnMetadata;

		while (columnIterator.hasNext())
		{
			columnMetadata = columnIterator.next();
			columnName = columnMetadata.getName();
			if ((isKeyColumn( columnName, partitionKeyComponentList ))
					|| (isKeyColumn( columnName, clusteringKeyComponentList ))) continue;
			addBlankLine( workingBuffer );
			dataType = getTypeFromValidator( columnMetadata.getType(), true );
			if (dataType.equals( "Date" ))
			{
				addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName,
						"long "+columnName );
				addMethodContent( workingBuffer, "return _", columnName, "( new Date( ", columnName, " ) );" );
				addMethodFooter( workingBuffer );
				addBlankLine( workingBuffer );
			}

			addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName,
					dataType+" "+columnName );
			addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", columnName, "\\\"\" )+\"", operationSuffix, "\" );" );
			parameterListEntry = columnName;
			if (dataType.startsWith( "ListType" ))
			{
				parameterListEntry =  "toByteArrayList( "+columnName+" ) ";
			}
			addMethodContent( workingBuffer, "dh.getParameterList().add( ", parameterListEntry, " );" );
			addMethodContent( workingBuffer, "return this;" );
			addMethodFooter( workingBuffer );

			dataType2 = getTypeFromValidator( columnMetadata.getType(), false );
			if (!dataType.equals( dataType2 ))
			{
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName,
						dataType2+" "+columnName );
				addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
				addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", columnName, "\\\"\" )+\"", operationSuffix, "\" );" );
				parameterListEntry = columnName;
				if (dataType2.startsWith( "ListType" ))
				{
					parameterListEntry =  "toByteArrayList( "+columnName+" ) ";
				}
				addMethodContent( workingBuffer, "dh.getParameterList().add( ", parameterListEntry, " );" );
				addMethodContent( workingBuffer, "return this;" );
				addMethodFooter( workingBuffer );
			}

			addBlankLine( workingBuffer );
			addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName+"_WITH",
					"Object "+columnName );
			addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
			addMethodContent( workingBuffer, "TypeConverter tc =  dh.getConverter( \"", tableName, "\", \"", columnName, "\" );" );
			addMethodContent( workingBuffer, "if (tc == null) throw new IllegalStateException( \"No type converter defined for table '",
					tableName, "', column '", columnName, "'\" );" );
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", columnName, "\\\"\" )+\"", operationSuffix, "\" );" );
			parameterListEntry = columnName;
			if (dataType2.startsWith( "ListType" ))
			{
				parameterListEntry =  "toByteArrayList( "+columnName+" ) ";
			}
			addMethodContent( workingBuffer, "dh.getParameterList().add( tc.convertToStorageFormat( ",
					parameterListEntry, " ) );" );
			addMethodContent( workingBuffer, "return this;" );
			addMethodFooter( workingBuffer );
		}
	}

	private static void addConstructor( StringBuilder workingBuffer, String classType, String tableName )
	{
		workingBuffer.append( indentation ).append( classType ).append( "_" ).append( tableName )
				.append( "( " ).append( COOQL.class.getName() ).append( " cooql )\r\n" ).append( indentation )
				.append( "{\r\n" ).append( indentation ).append( "\tsuper( cooql );\r\n" ).append( indentation ).append( "}\r\n" );
	}

	private static void addConstructorHeader( StringBuilder workingBuffer, String permissions,
			String name, String parameterList )
	{
		workingBuffer.append( indentation );
		if ( permissions.length() != 0 ) workingBuffer.append( permissions ).append( " " );
		workingBuffer.append( name );
		if (parameterList != null) workingBuffer.append( "( " ).append( parameterList ).append( " )\r\n" );
		else workingBuffer.append( "()\r\n" );
		workingBuffer.append( indentation ).append( "{\r\n" );
	}

	private static void addExecuteQuery( StringBuilder workingBuffer, String tableName )
	{
		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, COOQLResultSet.class.getSimpleName()+"_"+tableName, "executeQuery", null );
		addMethodContent( workingBuffer, "return new ", COOQLResultSet.class.getSimpleName(), "_", tableName,
				"( Xexecute(), ", COOQL.class.getSimpleName(), ".getDataHolder() );" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, COOQLResultSetFuture.class.getSimpleName()+"_"+tableName,
				"executeQueryAsynchronously", null );
		addMethodContent( workingBuffer, "return new ", COOQLResultSetFuture.class.getSimpleName(), "_", tableName,
				"( XexecuteAsynchronously(), ", COOQL.class.getSimpleName(), ".getDataHolder() );" );
		addMethodFooter( workingBuffer );
	}

	private static void addImport( StringBuilder workingBuffer, String importClass )
	{
		workingBuffer.append( "import " ).append( importClass).append( ";\r\n" );
	}

	private static void addLimit( StringBuilder workingBuffer, String tableName )
	{
		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, SelectLimit.class.getSimpleName()+"_"+tableName, "LIMIT", "int itemCount" );
		addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \" LIMIT ?\" );" );
		addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().addParameter( itemCount );" );
		addMethodContent( workingBuffer, "return new SelectLimit_", tableName, "( XgetCOOQL() );" );
		addMethodFooter( workingBuffer );
	}

	private static void addMethodContent( StringBuilder workingBuffer, String... content )
	{
		workingBuffer.append( indentation ).append( "\t" );
		for (int i=0; i<content.length; i++)
		{
			workingBuffer.append( content[ i ] );
		}
		workingBuffer.append( "\r\n" );
	}

	private static void addMethodFooter( StringBuilder workingBuffer )
	{

		workingBuffer.append( indentation ).append( "}\r\n" );
	}

	private static void addMethodHeader( StringBuilder workingBuffer, String permissions, String returnType,
			String name, String parameterList )
	{
		workingBuffer.append( indentation );
		if ( permissions.length() != 0 ) workingBuffer.append( permissions ).append( " " );
		workingBuffer.append( returnType ).append( " " ).append( name ).append( "(" );
		if (parameterList != null) workingBuffer.append( " " ).	append( parameterList ).append( " " );
		workingBuffer.append( ")\r\n" ).append( indentation ).append( "{\r\n" );
	}

	private static void addPackage( StringBuilder workingBuffer, String packageName )
	{
		workingBuffer.append( "package ").append( packageName ).append( ";\r\n\r\n" );
	}

	private static String buildClassName( String classType, String tableName )
	{
		blockBuffer.setLength( 0 );
		blockBuffer.append( classType).append( "_" ).append( tableName );
		return blockBuffer.toString();
	}

	private static void buildCommand( StringBuilder workingBuffer, KeyspaceMetadata metadata, String classType,
			String returnClassType, String COOQLText, boolean isCOOQLTextFirst, String toStringText, String packageName )
	{
		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, "Command" );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		workingBuffer.append( "public class " ).append(  classType ).append( " extends Command\r\n{\r\n" );

		setIndentation( 1 );
		workingBuffer.append( indentation ).append( classType ).append( "( " ).append( COOQL.class.getSimpleName() )
			.append( " cooql )\r\n" ).append( indentation ).append( "{\r\n" ).append( indentation ).append( "\tsuper( cooql );\r\n" )
			.append( indentation ).append( "}" );
		addBlankLine( workingBuffer );

		Collection<TableMetadata> tableCollection = metadata.getTables();
		Iterator<TableMetadata> tableIterator = tableCollection.iterator();

		String tableName, returnClassTypeVariable = returnClassType.substring( 0, 1 ).toLowerCase()
				+ returnClassType.substring( 1 );

		// Iterate over each table/column family in the keyspace 
		while (tableIterator.hasNext())
		{
			tableName = tableIterator.next().getName();
			addBlankLine( workingBuffer );
			addMethodHeader( workingBuffer, PUBLIC, returnClassType+"_"+tableName,
					(isCOOQLTextFirst)?(COOQLText+"_"+tableName):(tableName+"_"+COOQLText), null );
			addMethodContent( workingBuffer, returnClassType+"_", tableName, " "+returnClassTypeVariable );
			addMethodContent( workingBuffer, "\t\t= new "+returnClassType+"_", tableName, "( XgetCOOQL() );" );
			addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( ", returnClassTypeVariable, " );" );
			addMethodContent( workingBuffer, "return ", returnClassTypeVariable, ";" );
			addMethodFooter( workingBuffer );
		}

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, "String", "toString", null );
		addMethodContent( workingBuffer, "return \"", toStringText, "\";" );
		addMethodFooter( workingBuffer );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, null, workingBuffer.toString() );
	}

	private static void buildFrom( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName,
			String classMajorType )
	{
		String classType = classMajorType+From.class.getSimpleName();
		String tableName = tableMetadata.getName();
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();
		String suffix = buildPartitionSuffix( partitionKeyComponentList );
		String whereType = buildClassName( classMajorType+Where.class.getSimpleName(), tableName+suffix );
		String operatorType = buildClassName( classMajorType+Operator.class.getSimpleName(), tableName+suffix );

		workingBuffer.setLength( 0 );
		addPackage( workingBuffer, packageName );
		addImport( workingBuffer, "java.util.Date" );
		addBlankLine( workingBuffer );
		addCOOQLImport( workingBuffer );
		addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
		addClassTypeImport( workingBuffer, "TypeConverter" );
		addClassTypeImport( workingBuffer, classType );
		addBlankLine( workingBuffer );

		setIndentation( 0 );
		addClassHeader( workingBuffer, PUBLIC, classType, tableName );

		setIndentation( 1 );

		addConstructor( workingBuffer, classType, tableName );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_COUNT_STAR", null );
		addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
		addMethodContent( workingBuffer, "dh.getBuffer().append( \"count(*),\" );" );
		addMethodContent( workingBuffer, "dh.addColumn( \"count\" );" );
		addMethodContent( workingBuffer, "return this;" );
		addMethodFooter( workingBuffer );

		addBlankLine( workingBuffer );
		addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_STAR", null );
		addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
		addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"*\" )+\",\" );" );
		addMethodContent( workingBuffer, "return this;" );
		addMethodFooter( workingBuffer );

		// Add column inclusions
		String columnName, partitionKeyComponent;
		Iterator<ColumnMetadata> columnIterator = tableMetadata.getColumns().iterator();
		ColumnMetadata columnMetadata;

		while (columnIterator.hasNext())
		{
			columnMetadata = columnIterator.next();
			columnName = columnMetadata.getName();
			if ((classMajorType.equals( Delete.class.getSimpleName() )) &&
				(isKeyColumn( columnName, partitionKeyComponentList ))) continue;
			addBlankLine( workingBuffer );
			addMethodHeader( workingBuffer, PUBLIC, classType+"_"+tableName, "_"+columnName, null );
			addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
			addMethodContent( workingBuffer, "dh.getBuffer().append( dh.addColumn( \"\\\"", columnName, "\\\"\" )+\",\" );" );
			addMethodContent( workingBuffer, "return this;" );
			addMethodFooter( workingBuffer );
		}

		// Add where clause
		addBlankLine( workingBuffer );
		blockBuffer.setLength( 0 );
		blockBuffer.append( "WHERE" );
		for (int p = 0; p<partitionKeyComponentList.size(); p++)
		{
			partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
			blockBuffer.append( "_" ).append( partitionKeyComponent );
			if (p<partitionKeyComponentList.size() -1) blockBuffer.append( "_AND" );
		}
		blockBuffer.append( "_EQ" );
		addMethodHeader( workingBuffer, PUBLIC, operatorType, blockBuffer.toString(),
				getParameterDeclarationList( partitionKeyComponentList, false ) );
		addMethodContent( workingBuffer, whereType, " where" );
		addMethodContent( workingBuffer, "\t\t= new ", whereType, "( XgetCOOQL(), ",
				getParameterList( partitionKeyComponentList ), " );" );
		addMethodContent( workingBuffer, operatorType, " operator" );
		addMethodContent( workingBuffer, "\t\t= new ", operatorType, "( XgetCOOQL() );" );
		addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
		addMethodContent( workingBuffer, "CharSequence cs = dh.getBuffer().subSequence( dh.getBuffer().indexOf( \"{\" )+1,"  );
		addMethodContent( workingBuffer, "\t\tdh.getBuffer().indexOf( \"}\" ) );" );
		addMethodContent( workingBuffer, "XappendFrom();" );
		addMethodContent( workingBuffer, "return operator;" );
		addMethodFooter( workingBuffer );

		addClassFooter( workingBuffer );

		writeClassFile( packageName, classType, tableName, workingBuffer.toString() );
	}

	private static void buildOperators( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName, String classMajorType )
	{
		String classMinorType = Operator.class.getSimpleName();
		String tableName = tableMetadata.getName();
		String scopedTableName;
		String classType = classMajorType+classMinorType;
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();

		int cc = 0;
		do
		{
			String suffix = buildPartitionSuffix( partitionKeyComponentList );
			for (int c=0; c<cc; c++)
			{
				suffix += "_AND_";
				suffix += clusteringKeyComponentList.get( c ).getName();
			}
			scopedTableName = tableName+suffix;

			String whereType = buildClassName( classMajorType+Where.class.getSimpleName(), scopedTableName );
			
			workingBuffer.setLength( 0 );
			addPackage( workingBuffer, packageName );
			addCOOQLImport( workingBuffer );
			addClassTypeImport( workingBuffer, classType );
			addBlankLine( workingBuffer );

			setIndentation( 0 );
			addClassHeader( workingBuffer, PUBLIC, classType, scopedTableName );

			setIndentation( 1 );

			addConstructor( workingBuffer, classType, scopedTableName );

			if (cc < clusteringKeyComponentList.size())	// Without clustering components, these operations are not possible
			{
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, whereType, "AND", null );
				addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \" AND\" );" );
				addMethodContent( workingBuffer, "return new ", whereType, "( XgetCOOQL() );" );
				addMethodFooter( workingBuffer );
			}

			if ((clusteringKeyComponentList.size() > 0) && (cc <= clusteringKeyComponentList.size()) && ("Select".equals( classMajorType )))	// Without clustering components, these operations are not possible
			{
				suffix = "_BY";
				int limit = Math.max( (clusteringKeyComponentList.size() > 0)?1:0, cc);
				for (int c=0; c<limit; c++)
				{
					suffix += "_";
					suffix += clusteringKeyComponentList.get( c ).getName();
				}

				String orderByType = buildClassName( classMajorType+OrderBy.class.getSimpleName(), tableName+suffix );

				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, orderByType, "ORDER_BY", null );
				addMethodContent( workingBuffer, orderByType, " orderBy" );
				addMethodContent( workingBuffer, "\t\t= new ", orderByType, "( XgetCOOQL() );" );
				addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \" \"+orderBy.toString() );" );
				addMethodContent( workingBuffer, "return orderBy;" );
				addMethodFooter( workingBuffer );
			}

			if (classMajorType == "Select")
			{
				addLimit( workingBuffer, tableName );

				addExecuteQuery( workingBuffer, tableName );
			}

			addClassFooter( workingBuffer );

			writeClassFile( packageName, classType, scopedTableName, workingBuffer.toString() );
		}
		while (cc++ < clusteringKeyComponentList.size());
	}

	private static String buildPartitionSuffix( List<ColumnMetadata> partitionKeyComponentList )
	{
		String suffix = "_BY_";
		for (int i=0; i<partitionKeyComponentList.size(); i++)
		{
			suffix += partitionKeyComponentList.get( i ).getName();
			if (i < partitionKeyComponentList.size() -1) suffix += "_AND_";
		}

		return suffix;
	}

	private static void buildWheres( TableMetadata tableMetadata, StringBuilder workingBuffer, String packageName,
			String classMajorType )
	{
		String classMinorType = Where.class.getSimpleName();
		String tableName = tableMetadata.getName();
		String classType = classMajorType+classMinorType;
		List<ColumnMetadata> partitionKeyComponentList = tableMetadata.getPartitionKey();
		List<ColumnMetadata> clusteringKeyComponentList = tableMetadata.getClusteringColumns();
		List<ColumnMetadata> columnList = tableMetadata.getColumns();

		String suffix = buildPartitionSuffix( partitionKeyComponentList );

		Iterator<ColumnMetadata> iterator = columnList.iterator();
		ColumnMetadata dataColumn;
		boolean hasIndex = false;
		while (iterator.hasNext())
		{
			dataColumn = iterator.next();
			if (dataColumn.getIndex() != null) { hasIndex = true; break; }
		}

		int cc = 0;
		String scopedTableName;
		do
		{
			String clusteringKeyColumnName, partitionKeyComponent;
			
			scopedTableName = tableName + suffix;
			if (cc < clusteringKeyComponentList.size())
			{
				suffix += "_AND_"+clusteringKeyComponentList.get( cc ).getName();
			}

			workingBuffer.setLength( 0 );
			addPackage( workingBuffer, packageName );
			addImport( workingBuffer, "java.util.Date" );
			addBlankLine( workingBuffer );
			addCOOQLImport( workingBuffer );
			addClassTypeImport( workingBuffer, COOQL.class.getSimpleName()+".DataHolder" );
			addClassTypeImport( workingBuffer, classType );
			addBlankLine( workingBuffer );

			setIndentation( 0 );
			addClassHeader( workingBuffer, PUBLIC, classType, scopedTableName );

			setIndentation( 1 );

			addConstructor( workingBuffer, classType, scopedTableName );

			if (cc == 0)
			{
				// Append partition key constructor
				addBlankLine( workingBuffer );
				addConstructorHeader( workingBuffer, PACKAGE, classType+"_"+scopedTableName,
						COOQL.class.getSimpleName()+" cooql, "+getParameterDeclarationList( partitionKeyComponentList, false ) );
				addMethodContent( workingBuffer, "super( cooql );" );
				addMethodContent( workingBuffer, "XcleanSet();" );
				addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
				for (int p = 0; p< partitionKeyComponentList.size(); p++)
				{
					partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
					addMethodContent( workingBuffer, "dh.getBuffer().append( \" \\\"", partitionKeyComponent, "\\\"=?\" );" );
					addMethodContent( workingBuffer, "dh.addParameter( ", partitionKeyComponent, " );" );
					if (p < partitionKeyComponentList.size() -1) addMethodContent( workingBuffer, "AND();" );
				}
				addMethodFooter( workingBuffer );
			}

			if ((cc <= clusteringKeyComponentList.size() -1) || (hasIndex))
			{
				// AND clause
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, classType+"_"+scopedTableName, "AND", null );
				addMethodContent( workingBuffer, COOQL.class.getSimpleName(), ".getDataHolder().getBuffer().append( \" AND\" );" );
				addMethodContent( workingBuffer, "return this;" );
				addMethodFooter( workingBuffer );
			}

			// Append secondary index checks
			iterator = columnList.iterator();
			String partitionKeyColumnName;
			while (iterator.hasNext())
			{
				dataColumn = iterator.next();
				if (dataColumn.getIndex() == null) continue;
				partitionKeyColumnName = dataColumn.getName();
				addBlankLine( workingBuffer );
				addMethodHeader( workingBuffer, PUBLIC, classMajorType+"Operator_"+scopedTableName,
						partitionKeyColumnName+"_EQ", 
						getTypeFromValidator( dataColumn.getType(), false )+" "+partitionKeyColumnName );
				addMethodContent( workingBuffer, classMajorType+"Operator_"+scopedTableName, " "+classMajorType.toLowerCase()+"Operator" );
				addMethodContent( workingBuffer, "\t\t= new "+classMajorType+"Operator_", scopedTableName, "( XgetCOOQL() );" );
				addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
				addMethodContent( workingBuffer, "dh.getBuffer().append( \" \\\"", partitionKeyColumnName, "\\\"=?\" );" );
				addMethodContent( workingBuffer, "dh.addParameter( ", partitionKeyColumnName, " );" );
				addMethodContent( workingBuffer, "return "+classMajorType.toLowerCase()+"Operator;" );
				addMethodFooter( workingBuffer );

				if (dataColumn.getType().getName().name().equals( "TIMESTAMP" ))
				{
					addBlankLine( workingBuffer );
					addMethodHeader( workingBuffer, PUBLIC, classMajorType+"Operator_"+scopedTableName, partitionKeyColumnName+"_EQ",
							"long "+partitionKeyColumnName );
					addMethodContent( workingBuffer, "return ", partitionKeyColumnName, "_EQ( new Date( ", partitionKeyColumnName, " ) );" );
					addMethodFooter( workingBuffer );
				}
			}

			// Append clustering key index comparison for =, >=, <=
			if ((cc < clusteringKeyComponentList.size()) && (clusteringKeyComponentList.get( cc ).getName().length() != 0 ))
			{
				String operatorName = null, operatorSymbol= null, operatorType, operatorTypeSuffix;
				ColumnMetadata clusteringKeyColumn = clusteringKeyComponentList.get( cc );

				operatorType = classMajorType+"Operator_"+scopedTableName;

				for (int i= 0; i<3; i++)
				{
					operatorTypeSuffix = "";
					if (i == 0)
					{
						operatorName = "_EQ"; operatorSymbol = "=";
						operatorTypeSuffix += "_AND_"+clusteringKeyComponentList.get( cc ).getName();
					}
					else if (i == 1) { operatorName = "_GTE"; operatorSymbol = ">="; }
					else if (i == 2) { operatorName = "_LTE"; operatorSymbol = "<="; }

					clusteringKeyColumnName = clusteringKeyColumn.getName();
					addBlankLine( workingBuffer );
					addMethodHeader( workingBuffer, PUBLIC, operatorType+operatorTypeSuffix, clusteringKeyColumnName+operatorName,
							getTypeFromValidator( clusteringKeyColumn.getType(), true )+" "+clusteringKeyColumnName );
					addMethodContent( workingBuffer, operatorType, operatorTypeSuffix, " ", classMajorType.toLowerCase(), "Operator" );
					addMethodContent( workingBuffer, "\t\t= new ", operatorType, operatorTypeSuffix, "( XgetCOOQL() );" );
					addMethodContent( workingBuffer, "DataHolder dh = ", COOQL.class.getSimpleName(), ".getDataHolder();" );
					addMethodContent( workingBuffer, "dh.getBuffer().append( \" \\\"",
							clusteringKeyColumnName, "\\\"", operatorSymbol, "?\" );" );
					addMethodContent( workingBuffer, "dh.addParameter( ", clusteringKeyColumn.getName(), " );" );
					addMethodContent( workingBuffer, "return "+classMajorType.toLowerCase()+"Operator;" );
					addMethodFooter( workingBuffer );

					if (clusteringKeyColumn.getType().getName().name().equals( "TIMESTAMP" ))
					{
						addBlankLine( workingBuffer );
						addMethodHeader( workingBuffer, PUBLIC, operatorType+operatorTypeSuffix,
								clusteringKeyColumnName+operatorName, "long "+clusteringKeyColumnName );
						addMethodContent( workingBuffer, "return ", clusteringKeyColumnName, operatorName, "( new Date( ",
								clusteringKeyColumnName, " ) );" );
						addMethodFooter( workingBuffer );
					}
				}
			}

			addClassFooter( workingBuffer );

			writeClassFile( packageName, classType, scopedTableName, workingBuffer.toString() );
		}
		while (cc++ < clusteringKeyComponentList.size());
	}

	private static String capitalize( String stringToCapitalize )
	{
		return stringToCapitalize.substring( 0, 1 ).toUpperCase() + stringToCapitalize.substring( 1 );
	}

	private static boolean isKeyColumn( String columnName, List<ColumnMetadata> keyComponents )
	{
		String quotedColumnName = columnName;
		for (int p=0; p<keyComponents.size(); p++)
		{
			if (keyComponents.get( p ).getName().equals( quotedColumnName ) ) return true;
		}
		return false;
	}

	private static String getAccessorMethodByType( String fieldName, String dataType, boolean getGetter )
	{
		String methodCall = (getGetter)?"get":"set";
		String apiParameterText = "( \"\\\""+fieldName+"\\\"\""+((getGetter)?"":(", "+fieldName))+" );";

		switch (dataType)
		{
			case "boolean": methodCall += "Bool"; break;
			case "ByteBuffer": methodCall += "Bytes"; break;
			case "Date": methodCall += "Date"; break;
			case "double": methodCall += "Double"; break;
			case "float": methodCall += "Float"; break;
			case "int": methodCall += "Int"; break;
			case "long": methodCall += "Long"; break;
			case "String": methodCall += "String"; break;
			default:
			{
				if (dataType.startsWith( "List" ))
				{
					methodCall += "List";
					if (getGetter) apiParameterText = apiParameterText.replace( " )", ", "+dataType.substring( 5, dataType.length() -1 )+".class )" );
					else apiParameterText = apiParameterText.replace( " )", ", "+fieldName+" );" );
					break;
				}
				if (dataType.startsWith( "Map" ))
				{
					methodCall += "Map";
					if (getGetter) apiParameterText = apiParameterText.replace( " )", ", "+dataType.substring( 4, dataType.length() -1 )+".class )" );
					else apiParameterText = apiParameterText.replace( " )", ", "+fieldName+" );" );
					break;
				}
				if (dataType.startsWith( "Set" ))
				{
					methodCall += "Set";
					if (getGetter) apiParameterText = apiParameterText.replace( " )", ", "+dataType.substring( 4, dataType.length() -1 )+".class )" );
					else apiParameterText = apiParameterText.replace( " )", ", "+fieldName+" );" );
					break;
				}
			}
		}

		return methodCall + apiParameterText;
	}

	private static String getParameterDeclarationList( List<ColumnMetadata> partitionKeyComponentList,
			boolean useDateAlternate )
	{
		String partitionKeyComponent;

		blockBuffer.setLength( 0 );
		for (int p = 0; p<partitionKeyComponentList.size(); p++)
		{
			partitionKeyComponent = partitionKeyComponentList.get( p ).getName();
			if (p != 0) blockBuffer.append( " " );
			if ((useDateAlternate) && (partitionKeyComponentList.get( p ).getType().getName().name().equals( "TIMESTAMP" )))
			{
				blockBuffer.append( "long" );
			}
			else blockBuffer.append( getTypeFromValidator( partitionKeyComponentList.get( p ).getType(), true ) );
			blockBuffer.append( " " ).append( partitionKeyComponent );
			if (p<partitionKeyComponentList.size() -1) blockBuffer.append( "," );
		}

		return blockBuffer.toString();
	}

	private static String getParameterList( List<ColumnMetadata> partitionKeyComponentList )
	{
		blockBuffer.setLength( 0 );
		for (int p = 0; p<partitionKeyComponentList.size(); p++)
		{
			if (p != 0) blockBuffer.append( " " );
			blockBuffer.append( partitionKeyComponentList.get( p ).getName() );
			if (p<partitionKeyComponentList.size() -1) blockBuffer.append( "," );
		}

		return blockBuffer.toString();
	}

	private static String getTypeFromValidator( DataType validatorClassType, boolean preferPrimitive )
	{
		switch( validatorClassType.getName().name() )
		{
			case "BIGINT": return (preferPrimitive)?"long":"Long";
			case "BLOB": return "ByteBuffer";
			case "BOOLEAN": return (preferPrimitive)?"boolean":"Boolean";
			case "DOUBLE": return (preferPrimitive)?"double":"Double";
			case "FLOAT": return (preferPrimitive)?"float":"Float";
			case "INT": return (preferPrimitive)?"int":"Integer";
			case "TEXT": return "String";
			case "TIMESTAMP": return "Date";
			case "UDT":
			{
				String s = validatorClassType.toString();
				return COOQLUDTValue.class.getSimpleName()+"_"+s.substring( s.indexOf( '.' )+2, s.length() -1 );
			}
			case "LIST":
			{
				return "List<"+getTypeFromValidator( validatorClassType.getTypeArguments().get( 0 ), false )+">";
			}
			default:
			{
//				if (validatorClassType.startsWith( "org.apache.cassandra.db.marshal.ListType" ))
//				if (validatorClassType.startsWith( "org.apache.cassandra.db.marshal.ListType" ))
//				{
//					String listDataType = getTypeFromValidator( validatorClassType.substring( validatorClassType.indexOf( '(' ) +1,
//							validatorClassType.length() -1 ), false );
//					return "List<"+listDataType+">";
//				}
//				if (validatorClassType.startsWith( "org.apache.cassandra.db.marshal.MapType" ))
//				{
//					return "Map";
//				}
//				if (validatorClassType.startsWith( "org.apache.cassandra.db.marshal.SetType" ))
//				{
//					return "Set";
//				}
//				if (validatorClassType.startsWith( "org.apache.cassandra.db.marshal.UserType" ))
//				{
//					String[] fieldList = (validatorClassType.substring( validatorClassType.indexOf( '(' ) +1, validatorClassType.length() -1 )).split( "," );
//					String signatureString = "";
//					for (int f=2; f<fieldList.length; f++)
//					{
//						signatureString += fieldList[ f ].substring( fieldList[ f ].indexOf( ':' )+ 1 );
//					}
//					return userTypeMap.get( signatureString );
//				}
				throw new IllegalArgumentException( "Unsupported data type: "+ validatorClassType );
			}
		}
	}

	private static boolean requiresDateAlternate( List<ColumnMetadata> keyColumnList )
	{
		for (int p=0; p<keyColumnList.size(); p++)
		{
			if (keyColumnList.get( p ).getType().getName().name().equals( "TIMESTAMP" )) return true;
		}

		return false;
	}

	private static void setIndentation( int aDepth )
	{
		indentation = "";
		for (int d=0; d<aDepth; d++) indentation += "\t";
	}

	private static void writeClassFile( String packageName, String classType, String tableName, String contents )
	{
		String src = ((new File("src")).exists())?"src/":"";
		String path = src+(packageName+".").replace( '.', '/' );
		File dir = new File( path );
		if (!dir.exists()) dir.mkdirs();

		File file = (new File( path+classType+((tableName != null)?("_"+tableName):"")+".java" ));
		try
		{
			BufferedWriter bw = new BufferedWriter( new FileWriter( file ));
			bw.write( contents );
			bw.close();
		}
		catch (FileNotFoundException fnfe)
		{
			// TODO Auto-generated catch block
			fnfe.printStackTrace();
		}
		catch (IOException ioe)
		{
			// TODO Auto-generated catch block
			ioe.printStackTrace();
		}
	}
}
