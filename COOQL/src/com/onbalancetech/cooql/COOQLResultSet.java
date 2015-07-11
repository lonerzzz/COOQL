package com.onbalancetech.cooql;

import static com.datastax.driver.core.DataType.Name;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.onbalancetech.cooql.COOQL.DataHolder;

public abstract class COOQLResultSet implements Iterable<COOQLRow>
{
	private static final String POPULATE_OBJECT = "populateObject";
	private int classListIndex;
	protected Map<String,Map<String,TypeConverter>> converterMap;
	protected ResultSet resultSet;
	protected Set<String> queriedColumnSet;
	
	protected COOQLResultSet( ResultSet resultSet, DataHolder dataHolder )
	{
		this.resultSet = resultSet;
		this.queriedColumnSet = dataHolder.getQueryColumnSet();
		this.converterMap = dataHolder.getConverterMap();
	}

	public long getCount()
	{
		if (XcontainsColumn( "count" ))
		{
			Row row = resultSet.one();
			return row.getLong( "count" );
		}
		else
		{
			if (resultSet.isExhausted()) return 0;
			List<Row> rowList = resultSet.all();
			return rowList.size();
		}
	}

	public abstract Iterator<COOQLRow> iterator();

	@SafeVarargs
	@SuppressWarnings({ "rawtypes" })
	public final Object populateObject( Class objectClass, Class<? extends COOQLUDTValue>... objectClassList )
	{
		if (resultSet.isExhausted()) return null;
		try
		{
			Row row = resultSet.one();
			String queriedColumn;
			if ((queriedColumnSet.size() == 1) && (objectClass.getName().startsWith( "java" )) &&
				(!(queriedColumn = queriedColumnSet.iterator().next()).equals("*")))
			{
				return XpopulateObject( row, objectClass, queriedColumn );
			}
			classListIndex = 0;
			return XpopulateObject( XcreateRow( row ), objectClass.newInstance(), objectClassList );
		}
		catch (InstantiationException|IllegalAccessException e)
		{
			// TODO Make friendlier
			e.printStackTrace();
		}
		return null;
	}

	@SafeVarargs
	public final Object populateObject( Object objectToPopulate, Class<? extends COOQLUDTValue>... objectClassList )
	{
		classListIndex = 0;
		return XpopulateObject( XcreateRow( resultSet.one() ), objectToPopulate, objectClassList );
	}

	@SafeVarargs
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final List populateObjectList( Class objectListClass, Class listObjectClass,
			Class<? extends COOQLUDTValue>... objectClassList )
	{
		List<Object> objectListToPopulate;
		try
		{
			objectListToPopulate = (List)objectListClass.newInstance();
		}
		catch (InstantiationException|IllegalAccessException iaeie)
		{
			iaeie.printStackTrace();
			return null;
		}

		return XpopulateObjectList( objectListToPopulate, listObjectClass, objectClassList );
	}

	@SafeVarargs
	@SuppressWarnings({ "rawtypes" })
	public final List populateObjectList( List objectListToPopulate, Class listObjectClass,
			Class<? extends COOQLUDTValue>... objectClassList )
	{
		if (objectListToPopulate == null) throw new IllegalArgumentException( "The list to be populated must be non-null.");
		objectListToPopulate.clear();

		return XpopulateObjectList( objectListToPopulate, listObjectClass, objectClassList );
	}

	@SafeVarargs
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final Map populateObjectMap( Class objectMapClass, Class mapObjectKeyClass, Class mapObjectValueClass,
			Class<? extends COOQLUDTValue>... objectClassList )
	{
		Map<Object,Object> objectMapToPopulate;
		try
		{
			objectMapToPopulate = (Map)objectMapClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException iaeie)
		{
			iaeie.printStackTrace(); //TODO Make friendlier
			return null;
		}

		return XpopulateObjectMap( objectMapToPopulate, mapObjectKeyClass, mapObjectValueClass, objectClassList );
	}

	@SafeVarargs
	@SuppressWarnings({ "rawtypes" })
	public final Map populateObjectMap( Map objectMapToPopulate, Class mapObjectKeyClass, Class mapObjectValueClass,
			Class<? extends COOQLUDTValue>... objectClassList )
	{
		if (objectMapToPopulate == null) throw new IllegalArgumentException( "The map to be populated must be non-null.");
		objectMapToPopulate.clear();

		return XpopulateObjectMap( objectMapToPopulate, mapObjectKeyClass, mapObjectValueClass, objectClassList );
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final Map populateObjectMap( Class objectMapClass )
	{
		Map<Object,Object> objectMapToPopulate;
		try
		{
			objectMapToPopulate = (Map)objectMapClass.newInstance();
		}
		catch (InstantiationException | IllegalAccessException iaeie)
		{
			iaeie.printStackTrace(); //TODO Make friendlier
			return null;
		}

		return XpopulateObjectMap( objectMapToPopulate );
	}

	@SuppressWarnings({ "rawtypes" })
	public final Map populateObjectMap( Map objectMapToPopulate )
	{
		if (objectMapToPopulate == null) throw new IllegalArgumentException( "The map to be populated must be non-null.");
		objectMapToPopulate.clear();

		return XpopulateObjectMap( objectMapToPopulate );
	}

	private float XcheckRangeDoubleToFloat( double value, String methodName )
	{
		if ((value < Float.MIN_VALUE) || (value > Float.MAX_VALUE))
		{
			throw new RuntimeException( "Number truncated converting Double to Float for method '"+methodName+"'");
		}
		return (float)value;
	}

	private byte XcheckRangeIntegerToByte( long value, String methodName )
	{
		if ((value < Byte.MIN_VALUE) || (value > Byte.MAX_VALUE))
		{
			throw new RuntimeException( "Number truncated converting Long to Integer for method '"+methodName+"'");
		}
		return (byte)value;
	}

	private short XcheckRangeIntegerToShort( long value, String methodName )
	{
		if ((value < Integer.MIN_VALUE) || (value > Integer.MAX_VALUE))
		{
			throw new RuntimeException( "Number truncated converting Long to Short for method '"+methodName+"'");
		}
		return (short)value;
	}

	private byte XcheckRangeLongToByte( long value, String methodName )
	{
		if ((value < Byte.MIN_VALUE) || (value > Byte.MAX_VALUE))
		{
			throw new RuntimeException( "Number truncated converting Long to Integer for method '"+methodName+"'");
		}
		return (byte)value;
	}

	private int XcheckRangeLongToInteger( long value, String methodName )
	{
		if ((value < Integer.MIN_VALUE) || (value > Integer.MAX_VALUE))
		{
			throw new RuntimeException( "Number truncated converting Long to Integer for method '"+methodName+"'");
		}
		return (int)value;
	}

	private short XcheckRangeLongToShort( long value, String methodName )
	{
		if ((value < Integer.MIN_VALUE) || (value > Integer.MAX_VALUE))
		{
			throw new RuntimeException( "Number truncated converting Long to Short for method '"+methodName+"'");
		}
		return (short)value;
	}

	protected boolean XcontainsColumn( String columnName )
	{
		return ((queriedColumnSet.contains( columnName )) || (queriedColumnSet.contains( "*" )));
	}

	private Object XconvertToObjectType( String methodName, Object sourceObject,
			@SuppressWarnings("rawtypes") Class destinationParameterType,
			Class<? extends COOQLUDTValue>[] objectClassList )
	{
		if (sourceObject == null) return null;

		if (sourceObject instanceof Date)
		{
			if ((destinationParameterType.equals( Long.class )) || (destinationParameterType.equals( Long.TYPE )))
			{
				return new Long( ((Date)sourceObject).getTime() );
			}
		}
		else if ((sourceObject instanceof Double) || (sourceObject.getClass().equals( Double.TYPE )))
		{
			if ((destinationParameterType.equals( Float.class )) || (destinationParameterType.equals( Float.TYPE )))
			{
				return new Float( XcheckRangeDoubleToFloat( (Double)sourceObject, methodName ) );
			}
		}
		else if ((sourceObject instanceof Float) || (sourceObject.getClass().equals( Float.TYPE )))
		{
			if ((destinationParameterType.equals( Double.class )) || (destinationParameterType.equals( Double.TYPE )))
			{
				return new Double( ((Float)sourceObject) );
			}
		}
		else if ((sourceObject instanceof Integer) || (sourceObject.getClass().equals( Integer.TYPE )))
		{
			int value = ((Integer)sourceObject).intValue();
			if ((destinationParameterType.equals( Long.class )) || (destinationParameterType.equals( Long.TYPE )))
			{
				return new Long( value );
			}
			else if ((destinationParameterType.equals( Short.class )) || (destinationParameterType.equals( Short.TYPE )))
			{
				return new Short( XcheckRangeIntegerToShort( value, methodName ) );
			}
			else if ((destinationParameterType.equals( Byte.class )) || (destinationParameterType.equals( Byte.TYPE )))
			{
				return new Byte( XcheckRangeIntegerToByte( value, methodName ) );
			}
		}
		else if ((sourceObject instanceof Long) || (sourceObject.getClass().equals( Long.TYPE )))
		{
			long value = ((Long)sourceObject).longValue();
			if (destinationParameterType.equals( Integer.class ))
			{
				return new Integer( XcheckRangeLongToInteger( value, methodName ) );
			}
			else if (destinationParameterType.equals( Short.class ))
			{
				return new Short( XcheckRangeLongToShort( value, methodName ) );
			}
			else if ((destinationParameterType.equals( Byte.class )) || (destinationParameterType.equals( Byte.TYPE )))
			{
				return new Byte( XcheckRangeLongToByte( value, methodName ) );
			}
		}
		else if ((sourceObject instanceof Boolean) || (sourceObject.getClass().equals( Boolean.TYPE )))
		{
			return sourceObject;
		}
		else if (sourceObject instanceof List)
		{
			@SuppressWarnings("rawtypes")
			List list = (List)sourceObject;
			if ((list.size() != 0) && (list.get( 0 ) instanceof COOQLUDTValue))
			{
				List<COOQLUDTValue> destinationList = new ArrayList<COOQLUDTValue>();
				if (classListIndex >= objectClassList.length )
				{
					throw new IllegalArgumentException( "The number of classes provided for object population was insufficient"
							+" to allow mapping of all encountered types: Method " + methodName
							+ " could not be called." );
				}
				try
				{
					for (int i=0; i<list.size(); i++)
					{
						COOQLUDTValue objectToPopulate = (COOQLUDTValue)objectClassList[ classListIndex ].newInstance();
						destinationList.add( (COOQLUDTValue)XpopulateObject( list.get( i ), objectToPopulate, objectClassList ) );
					}
					return destinationList;
				}
				catch (InstantiationException|IllegalAccessException ieiae)
				{
					throw new RuntimeException( "The permissions on class " + objectClassList[ classListIndex ].getSimpleName()
							+ " are insufficient to allow an instance to be instantiated in order to be populated.");
				}
			}
		}
		else if (!destinationParameterType.isInstance( sourceObject ))
		{
			throw new RuntimeException( "Data type '"+sourceObject.getClass().getSimpleName()+"' for method '"
					+methodName+"' is unsupported" );
		}
		return sourceObject;
	}

	protected abstract COOQLRow XcreateRow( Row row );

	private String XgenerateMismatchErrorMessage( String name, String simpleName )
	{
		return "No implicit conversion is known for converting from "+name+" to "+simpleName;
	}

	@SuppressWarnings({ "rawtypes" })
	private Object XpopulateObject( Row row, Class objectClass, String queriedColumn )
	{
		ColumnDefinitions columnDefinitionList = resultSet.getColumnDefinitions();
		Name dataType = columnDefinitionList.getType( queriedColumn ).getName();
		if (objectClass.equals(Boolean.class))
		{
			if (dataType.equals(Name.BOOLEAN)) return new Boolean( row.getBool( queriedColumn ) );
		}
		else if (objectClass.equals(Byte.class))
		{
			if (dataType.equals(Name.INT))
			{
				return new Short( XcheckRangeIntegerToByte( row.getInt( queriedColumn ), POPULATE_OBJECT ) );
			}
			else if (dataType.equals(Name.BIGINT))
			{
				return new Short( XcheckRangeLongToByte( row.getLong( queriedColumn ), POPULATE_OBJECT ) );
			}
		}
		else if (objectClass.equals(ByteBuffer.class))
		{
			if (dataType.equals(Name.BLOB)) return row.getBytes( queriedColumn );
		}
		else if (objectClass.equals(Date.class))
		{
			if (dataType.equals( Name.TIMESTAMP )) return row.getDate( queriedColumn );
		}
		else if (objectClass.equals(Double.class))
		{
			if (dataType.equals(Name.BIGINT)) return new Double( row.getLong( queriedColumn ) );
			else if (dataType.equals(Name.DOUBLE)) return new Double( row.getDouble( queriedColumn ) );
			else if (dataType.equals(Name.FLOAT)) return new Double( row.getFloat( queriedColumn ) );
			else if (dataType.equals(Name.INT)) return new Double( row.getInt( queriedColumn ) );
		}
		else if (objectClass.equals(Float.class))
		{
			if (dataType.equals(Name.DOUBLE))
			{
				return new Double( XcheckRangeDoubleToFloat( row.getDouble( queriedColumn ), POPULATE_OBJECT ) );
			}
			if (dataType.equals(Name.BIGINT)) return new Float( row.getLong( queriedColumn ) );
			else if (dataType.equals(Name.FLOAT)) return new Float( row.getFloat( queriedColumn ) );
			else if (dataType.equals(Name.INT)) return new Float( row.getInt( queriedColumn ) );
			
		}
		else if (objectClass.equals(Integer.class))
		{
			if (dataType.equals(Name.BIGINT))
			{
				return new Integer( XcheckRangeLongToInteger( row.getLong( queriedColumn ), POPULATE_OBJECT ) );
			}
			else if (dataType.equals(Name.INT)) return new Integer( row.getInt( queriedColumn ) );
		}
		else if (objectClass.equals(Long.class))
		{
			if (dataType.equals(Name.BIGINT)) return new Long( row.getLong( queriedColumn ) );
			else if (dataType.equals(Name.INT)) return new Long( row.getInt( queriedColumn ) );
			else if (dataType.equals(Name.TIMESTAMP)) return new Long( row.getDate( queriedColumn ).getTime() );
		}
		else if (objectClass.equals(Short.class))
		{
			if (dataType.equals(Name.BIGINT))
			{
				return new Short( XcheckRangeLongToShort( row.getLong( queriedColumn ), POPULATE_OBJECT ) );
			}
			else if (dataType.equals(Name.INT))
			{
				return new Short( XcheckRangeIntegerToShort( row.getInt( queriedColumn ), POPULATE_OBJECT ) );
			}
		}
		else if (objectClass.equals(String.class))
		{
			if (dataType.equals(Name.VARCHAR)) return new String( row.getString( queriedColumn ) );
		}
		else
		{
			String tableName = this.getClass().getSimpleName().substring( 15 );
			Map<String,TypeConverter> fieldConverterMap = converterMap.get( tableName );
			if (fieldConverterMap != null)
			{
				TypeConverter typeConverter = fieldConverterMap.get( queriedColumn );
				if (typeConverter != null)
				{
					Object sourceObject = null;
					switch (dataType)
					{
						case BIGINT: sourceObject = row.getLong( queriedColumn ); break;
						case BLOB: sourceObject = row.getBytes( queriedColumn ); break;
						case BOOLEAN: sourceObject = row.getBool( queriedColumn ); break;
						case DECIMAL: sourceObject = row.getDecimal( queriedColumn ); break;
						case DOUBLE: sourceObject = row.getDouble( queriedColumn ); break;
						case FLOAT: sourceObject = row.getFloat( queriedColumn ); break;
						case INET: sourceObject = row.getInet( queriedColumn ); break;
						case INT: sourceObject = row.getInt( queriedColumn ); break;
						case TEXT: sourceObject = row.getString( queriedColumn ); break;
						case TIMESTAMP: sourceObject = row.getDate( queriedColumn ); break;
						case TUPLE: sourceObject = row.getTupleValue( queriedColumn ); break;
						case UUID: sourceObject = row.getUUID( queriedColumn ); break;
						case UDT: sourceObject = row.getUDTValue( queriedColumn ); break;
						case VARCHAR: sourceObject = row.getString( queriedColumn ); break;
						case VARINT: sourceObject = row.getVarint( queriedColumn ); break;
					}
					
					if (sourceObject != null)
					{
						Object returnObject = typeConverter.convert( sourceObject );
						if (returnObject != null) return returnObject;
					}
				}
			}
		}

		throw new IllegalArgumentException( XgenerateMismatchErrorMessage(
				dataType.name(), objectClass.getSimpleName()) );
	}

	@SuppressWarnings("unchecked")
	private Object XpopulateObject( Object sourceObject, Object objectToPopulate,
			Class<? extends COOQLUDTValue>... objectClassList )
	{
		List<String> methodNameList = new ArrayList<String>(); //TODO Make static
		MethodAccess objectGetterAccessor = MethodAccess.get( sourceObject.getClass() );
		MethodAccess objectSetterAccessor = MethodAccess.get( objectToPopulate.getClass() );
		String setterMethodName, columnName;
		boolean nullObject = false;
		
		objectGetterAccessor.getGetterMethodNameList( methodNameList );
		for (int i = methodNameList.size() -1; i>=0; i--)
		{
			columnName = methodNameList.get( i ).substring( 3 );
			columnName = "\""+columnName.substring( 0, 1 ).toLowerCase() + columnName.substring( 1 ) + "\"";
			if ((XcontainsColumn( columnName )) &&
				((setterMethodName = objectSetterAccessor.getMatchingSetter( methodNameList.get( i ) )) != null))
			{
				try
				{
					Object retrievedValue = objectGetterAccessor.invoke( sourceObject, methodNameList.get( i ) );
					nullObject = (retrievedValue == null);
					objectSetterAccessor.invoke( objectToPopulate, setterMethodName, 	XconvertToObjectType( methodNameList.get( i ),
							retrievedValue, objectSetterAccessor.getParameterType( setterMethodName ), objectClassList ) );
				}
				catch (ClassCastException cce)
				{
					throw new ClassCastException( "Method mismatch encountered during mapping: "+ setterMethodName +
							" does not use the same data type as the assigned data column" );
				}
				catch (NullPointerException npe)
				{
					if (!nullObject) throw npe;
					throw new NullPointerException( "Could not pass a null value to method "+methodNameList.get( i )+
							" on an object of type "+objectToPopulate.getClass().getSimpleName() );
				}
			}
		}

		return objectToPopulate;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List XpopulateObjectList( List objectListToPopulate, Class listObjectClass,
			Class<? extends COOQLUDTValue>... objectClassList )
	{
		String queriedColumn = null;
		Iterator<Row> iterator = resultSet.iterator();
		boolean isPrimitive = (queriedColumnSet.size() == 1) && (listObjectClass.getName().startsWith( "java" )) &&
				(!(queriedColumn = queriedColumnSet.iterator().next()).equals("*"));

		while (iterator.hasNext())
		{
			try
			{
				classListIndex = 0;
				if (isPrimitive)
				{
					objectListToPopulate.add( XpopulateObject( XcreateRow( iterator.next() ), listObjectClass, queriedColumn ) );
				}
				else
				{
					objectListToPopulate.add( XpopulateObject( XcreateRow( iterator.next() ), listObjectClass.newInstance(), objectClassList ) );
				}
			}
			catch (InstantiationException|IllegalAccessException ieiae)
			{
				ieiae.printStackTrace(); //TODO Convert to friendlier error
				return null;
			}
		}

		return objectListToPopulate;
	}

	/**
	 * Populates a map using one column value as key and the other column value as the value
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Map XpopulateObjectMap( Map objectMapToPopulate, Class mapObjectKeyClass, Class mapObjectValueClass,
			Class<? extends COOQLUDTValue>[] objectClassList )
	{
		String queriedKeyColumn = null, queriedValueColumn = null;
		Iterator<Row> iterator = resultSet.iterator();
		boolean isPrimitiveKey = (queriedColumnSet.size() == 2) &&
				(mapObjectKeyClass.getName().startsWith( "java" )) &&
				(!(queriedKeyColumn = queriedColumnSet.iterator().next()).equals("*"));
		boolean isPrimitiveValue = (queriedColumnSet.size() == 2) &&
				(mapObjectKeyClass.getName().startsWith( "java" )) &&
				(!(queriedValueColumn = queriedColumnSet.iterator().next()).equals("*"));

		COOQLRow row;
		Object keyObject, valueObject;
		while (iterator.hasNext())
		{
			try
			{
				classListIndex = 0;
				row = XcreateRow( iterator.next() );
				if (isPrimitiveKey) keyObject = XpopulateObject( row, mapObjectKeyClass, queriedKeyColumn );
				else keyObject = XpopulateObject( row, mapObjectKeyClass.newInstance(), objectClassList );
				if (isPrimitiveValue) valueObject = XpopulateObject( row, mapObjectValueClass, queriedValueColumn );
				else valueObject = XpopulateObject( row, mapObjectValueClass.newInstance(), objectClassList );
				objectMapToPopulate.put( keyObject, valueObject );
			}
			catch (InstantiationException | IllegalAccessException ieiae)
			{
				ieiae.printStackTrace(); //TODO Convert to friendlier error
				return null;
			}
		}

		return objectMapToPopulate;
	}

	/**
	 * Populates a map using the column names as keys and column values as values
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Map XpopulateObjectMap( Map objectMapToPopulate )
	{
		Iterator<Row> iterator = resultSet.iterator();
		classListIndex = 0;
		COOQLRow row = XcreateRow( iterator.next() );
		Object valueObject;
		String keyString;

		ColumnDefinitions columnDefinitionList = row.getColumnDefinitions();
		for (int i=0; i<columnDefinitionList.size(); i++)
		{
			keyString = columnDefinitionList.getName( i );
			valueObject = null;
			switch (columnDefinitionList.getType( i ).getName())
			{
				case ASCII:
				{
					valueObject = row.getString( i );
					break;
				}
				case BIGINT:
				{
					valueObject = row.getLong( i );
					break;
				}
				case BLOB:
				{
					valueObject = row.getBytes( i );
					break;
				}
				case BOOLEAN:
				{
					valueObject = row.getBool( i );
					break;
				}
				case COUNTER:
				{
					break;
				}
				case CUSTOM:
				{
					break;
				}
				case DECIMAL:
				{
					valueObject = row.getDecimal( i );
					break;
				}
				case DOUBLE:
				{
					valueObject = row.getDouble( i );
					break;
				}
				case FLOAT:
				{
					valueObject = row.getFloat( i );
					break;
				}
				case INET:
				{
					valueObject = row.getInet( i );
					break;
				}
				case INT:
				{
					valueObject = row.getInt( i );
					break;
				}
				case LIST:
				{
					break;
				}
				case MAP:
				{
					break;
				}
				case SET:
				{
					break;
				}
				case TEXT:
				{
					valueObject = row.getString( i );
					break;
				}
				case TIMESTAMP:
				{
					valueObject = row.getDate( i );
					break;
				}
				case TIMEUUID:
				{
					valueObject = row.getUUID( i );
					break;
				}
				case TUPLE:
				{
					valueObject = row.getTupleValue( i );
					break;
				}
				case UDT:
				{
					valueObject = row.getUDTValue( i );
					break;
				}
				case UUID:
				{
					valueObject = row.getUUID( i );
					break;
				}
				case VARCHAR:
				{
					valueObject = row.getString( i );
					break;
				}
				case VARINT:
				{
					valueObject = row.getVarint( i );
					break;
				}
			}

			if (valueObject != null) objectMapToPopulate.put( keyString, valueObject );
		}

		return objectMapToPopulate;
	}
}
