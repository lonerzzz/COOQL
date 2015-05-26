package com.onbalancetech.cooql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

public class COOQLRow implements Row
{
	protected COOQLResultSet resultSet;
	protected Row row;

	protected COOQLRow(COOQLResultSet resultSet, Row row)
	{
		this.resultSet = resultSet;
		this.row = row;
	}

	public boolean getBool( int columnIndex )
	{
		return row.getBool( columnIndex );
	}

	public boolean getBool( String columnName )
	{
		return row.getBool( columnName );
	}

	public ByteBuffer getBytes( int columnIndex )
	{
		return row.getBytes( columnIndex );
	}

	public ByteBuffer getBytes( String columnName )
	{
		return row.getBytes( columnName );
	}

	public ByteBuffer getBytesUnsafe( int columnIndex )
	{
		return row.getBytesUnsafe( columnIndex );
	}

	public ByteBuffer getBytesUnsafe( String columnName )
	{
		return row.getBytesUnsafe( columnName );
	}

	public ColumnDefinitions getColumnDefinitions()
	{
		return row.getColumnDefinitions();
	}

	public Date getDate( int columnIndex )
	{
		return row.getDate( columnIndex );
	}

	public Date getDate( String columnName )
	{
		return row.getDate( columnName );
	}

	public BigDecimal getDecimal( int columnIndex )
	{
		return row.getDecimal( columnIndex );
	}

	public BigDecimal getDecimal( String columnName )
	{
		return row.getDecimal( columnName );
	}

	public double getDouble( int columnIndex )
	{
		return row.getDouble( columnIndex );
	}

	public double getDouble( String columnName )
	{
		return row.getDouble( columnName );
	}

	public float getFloat( int columnIndex )
	{
		return row.getFloat( columnIndex );
	}

	public float getFloat( String columnName )
	{
		return row.getFloat( columnName );
	}

	public InetAddress getInet( int columnIndex )
	{
		return row.getInet( columnIndex );
	}

	public InetAddress getInet( String columnName )
	{
		return row.getInet( columnName );
	}

	public int getInt( String columnName )
	{
		return row.getInt( columnName );
	}

	public int getInt( int columnIndex )
	{
		return row.getInt( columnIndex );
	}

	public <T> List<T> getList( int columnIndex, Class<T> elementClass )
	{
		return row.getList( columnIndex, elementClass );
	}

	public <T> List<T> getList( String columnName, Class<T> elementClass )
	{
		return row.getList( columnName, elementClass );
	}

	public long getLong( int columnIndex )
	{
		return row.getLong( columnIndex );
	}

	public long getLong( String columnName )
	{
		return row.getLong( columnName );
	}

	public <K, V> Map<K, V> getMap( int columnIndex, Class<K> keysClass, Class<V> valuesClass )
	{
		return row.getMap( columnIndex, keysClass, valuesClass );
	}

	public <K, V> Map<K, V> getMap( String columnName, Class<K> keysClass, Class<V> valuesClass )
	{
		return row.getMap( columnName, keysClass, valuesClass );
	}

	public <T> Set<T> getSet( int columnIndex, Class<T> elementClass )
	{
		return row.getSet( columnIndex, elementClass );
	}

	public <T> Set<T> getSet( String columnName, Class<T> elementClass )
	{
		return row.getSet( columnName, elementClass );
	}

	public String getString( int columnIndex )
	{
		return row.getString( columnIndex );
	}

	public String getString( String columnName )
	{
		return row.getString( columnName );
	}

	public TupleValue getTupleValue( int columnIndex )
	{
		return row.getTupleValue( columnIndex );
	}

	public TupleValue getTupleValue( String columnName )
	{
		return row.getTupleValue( columnName );
	}

	public UDTValue getUDTValue( int columnIndex )
	{
		return row.getUDTValue( columnIndex );
	}

	public UDTValue getUDTValue( String columnName )
	{
		return row.getUDTValue( columnName );
	}

	public UUID getUUID( int columnIndex )
	{
		return row.getUUID( columnIndex );
	}

	public UUID getUUID( String columnName )
	{
		return row.getUUID( columnName );
	}

	public BigInteger getVarint( int columnIndex )
	{
		return row.getVarint( columnIndex );
	}

	public BigInteger getVarint( String columnName )
	{
		return row.getVarint( columnName );
	}

	public boolean isNull( int columnIndex )
	{
		return row.isNull( columnIndex );
	}

	public boolean isNull( String columnName )
	{
		return row.isNull( columnName );
	}

	protected List<COOQLUDTValue> wrapUDTValueList( List<UDTValue> list, Class<? extends COOQLUDTValue> udtValueClass )
	{
		try
		{
			COOQLUDTValueImpl cooqlUdtValue;
			List<COOQLUDTValue> returnList = new ArrayList<COOQLUDTValue>( list.size() );

			for (int i=0; i<list.size(); i++)
			{
				cooqlUdtValue = (COOQLUDTValueImpl)udtValueClass.newInstance();
				cooqlUdtValue.setUDTValue( list.get( i ) );
				returnList.add( cooqlUdtValue );
			}

			return returnList;
		}
		catch (InstantiationException ie)
		{
			// TODO Auto-generated catch block
			ie.printStackTrace();
		}
		catch (IllegalAccessException iae)
		{
			// TODO Auto-generated catch block
			iae.printStackTrace();
		}
		return null;
	}
}
