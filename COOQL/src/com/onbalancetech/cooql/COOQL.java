package com.onbalancetech.cooql;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.onbalancetech.cooql.builder.COOQLBuilder;

public class COOQL
{
	private static ThreadLocal<DataHolder> dataHolder = new ThreadLocal<DataHolder>();
	private static Map<Session,Map<String,PreparedStatement>> preparedStatementMap
		= new ConcurrentHashMap<Session,Map<String,PreparedStatement>>();
	private static Map<String,Map<String,Class<? extends TypeConverter>>> converterClassMap
		= new ConcurrentHashMap<String, Map<String,Class<? extends TypeConverter>>>();

	private Session session;

	public static class DataHolder
	{
		private StringBuffer sb;
		private ArrayList<Object> parameterList;
		@SuppressWarnings("rawtypes")
		private Map<String,Map<String,TypeConverter>> converterMap;
		private Set<String> queriedColumnSet;

		@SuppressWarnings("rawtypes")
		public DataHolder()
		{
			sb = new StringBuffer();
			converterMap = new HashMap<String,Map<String,TypeConverter>>( 4 );
			parameterList = new ArrayList<Object>(10);
			queriedColumnSet = new HashSet<String>(10);
		}

		public void clearParameterList()
		{
			parameterList.clear();
		}

		public StringBuffer getBuffer()
		{
			return sb;
		}

		public String addColumn( String columnName )
		{
			queriedColumnSet.add( columnName );

			return columnName;
		}

		public void addParameter( Object parameter )
		{
			parameterList.add( parameter );
		}

		@SuppressWarnings("rawtypes")
		public TypeConverter getConverter( String tableName, String columnName )
		{
			Map<String, TypeConverter> typeConverterMap = converterMap.get( tableName );
			Class<? extends TypeConverter> typeConverterClass;
			TypeConverter typeConverter = null;
			if ((typeConverterMap == null) || ((typeConverter = typeConverterMap.get( columnName ))) == null)
			{
				Map<String, Class<? extends TypeConverter>> typeConverterClassMap = converterClassMap.get( tableName );
				if (typeConverterClassMap == null) return null;
				typeConverterClass = typeConverterClassMap.get( columnName );
				if (typeConverterClass == null) return null;
				if (typeConverterMap == null)
				{
					typeConverterMap = new HashMap<String,TypeConverter>();
					converterMap.put( tableName, typeConverterMap );
				}

				try
				{
					typeConverterMap.put( columnName, typeConverter = typeConverterClass.newInstance() );
				}
				catch (InstantiationException|IllegalAccessException e)
				{
					throw new RuntimeException( e );
				}
			}

			return typeConverter;
		}

		public ArrayList<Object> getParameterList()
		{
			return parameterList;
		}

		public Set<String> getQueryColumnSet()
		{
			return queriedColumnSet;
		}

		public void clearQueryColumnSet()
		{
			queriedColumnSet.clear();
		}
	}

	protected COOQL( Session session )
	{
		this.session = session;
		if (!preparedStatementMap.containsKey( session ))
		{
			preparedStatementMap.put( session, new ConcurrentHashMap<String,PreparedStatement>() );
		}
	}

	protected void checkForSession()
	{
		if (session == null)
		{
			throw new IllegalStateException( "No session has been set so CQL commands cannot be executed" );
		}
	}

	PreparedStatement addPreparedStatement( String statement )
	{
		if (preparedStatementMap.get( session ).containsKey( statement ))
		{
			throw new IllegalStateException( "The prepared statement being cached has been added previously:"
					+ "'" +statement+"'");
		}
		PreparedStatement preparedStatement = session.prepare( statement+";" );
		preparedStatementMap.get( session ).put( statement, preparedStatement );

		return preparedStatement;
	}

	public void buildAPI( InetAddress hostAddress, String keyspaceName )
	{
		COOQLBuilder.buildAPI( hostAddress, keyspaceName );
	}

	protected Session getSession()
	{
		return session;
	}

	public static final DataHolder getDataHolder()
	{
		if (dataHolder.get() == null) dataHolder.set( new DataHolder() );

		return dataHolder.get();
	}

	public static COOQL getInstance( Session session )
	{
		return new COOQL( session );
	}

	PreparedStatement getPreparedStatement( String statement )
	{
		return preparedStatementMap.get( session ).get( statement );
	}

	protected UDTValue getUDTValue( String userTypeName )
	{
		Collection<UserType> userTypeCollection = session.getCluster().getMetadata()
				.getKeyspace( getSession().getLoggedKeyspace() ).getUserTypes();
		Iterator<UserType> iterator = userTypeCollection.iterator();
		UserType userType;
		while (iterator.hasNext())
		{
			userType = iterator.next();
			if (userType.getTypeName().equals( userTypeName )) return userType.newValue();
		}

		return null;
	}

	@SuppressWarnings("rawtypes")
	public void registerConverter( String tableName, String fieldName, Class<? extends TypeConverter> converterClass )
	{
		Map<String,Class<? extends TypeConverter>> fieldConverterMap;
		if ((fieldConverterMap = converterClassMap.get( tableName )) == null)
		{
			fieldConverterMap = new HashMap<String, Class<? extends TypeConverter>>( 4 );
			converterClassMap.put( tableName, fieldConverterMap );
		}
		fieldConverterMap.put( fieldName, converterClass );
	}

	protected void resetCommand()
	{
		checkForSession();
		getDataHolder().getBuffer().setLength( 0 );
		getDataHolder().clearParameterList();
		getDataHolder().clearQueryColumnSet();
	}
}
