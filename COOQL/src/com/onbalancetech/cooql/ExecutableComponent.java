package com.onbalancetech.cooql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public abstract class ExecutableComponent extends Component
{
	private List<String> methodNameList;

	protected ExecutableComponent( COOQL cooql )
	{
		super( cooql );
		methodNameList = new ArrayList<String>();
	}

	protected final PreparedStatement XbuildQuery( boolean printQuery )
	{
		XcleanUp();
		String statement = COOQL.getDataHolder().getBuffer().toString();
		PreparedStatement ps = XgetCOOQL().getPreparedStatement( statement );
		if (ps == null) ps = XgetCOOQL().addPreparedStatement( statement );
		
		if (printQuery) System.out.println( statement );

		return ps;
	}

	protected final ResultSet Xexecute()
	{
		PreparedStatement ps = XbuildQuery( false );

		try
		{
			return XgetCOOQL().getSession().execute( ps.bind( XresolveParameterValues() ) );
		}
		catch (InvalidTypeException ite)
		{
			System.out.println( "JK "+COOQL.getDataHolder().getParameterList().toArray() );
			throw new RuntimeException( ite );
		}
	}

	protected final ResultSetFuture XexecuteAsynchronously()
	{
		PreparedStatement ps = XbuildQuery( false );
		return XgetCOOQL().getSession().executeAsync( ps.bind( XresolveParameterValues() ) );
	}

	private UDTValue XmapFields( COOQLUDTValue valueObject )
	{
		@SuppressWarnings("rawtypes")
		Class[] interfaceList = valueObject.getClass().getInterfaces();
		Class objectInterface = null;
		String userType = null;
		for (int i=0; i<interfaceList.length; i++)
		{
			if (interfaceList[ i ].getSimpleName().startsWith( COOQLUDTValue.class.getSimpleName() ))
			{
				userType = "\""+interfaceList[ i ].getSimpleName().split( "_" )[ 1 ]+"\"";
				objectInterface = interfaceList[ i ];
				break;
			}
		}
		if (userType == null)
		{
			throw new IllegalArgumentException( "No interface extending '"+COOQLUDTValue.class.getSimpleName()
					+"' was found for the object being mapped to the UDT value.");
		}
		MethodAccess objectGetterAccessor = MethodAccess.get( objectInterface );
		objectGetterAccessor.getGetterMethodNameList( methodNameList );
		UDTValue udtValue = XgetCOOQL().getSession().getCluster().getMetadata()
				.getKeyspace( XgetCOOQL().getSession().getLoggedKeyspace() ).getUserType( userType ).newValue();
		Object returnObject;
		String fieldName;

		for (int m = methodNameList.size() -1; m>=0; m--)
		{
			returnObject = objectGetterAccessor.invoke( valueObject, methodNameList.get( m ) );
			fieldName = methodNameList.get( m ).substring( 3 );
			fieldName = "\""+fieldName.substring( 0, 1).toLowerCase()+fieldName.substring( 1 )+"\"";
			if (returnObject instanceof Boolean) udtValue.setBool( fieldName, (Boolean)returnObject );
			else if (returnObject instanceof ByteBuffer) udtValue.setBytes( fieldName, (ByteBuffer)returnObject );
			else if (returnObject instanceof BigDecimal) udtValue.setDecimal( fieldName, (BigDecimal)returnObject );
			else if (returnObject instanceof Date) udtValue.setDate( fieldName, (Date)returnObject );
			else if (returnObject instanceof Double) udtValue.setDouble( fieldName, (Double)returnObject );
			else if (returnObject instanceof Float) udtValue.setFloat( fieldName, (Float)returnObject );
			else if (returnObject instanceof InetAddress) udtValue.setInet( fieldName, (InetAddress)returnObject );
			else if (returnObject instanceof Integer) udtValue.setInt( fieldName, (Integer)returnObject );
			else if (returnObject instanceof Long) udtValue.setLong( fieldName, (Long)returnObject );
			else if (returnObject instanceof String) udtValue.setString( fieldName, (String)returnObject );
			else if (returnObject instanceof BigInteger) udtValue.setVarint( fieldName, (BigInteger)returnObject );
		}

		return udtValue;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object[] XresolveParameterValues()
	{
		Object[] parameterValueList = COOQL.getDataHolder().getParameterList().toArray();

		for (int i=0; i<parameterValueList.length; i++)
		{
			if (parameterValueList[ i ] instanceof COOQLUDTValue)
			{
				parameterValueList[ i ] = XmapFields( ((COOQLUDTValue)parameterValueList[ i ]) );
			}
			else if (parameterValueList[ i ] instanceof List)
			{
				if (((List)parameterValueList[ i ]).get( 0 ) instanceof COOQLUDTValue)
				{
					List<COOQLUDTValue> list = (List<COOQLUDTValue>)parameterValueList[ i ];
					List<UDTValue> destinationList = new ArrayList<UDTValue>();
					for (int j=0; j<list.size(); j++)
					{
						destinationList.add( XmapFields( list.get( j ) ) );
					}
					parameterValueList[ i ] = destinationList;
				}
			}
		}

		return parameterValueList;
	}
}
