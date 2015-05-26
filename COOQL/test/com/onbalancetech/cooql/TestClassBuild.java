package com.onbalancetech.cooql;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class TestClassBuild
{
	public static void main( String[] args )
	{
		String keyspaceName = args[ 1 ];
		InetAddress ipAddress;
		try
		{
			ipAddress = InetAddress.getByName( args[ 0 ] );
		}
		catch (UnknownHostException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		Cluster cluster = Cluster.builder().addContactPoint( args[ 0 ] ).build();
		Session session = cluster.connect( args[ 1 ] );

		COOQL cooql = COOQL.getInstance( session );

		cooql.buildAPI( ipAddress, keyspaceName );

		session.close();
		cluster.close();
		System.exit( 0 );
	}
}
