package com.onbalancetech.cooql;

public class Values extends ExecutableComponent
{
	protected Values( COOQL cooql )
	{
		super( cooql );
	}

	public void printQuery()
	{
		XbuildQuery( true );
	}

	public String toString()
	{
		return ") VALUES (";
	}

	protected void XcleanUp()
	{
		int length = COOQL.getDataHolder().getBuffer().length();
		COOQL.getDataHolder().getBuffer().setLength( length -1 );
		COOQL.getDataHolder().getBuffer().append( toString() );
		for (int i=0; i<COOQL.getDataHolder().getQueryColumnSet().size(); i++)
		{
			COOQL.getDataHolder().getBuffer().append("?,");
		}
		length = COOQL.getDataHolder().getBuffer().length();
		COOQL.getDataHolder().getBuffer().replace( length -1, length, ")" );
	}
}
