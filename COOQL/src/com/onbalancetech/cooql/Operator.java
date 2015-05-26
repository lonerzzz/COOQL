package com.onbalancetech.cooql;

public abstract class Operator extends ExecutableComponent
{
	protected Operator( COOQL cooql )
	{
		super( cooql );
	}

	public void printQuery()
	{
		XbuildQuery( true );
	}

	public String toString()
	{
		return "";
	}

	protected void XcleanUp() {}
}
