package com.onbalancetech.cooql;

public abstract class InsertInto extends Component
{
	protected InsertInto( COOQL cooql )
	{
		super( cooql );
	}

	protected void XcleanUp() {}

	public String toString()
	{
		return " INTO \""+XgetTableName()+"\" (";
	}
}
