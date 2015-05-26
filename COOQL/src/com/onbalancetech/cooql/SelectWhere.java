package com.onbalancetech.cooql;

public class SelectWhere extends Where
{
	protected SelectWhere( COOQL cooql )
	{
		super( cooql );
	}

	protected void XcleanSet()
	{
		COOQL.getDataHolder().getBuffer().append( this.toString() );
	}
}
