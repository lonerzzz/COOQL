package com.onbalancetech.cooql;

public abstract class UpdateWhere extends Where
{
	protected UpdateWhere( COOQL cooql )
	{
		super( cooql );
	}

	public void executeQuery()
	{
		Xexecute();
	}

	protected void XcleanSet()
	{
		COOQL.getDataHolder().getBuffer().append( this.toString() );
		int index = COOQL.getDataHolder().getBuffer().indexOf( ",}" );
		COOQL.getDataHolder().getBuffer().replace( index, index+2, " " );
	}
}
