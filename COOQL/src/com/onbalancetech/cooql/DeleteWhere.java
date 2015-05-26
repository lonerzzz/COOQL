package com.onbalancetech.cooql;

public abstract class DeleteWhere extends Where
{
	protected DeleteWhere(COOQL cooql)
	{
		super( cooql );
	}

	public void executeQuery()
	{
		Xexecute();
	}

	public void executeQueryAsynchronously()
	{
		XexecuteAsynchronously();
	}

	protected void XcleanSet()
	{
		COOQL.getDataHolder().getBuffer().append( this.toString() );
	}
}
