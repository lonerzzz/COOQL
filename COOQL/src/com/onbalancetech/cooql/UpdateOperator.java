package com.onbalancetech.cooql;

public abstract class UpdateOperator extends Operator
{
	protected UpdateOperator( COOQL cooql )
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
}
