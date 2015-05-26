package com.onbalancetech.cooql;

public abstract class DeleteOperator extends Operator
{
	protected DeleteOperator( COOQL cooql )
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
