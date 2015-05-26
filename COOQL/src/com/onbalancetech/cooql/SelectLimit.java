package com.onbalancetech.cooql;

public class SelectLimit extends Operator
{
	public SelectLimit( COOQL cooql )
	{
		super( cooql );
	}

	public String toString()
	{
		return "LIMIT";
	}
}
