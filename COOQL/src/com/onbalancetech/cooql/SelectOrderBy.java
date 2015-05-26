package com.onbalancetech.cooql;

public abstract class SelectOrderBy extends OrderBy
{
	protected SelectOrderBy( COOQL cooql )
	{
		super( cooql );
	}

	public String toString()
	{
		return "ORDER BY";
	}
}
