package com.onbalancetech.cooql;

public abstract class Component
{
	private COOQL cooql;

	protected Component( COOQL cooql )
	{
		this.cooql = cooql;
	}

	public abstract String toString();

	protected abstract void XcleanUp();

	protected final COOQL XgetCOOQL()
	{
		return cooql;
	}

	protected final String XgetTableName()
	{
		String name = this.getClass().getSimpleName();
		return name.substring( name.indexOf( "_" ) +1 );
	}
}
