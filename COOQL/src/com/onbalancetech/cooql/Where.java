package com.onbalancetech.cooql;

public abstract class Where extends ExecutableComponent
{
	protected Where(COOQL cooql)
	{
		super( cooql );
	}

	public String toString()
	{
		return "} WHERE";
	}

	protected void XcleanUp() {}
}
