package com.onbalancetech.cooql;

public abstract class OrderBy extends Component
{
	protected OrderBy( COOQL cooql )
	{
		super( cooql );
	}

	public String toString()
	{
		return " ORDER BY";
	}

	protected void XcleanUp() {}
}
