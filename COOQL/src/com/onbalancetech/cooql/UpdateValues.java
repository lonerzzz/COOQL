package com.onbalancetech.cooql;

public class UpdateValues extends Component
{
	protected UpdateValues( COOQL cooql )
	{
		super( cooql );
	}

	public String toString()
	{
		return "\""+XgetTableName()+"\" SET ";
	}

	protected void XcleanUp() {}
}
