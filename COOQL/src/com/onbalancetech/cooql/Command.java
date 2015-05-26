package com.onbalancetech.cooql;

public abstract class Command extends Component
{
	protected Command( COOQL cooql )
	{
		super( cooql );
	}

	protected void XcleanUp() {}
}
