package com.onbalancetech.cooql;

import com.datastax.driver.core.UDTValue;

public abstract class COOQLUDTValueImpl implements COOQLUDTValue
{
	protected UDTValue udtValue;

	public final void setUDTValue( UDTValue udtValue )
	{
		this.udtValue = udtValue;
	}
}
