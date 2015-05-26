package com.onbalancetech.cooql;

public abstract class TypeConverter
{
	public Object convert( Object sourceObject )
	{
		if (sourceObject == null) return null;

		return doConversion( sourceObject );
	}

	protected abstract Object doConversion( Object sourceObject );
}
