package com.onbalancetech.cooql;

public abstract class TypeConverter<T>
{
	public final T convertToStorageFormat( Object sourceObject )
	{
		if (sourceObject == null) return null;

		return doInboundConversion( sourceObject );
	}

	public final Object convertFromStorageFormat( T sourceObject )
	{
		if (sourceObject == null) return null;

		return doOutboundConversion( sourceObject );
	}

	protected abstract T doInboundConversion( Object sourceObject );

	protected abstract Object doOutboundConversion( T sourceObject );
}
