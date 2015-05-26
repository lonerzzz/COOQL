package com.onbalancetech.cooql;

import java.util.Map;

public abstract class SelectFrom extends From
{
	Map<String,TypeConverter> converterMap;

	protected SelectFrom( COOQL cooql )
	{
		super( cooql );
	}
}
