package com.onbalancetech.cooql;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSetFuture;
import com.onbalancetech.cooql.COOQL.DataHolder;

public abstract class COOQLResultSetFuture
{
	protected Map<String,Map<String,TypeConverter>> converterMap;
	protected DataHolder dataHolder;
	protected ResultSetFuture resultSetFuture;

	protected COOQLResultSetFuture( ResultSetFuture resultSetFuture, DataHolder dataHolder )
	{
		this.resultSetFuture = resultSetFuture;
		this.dataHolder = dataHolder;
	}

	public abstract COOQLResultSet getUninterruptibly();

	public abstract COOQLResultSet getUninterruptibly( long timeout, TimeUnit unit )  throws TimeoutException;

	public void cancel( boolean mayInterruptIfRunning )
	{
		resultSetFuture.cancel( mayInterruptIfRunning );
	}
}
