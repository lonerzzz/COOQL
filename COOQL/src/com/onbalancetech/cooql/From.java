package com.onbalancetech.cooql;

public abstract class From extends Component
{
	protected From( COOQL cooql )
	{
		super( cooql );
	}

	public String toString()
	{
		return " FROM \""+XgetTableName()+"\" {";
	}

	protected void XappendFrom()
	{
		CharSequence cs = COOQL.getDataHolder().getBuffer().subSequence( COOQL.getDataHolder().getBuffer().indexOf( "{" )+1,
				COOQL.getDataHolder().getBuffer().indexOf( "}" ) );
		COOQL.getDataHolder().getBuffer().replace( COOQL.getDataHolder().getBuffer().indexOf( "[" ), COOQL.getDataHolder().getBuffer().indexOf( "]" ) +1, cs.toString() );
		COOQL.getDataHolder().getBuffer().replace( COOQL.getDataHolder().getBuffer().indexOf( "{" ), COOQL.getDataHolder().getBuffer().indexOf( "}" ) +2, "" );
		COOQL.getDataHolder().getBuffer().replace( COOQL.getDataHolder().getBuffer().indexOf( " FROM" ) -1, COOQL.getDataHolder().getBuffer().indexOf( " FROM" ), "" );
	}

	protected void XcleanUp() {}
}
