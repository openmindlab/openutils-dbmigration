/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.sqlserver;

import javax.sql.DataSource;


/**
 * @author Danilo Ghirardelli
 * @version $Id:SqlServerFunctionCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerFunctionCreationTask extends SqlServerObjCreationTask
{

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DataSource dataSource)
    {
        setQualifiedObjQuery("select count(*) from dbo.sysobjects where id = object_id(?) and xtype in (N'FN', N'IF', N'TF')");
        setUnqualifiedObjQuery("select count(*) from dbo.sysobjects where id = object_id(?) and xtype in (N'FN', N'IF', N'TF')");
        super.execute(dataSource);
    }
}