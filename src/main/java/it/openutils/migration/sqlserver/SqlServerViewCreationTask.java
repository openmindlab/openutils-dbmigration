/*
 * Copyright (c) Openmind.  All rights reserved. http://www.openmindonline.it
 */
package it.openutils.migration.sqlserver;

import javax.sql.DataSource;


/**
 * @author Danilo Ghirardelli
 * @version $Id:SqlServerViewCreationTask.java 3143 2007-09-24 19:50:49Z fgiust $
 */
public class SqlServerViewCreationTask extends SqlServerObjCreationTask
{

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DataSource dataSource)
    {
        setQualifiedObjQuery("select count(*) from dbo.sysobjects where id = object_id(?) and OBJECTPROPERTY(id, N'IsView') = 1");
        setUnqualifiedObjQuery("select count(*) from dbo.sysobjects where id = object_id(?) and OBJECTPROPERTY(id, N'IsView') = 1");
        super.execute(dataSource);
    }
}