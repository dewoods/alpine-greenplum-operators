/*
 * Copyright (c) 2014 Dillon Woods <dewoods@gmail.com>
 *
 * alpine-greenplum-operators is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */
package com.alpine.plugins;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import com.alpine.datamining.api.AnalyticSource;
import com.alpine.datamining.api.impl.db.DataBaseAnalyticSource;
import com.alpine.datamining.api.impl.db.TableInfo;
import com.alpine.datamining.model.AnalyticModel;
import com.alpine.datamining.model.AnalyticModelGeneric;
import com.alpine.datamining.model.impl.AnalyticModelGenericImpl;
import com.alpine.miner.workflow.operator.OperatorInputTableInfo;
import com.alpine.utility.db.GPSqlType;
import com.alpine.datamining.parameter.AlgorithmParameter;
import com.alpine.datamining.parameter.ParameterValidationMessage;
import com.alpine.datamining.parameter.LinkValidationMessage;
import com.alpine.datamining.parameter.TableNameParameter;
import com.alpine.datamining.parameter.SchemaNameParameter;
import com.alpine.datamining.parameter.SingleValueParameter;
import com.alpine.datamining.parameter.ParameterType;
import com.alpine.datamining.parameter.ParameterFactory;
import com.alpine.datamining.plugin.AnalyzerPlugin;
import com.alpine.datamining.plugin.PluginRunningListener;
import com.alpine.datamining.plugin.PluginMetaData;
import com.alpine.datamining.plugin.PluginUtil;
import com.alpine.datamining.plugin.PluginConstants.DataSourceType;
import com.alpine.datamining.plugin.PluginConstants.DataSourcePlatform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * This plugin is used to update a target table using a source table
 * Only one preceding operator is accepted, and the output of that operator will
 * be used as the source in an UPDATE..FROM statement
 * <p>
 * Can optionally analyze the target table after update
 * <p>
 * This operator does not do any column validation, the columns in the source table
 * must match those in the target table
 */
public class GreenplumUpdatePlugin implements AnalyzerPlugin<AnalyticModelGeneric> {
    private static final String P_TARGET_SCHEMA = "Target Schema";
    private static final String P_TARGET_TABLE = "Target Table";
    private static final String P_JOIN_KEY = "Join Key (ex: col1,col2)";
    private static final String P_TARGET_ANALYZE = "Analyze After Insert";

    /**
     * This operator supports only database sources
     */
    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.DB;
    }

    /**
     * This operator only supports the Greenplum database
     */
    @Override
    public List<DataSourcePlatform> getSupportedDataSourcePlatform() {
        List<DataSourcePlatform> supportedPlatforms = new ArrayList<DataSourcePlatform>();

        supportedPlatforms.add( DataSourcePlatform.Greenplum );

        return supportedPlatforms;
    }

    /**
     * Returns metadata associated with this operator
     *
     * @return      operator metadata
     */
    @Override
    public PluginMetaData getPluginMetaData() {
        return new PluginMetaData(
            "Greenplum Data Operators",
            "Dillon Woods",
            1,
            "Greenplum Update From",
            "/com/alpine/plugin/madlib/resource/icon/join.png",
            "Greenplum Update From"
        );
    }

    /**
     * Define the parameters this operator will accept through the UI
     *
     * @return      List of AlgorithmParameter objects
     */
    @Override
    public List<AlgorithmParameter> getParameters() {
        List<AlgorithmParameter> parameterList = new ArrayList<AlgorithmParameter>();

        parameterList.add( new SchemaNameParameter( P_TARGET_SCHEMA, "" ) );
        parameterList.add( new SingleValueParameter( P_TARGET_TABLE, null, "", ParameterType.STRING_TYPE, true ) );
        parameterList.add( new SingleValueParameter( P_JOIN_KEY, null, "", ParameterType.STRING_TYPE, true ) );
        parameterList.add( ParameterFactory.createSimpleRequiredParameterWithAvailableValues(
            P_TARGET_ANALYZE,
            Boolean.FALSE.toString(),
            ParameterType.BOOLEAN_TYPE,
            Boolean.TRUE.toString(),
            Boolean.FALSE.toString()
        ) );

        return parameterList;
    }

    /**
     * Custom parameter validation beyond type checking provided by framework
     *
     * @param   nameValuePair   Name/Value pairs of all parameters as set by the user
     * @return                  List of validation messages if any parameters are invalid                                
     */
    @Override
    public List<ParameterValidationMessage> validateParameters( HashMap<String,Object> nameValuePair ) {
        return null;
    }

    /**
     * Used by the UI to validate incoming operators when a user tries to create a link to this plugin
     * Note that framework will automaticall validate the Class of incoming operators as defined
     * by the getInputClassList method
     *
     * @param   precedingOutPutObject   The object the user is trying to connect
     * @param   existingInputObjects    The objects already connected to this operator
     * @return                          Validation message if incoming operator is invalid
     */
    @Override
    public LinkValidationMessage validateInputLink( List<Object> precedingOutPutObject, List<Object> existingInputObjects ) {
        return null;
    }

    /**
     * Validate whether or not all link connections are ok, only used for special requirements
     *
     * @param   validateAllInputLinks   The object the user is trying to connect
     * @return                          Validation message if any incoming operators are invalid
     */
    @Override
    public LinkValidationMessage validateAllInputLinks( List<Object> allInputObjects ) {
        return null;
    }

    /**
     * Validate whether or not all link connections are ok, only used for special requirements
     *
     * @param   source      The operator connected to this one
     * @param   listener    Handles notification while plugin is running
     * @return              Output of operator
     */
    @Override
    public AnalyticModelGeneric run( AnalyticSource source, PluginRunningListener listener ) throws Exception {
        PreparedStatement pstmt;
        int retcode;

        /**
         * Use the same database connection attached to the input operator
         */
        DataBaseAnalyticSource dbsource = (DataBaseAnalyticSource) source;
        Connection conn = dbsource.getConnection();

        /**
         * This operator will output a status table with step/result pairs
         */
        List<String> columnNames = Arrays.asList( new String[] { "Step", "Result" } );
        List<String> columnTypes = Arrays.asList( new String[] { GPSqlType.INSTANCE.getTextType(), GPSqlType.INSTANCE.getTextType() } );
        List< List<String> > rows = new ArrayList< List<String> >();

        /**
         * Create fully qualified table names for the source and target tables
         */
        TableInfo tableInfo = dbsource.getTableInfo();
        String sourceSchema = tableInfo.getSchema();
        String sourceTable = tableInfo.getTableName();
        String sourceFQN = sourceSchema + "." + sourceTable;

        String targetSchema = PluginUtil.getAlgorithmParameterValue( source, P_TARGET_SCHEMA );
        String targetTable = PluginUtil.getAlgorithmParameterValue( source, P_TARGET_TABLE );
        String targetFQN = targetSchema + "." + targetTable;

        /**
         * Make sure the target table exists, error if not
         */
        pstmt = conn.prepareStatement( "SELECT count(*) FROM pg_tables WHERE schemaname = ? AND tablename = ?" );
        pstmt.setString( 1, targetSchema );
        pstmt.setString( 2, targetTable );
        ResultSet rs = pstmt.executeQuery();
        rs.next();

        if( rs.getInt(1) < 1 ) {
            throw new Exception( "Error: Target Table '" + targetFQN + "' does not exit" );
        }
        rows.add( Arrays.asList( new String[] { "Target Exists", "true" } ) );

        /**
         * Extract source/target Join Key, expecting comma separated list
         */
        String joinKey = PluginUtil.getAlgorithmParameterValue( source, P_JOIN_KEY );
        List<String> joinKeyColumns = Arrays.asList( joinKey.split( "[ ,]" ) );
        String joinKeyPredicate = "1=1";

        for( String key : joinKeyColumns ) {
            joinKeyPredicate += " AND " + sourceTable + "." + key + " = " + targetTable + "." + key;
        }

        /**
         * Build set statements for all columns not in the join key
         */
        List<String> sourceColumnNames = tableInfo.getColumnNames();
        List<String> setColumnNames = new ArrayList<String>();
        String setStatement = "";

        for( String sc : sourceColumnNames ) {
            if( joinKeyColumns.contains( sc ) )
                continue;

            setColumnNames.add( sc );
        }

        for( int i=0; i < setColumnNames.size(); i++ ) {
            String col = setColumnNames.get(i);
            if( i > 0 )
                setStatement += " , ";
            setStatement += col + " = " + sourceTable + "." + col;
        }

        /**
         * Attempt to run the update
         */
        pstmt = conn.prepareStatement( "UPDATE " + targetFQN + " SET " + setStatement + " FROM " + sourceFQN + " WHERE " + joinKeyPredicate );
        retcode = pstmt.executeUpdate();
        conn.commit();
        rows.add( Arrays.asList( new String[] { "Update FROM", String.valueOf( retcode ) } ) );

        /**
         * Analyze the target table after load if necessary
         */
        String analyzeTarget = PluginUtil.getAlgorithmParameterValue( source, P_TARGET_ANALYZE );
        if( analyzeTarget.equals( "true" ) ) {
            pstmt = conn.prepareStatement( "ANALYZE " + targetFQN );
            retcode = pstmt.executeUpdate();
            conn.commit();
            rows.add( Arrays.asList( new String[] { "Analyze Target", String.valueOf( retcode ) } ) );
        }

        /**
         * Return the status table
         */
        AnalyticModelGeneric result = new AnalyticModelGenericImpl(
            "Greenplum Insert Result",
            columnNames,
            columnTypes,
            rows
        );

        return result;
    }

    /**
     * Alters the operator label as displayed in the UI, used for localization
     *
     * @param   locale      The locale of the user
     * @param   key         The un-localized label
     * @return              The label to be displaed
     */
    @Override
    public String getDisplayLabel( Locale locale, String key ) {
        return key;
    }

    /**
     * Defines the valid Classes of input operators
     *
     * @return      List of valid input classes
     */
    @Override
    public List<String> getInputClassList() {
        ArrayList<String> inputClassList = new ArrayList<String>();

        inputClassList.add( OperatorInputTableInfo.class.getName() );

        return inputClassList;
    }

}
