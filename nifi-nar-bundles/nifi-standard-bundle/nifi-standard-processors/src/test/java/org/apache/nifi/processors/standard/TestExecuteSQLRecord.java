/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.TestJdbcHugeStream;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestExecuteSQLRecord {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteSQLRecord", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteSQLRecord", "debug");
        LOGGER = LoggerFactory.getLogger(TestExecuteSQLRecord.class);
    }

    final static String DB_LOCATION = "target/db";

    final static String QUERY_WITH_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = ${person.id}";

    final static String QUERY_WITHOUT_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = 10";

    final static String QUERY_WITHOUT_EL_WITH_PARAMS = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID < ? AND REL.ID < ?";


    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteSQLRecord.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM persons");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(true);
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test(expected = AssertionError.class)
    public void testNoIncomingConnectionAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(false);
        runner.run();
    }

    @Test
    public void testNoIncomingConnection() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, false, null, true);
        assertEquals(ProvenanceEventType.RECEIVE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    public void testSelectQueryInFlowFile() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
        assertEquals(ProvenanceEventType.FORK, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals(ProvenanceEventType.FETCH, runner.getProvenanceEvents().get(1).getEventType());
    }

    @Test
    public void testMaxRowsPerFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(AbstractExecuteSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 200);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals("record.count", "5");
        firstFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals("record.count", "5");
        lastFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testInsertStatementCreatesFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0).assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "0");
    }

    @Test
    public void testNoRowsStatementCreatesEmptyFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(0);
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "0");
        firstFlowFile.assertContentEquals("");
    }

    @Test
    public void testWithSqlException() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        //No incoming flow file containing a query, and an exception causes no outbound flowfile.
        // There should be no flow files on either relationship
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 0);
    }

    public void invokeOnTriggerRecords(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
            throws InitializationException, ClassNotFoundException, SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(AbstractExecuteSQL.QUERY_TIMEOUT, queryTimeout.toString() + " secs");
        }

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        TestJdbcHugeStream.loadTestData2Database(con, 100, 200, 100);
        LOGGER.info("test data loaded");

        // ResultSet size will be 1x200x100 = 20 000 rows
        // because of where PER.ID = ${person.id}
        final int nrOfRows = 20000;

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = (attrs == null) ? new HashMap<>() : attrs;
            attributes.put("person.id", "10");
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if (setQueryProperty) {
            runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecuteSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecuteSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecuteSQL.RESULT_QUERY_DURATION));
        assertEquals(durationTime, fetchTime + executionTime);
    }


    /**
     * Simple implementation only for ExecuteSQL processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

}
