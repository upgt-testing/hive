/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc.miniHS2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMultiMiniHS2 {

    private MiniHS2 firstMiniHS2;
    private MiniHS2 secondMiniHS2;

    @BeforeClass
    public static void beforeTest() throws Exception {
        Class.forName(MiniHS2.getJdbcDriverName());
    }

    @After
    public void tearDown() throws Exception {
        firstMiniHS2.stop();
        secondMiniHS2.stop();
    }

    /**
     * Test if the MiniHS2 configuration gets passed down to the session
     * configuration
     *
     * @throws Exception
     */
    @Test
    public void testConfInSession() throws Exception {
        HiveConf hiveConf = new HiveConf();
        final String DUMMY_CONF_KEY = "hive.test.minihs2.dummy.config";
        final String DUMMY_CONF_VAL = "dummy.val";
        hiveConf.set(DUMMY_CONF_KEY, DUMMY_CONF_VAL);

        // also check a config that has default in hiveconf
        final String ZK_TIMEOUT_KEY = ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT.varname;
        final String ZK_TIMEOUT = "2562";
        hiveConf.set(ZK_TIMEOUT_KEY, ZK_TIMEOUT);

        // check the config used very often!
        hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

        firstMiniHS2 = new MiniHS2(hiveConf);
        firstMiniHS2.start(new HashMap<String, String>());

        secondMiniHS2 = new MiniHS2(hiveConf);
        secondMiniHS2.start(new HashMap<>());


        Connection firstHS2Conn = DriverManager.getConnection(firstMiniHS2.getJdbcURL(),
                System.getProperty("user.name"), "bar");
        Statement firstStmt = firstHS2Conn.createStatement();

        checkConfVal(DUMMY_CONF_KEY, DUMMY_CONF_KEY + "=" + DUMMY_CONF_VAL, firstStmt);
        checkConfVal(ZK_TIMEOUT_KEY, ZK_TIMEOUT_KEY + "=" + ZK_TIMEOUT, firstStmt);
        checkConfVal(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
                ConfVars.HIVE_SUPPORT_CONCURRENCY.varname + "=" + "false", firstStmt);

        firstStmt.close();
        firstHS2Conn.close();

        Connection secondHS2Conn = DriverManager.getConnection(secondMiniHS2.getJdbcURL(),
                System.getProperty("user.name"), "bar");
        Statement secondStmt = secondHS2Conn.createStatement();

        checkConfVal(DUMMY_CONF_KEY, DUMMY_CONF_KEY + "=" + DUMMY_CONF_VAL, secondStmt);
        checkConfVal(ZK_TIMEOUT_KEY, ZK_TIMEOUT_KEY + "=" + ZK_TIMEOUT, secondStmt);
        checkConfVal(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
                ConfVars.HIVE_SUPPORT_CONCURRENCY.varname + "=" + "false", secondStmt);

        secondStmt.close();
        secondHS2Conn.close();

    }

    private void checkConfVal(String confKey, String confResult, Statement stmt) throws SQLException {
        ResultSet res = stmt.executeQuery("set " + confKey);
        assertTrue(res.next());
        assertEquals("Expected config result", confResult, res.getString(1));
        res.close();
    }

    @Test
    public void testSharedMetastoreBetweenMultipleHS2() throws Exception {
        HiveConf hiveConf = new HiveConf();
        final String tableName = "multi_node_table";
        final String warehouseDir = System.getProperty("java.io.tmpdir") + "/shared_warehouse_" + System.nanoTime();
        final String metastoreDbDir = System.getProperty("java.io.tmpdir") + "/shared_metastore_" + System.nanoTime();
        final String tmpDir = System.getProperty("java.io.tmpdir") + "/hive_tmp_" + System.nanoTime();

        // Metastore configurations
        MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.WAREHOUSE, warehouseDir);
        MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.CONNECT_URL_KEY,
                "jdbc:derby:;databaseName=" + metastoreDbDir + ";create=true");
        MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE, "NONE");
        MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, "hive");

        // Execution engine and local mode setup
        hiveConf.set("hive.execution.engine", "mr");
        hiveConf.set("mapreduce.framework.name", "local");
        hiveConf.set("fs.defaultFS", "file:///");

        // Required temp/scratch dirs for local execution
        hiveConf.set("hadoop.tmp.dir", tmpDir);
        hiveConf.set("hive.exec.local.scratchdir", tmpDir + "/local_scratch");
        hiveConf.set("hive.downloaded.resources.dir", tmpDir + "/resources");
        hiveConf.set("hive.querylog.location", tmpDir + "/logs");

        // Optional but recommended
        hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        hiveConf.setBoolVar(ConfVars.HIVE_STATS_COLLECT_SCANCOLS, false);

        // Start both HS2 instances with shared metastore/warehouse
        firstMiniHS2 = new MiniHS2(hiveConf);
        firstMiniHS2.start(new HashMap<>());

        secondMiniHS2 = new MiniHS2(hiveConf);
        secondMiniHS2.start(new HashMap<>());

        // First HS2: Create table and insert data
        try (
                Connection conn1 = DriverManager.getConnection(firstMiniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
                Statement stmt1 = conn1.createStatement()
        ) {
            stmt1.execute("CREATE TABLE " + tableName + " (id INT)");
            stmt1.execute("INSERT INTO " + tableName + " VALUES (1), (2), (3)");

            ResultSet rs = stmt1.executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs.close();
        }

        // Second HS2: Drop the table
        try (
                Connection conn2 = DriverManager.getConnection(secondMiniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
                Statement stmt2 = conn2.createStatement()
        ) {
            stmt2.execute("DROP TABLE " + tableName);
        }

        // First HS2: Verify table is gone
        try (
                Connection conn1 = DriverManager.getConnection(firstMiniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
                Statement stmt1 = conn1.createStatement()
        ) {
            ResultSet rs = stmt1.executeQuery("SHOW TABLES LIKE '" + tableName + "'");
            assertFalse("Table should have been dropped by second HS2", rs.next());
            rs.close();
        }
    }


}
