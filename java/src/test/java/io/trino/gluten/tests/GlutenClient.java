package io.trino.gluten.tests;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class GlutenClient
{
    private Connection conn;

    public GlutenClient(String url, Properties properties)
            throws SQLException
    {
        conn = DriverManager.getConnection(url, properties);
    }

    public static void main(String[] args)
            throws SQLException
    {
        String url = "jdbc:trino://127.0.0.1:8080/";

        Properties properties = new Properties();
        properties.setProperty("user", "test");

        GlutenClient client = new GlutenClient(url, properties);
        client.executeQueryAndPrintResult("SELECT * FROM lineitem limit 10");

        client.close();
    }

    public void executeQueryAndPrintResult(String sql)
            throws SQLException
    {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
        System.out.println("run query successful! ");

        rs.close();
        stmt.close();
    }

    public void close()
            throws SQLException
    {
        conn.close();
    }
}
