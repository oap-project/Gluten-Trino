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
package io.trino.gluten.tests.tpch;

import com.google.common.io.Resources;
import io.trino.gluten.tests.GlutenClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;

public class TpchFuncTest
{
    public static void testOneQuery(String queryId)
            throws SQLException
    {
        TpchFuncTest test = new TpchFuncTest();

        String queryFile = "q" + queryId + ".sql";
        String sql = test.readSql(queryFile);

        String url = "jdbc:trino://127.0.0.1:8080/";

        Properties properties = new Properties();
        properties.setProperty("user", "test");

        GlutenClient client = new GlutenClient(url, properties);
        client.executeQueryAndPrintResult(sql);

        client.close();
    }

    public static void testAllQuery()
            throws SQLException
    {
        for (int i = 1; i <= 22; i++) {
            testOneQuery(Integer.toString(i));
        }
    }

    public static void main(String[] args)
            throws SQLException
    {
        testOneQuery(args[0]);
    }

    String readSql(String path)
    {
        try {
            URL url = getResource(TpchFuncTest.class, path);
            System.out.println(url);

            String sql = Resources.toString(url, StandardCharsets.UTF_8).replace("${database}.${schema}.", "")
                    .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"")
                    .replace("${scale}", "1");
            System.out.println(sql);
            return sql;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
