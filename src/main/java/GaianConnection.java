/* SPDX-License-Identifier: Apache-2.0 */


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Properties;

public class GaianConnection {

    public static final String DEFAULT_SELECT = "select * from mysql_employees_salary";

    public static void main(String[] args) {
        try {
            final String gaianDBURL = getGaianDBURL(args[0]);
            Connection connection = createGaianConnection(gaianDBURL);

            createGaianStatement(args[1], connection);
            closeGaianConnection(connection);
        } catch (Exception except) {
            except.printStackTrace();
        }
    }

    private static void createGaianStatement(String arg, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();

        queryGaian(arg, statement);

        statement.close();
    }

    private static void queryGaian(String arg, Statement statement) throws SQLException {
        ResultSet resultSet1 = getResultSet(arg, statement);

        printResultSet(resultSet1);

        resultSet1.close();
    }

    private static ResultSet getResultSet(String arg, Statement statement) throws SQLException {
        if (arg != null) {
            return statement.executeQuery(arg);
        } else {
            return statement.executeQuery(DEFAULT_SELECT);
        }
    }

    private static void closeGaianConnection(Connection connection) throws SQLException {
        connection.close();
        System.out.println("connection closed");
    }

    private static Connection createGaianConnection(String gaianDBURL) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        Connection connection = DriverManager.getConnection(gaianDBURL);

        System.out.println("connection opened");
        connection.setSchema("GAIANDB");
        return connection;
    }

    private static void printResultSet(ResultSet resultSet1) throws SQLException {
        ObjectNode json = ResultSet2Json.convert(resultSet1);

        printContent(json, "columns");

        printContent(json, "records");
    }

    private static void printContent(ObjectNode json, String nodeName) {
        final JsonNode columns = json.get(nodeName);
        printNode(columns);
    }

    private static String getGaianDBURL(String arg) throws IOException {
        final String rangerURL = getGaianHost();
        String dbURL = "jdbc:derby://{0}/gaiandb;user={1};proxy-user=gaiandb;proxy-pwd=passw0rd";
        String finalURL = MessageFormat.format(dbURL, rangerURL, arg);
        return finalURL;
    }

    private static String getGaianHost() throws IOException {
        final InputStream resourceAsStream = GaianConnection.class.getClassLoader().getResourceAsStream("application.properties");

        Properties properties = new Properties();
        properties.load(resourceAsStream);
        resourceAsStream.close();

        return properties.getProperty("gaian.url");
    }

    private static void printNode(JsonNode node) {
        if (node.isArray()) {
            for (final JsonNode objNode : node) {
                System.out.print(objNode + "\t");
            }
        } else {
            System.out.print(node);
        }
        System.out.println();
    }
}
