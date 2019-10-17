/* SPDX-License-Identifier: Apache-2.0 */


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.Vector;


public class GaianConnection extends Thread {

    public static final String DEFAULT_SELECT = "select * from mysql_employees_salary";
    public static String resultString = "";
    public static DemoUI demoUI;
    public static TableModel tableModel;

    public static void main(String[] args) {

        // Initialize the UI
        JFrame jFrame = new JFrame("DemoUI");
        demoUI =  new DemoUI();
        jFrame.setContentPane(demoUI.getRootPanel());
        jFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        jFrame.pack();
        jFrame.setVisible(true);

        demoUI.getButtonQuery().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                new GaianConnection().start();
            }
        });

    }

    private static void createGaianStatement(String arg, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();

        queryGaian(arg, statement);

        statement.close();
    }

    private static void queryGaian(String arg, Statement statement) throws SQLException {
        ResultSet resultSet1 = getResultSet(arg, statement);

        printResultSet(resultSet1);
        tableModel = buildTableModel(resultSet1);

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
                resultString = resultString + objNode.toString() + "\t";
            }
        } else {
            System.out.print(node);
            resultString = resultString + node.toString();        }
        System.out.println();
        resultString = resultString + "\n";
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            demoUI.getLabelQueryResult().setVisible(false);
            demoUI.getTableResult().setVisible(false);
            //resultString = "";
            final String gaianDBURL = getGaianDBURL(demoUI.getTextFieldUsername().getText());
            Connection connection = createGaianConnection(gaianDBURL);
            createGaianStatement(demoUI.getTextFieldQuery().getText(), connection);
            closeGaianConnection(connection);
            demoUI.getTableResult().setModel(tableModel);
            demoUI.getTableResult().setVisible(true);
            //demoUI.getLabelQueryResult().setText(resultString);
        } catch (Exception except) {
            demoUI.getLabelQueryResult().setVisible(true);
            except.printStackTrace();
            demoUI.getLabelQueryResult().setText(except.toString());
        }
    }


    private static TableModel buildTableModel(ResultSet rs)
            throws SQLException {

        ResultSetMetaData metaData = rs.getMetaData();

        // names of columns
        Vector<String> columnNames = new Vector<String>();
        int columnCount = metaData.getColumnCount();
        for (int column = 1; column <= columnCount; column++) {
            columnNames.add(metaData.getColumnName(column));
        }

        // data of the table
        Vector<Vector<Object>> data = new Vector<Vector<Object>>();
        while (rs.next()) {
            Vector<Object> vector = new Vector<Object>();
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                vector.add(rs.getObject(columnIndex));
            }
            data.add(vector);
        }

        return new DefaultTableModel(data, columnNames);

    }
}
