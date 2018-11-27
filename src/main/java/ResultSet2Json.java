import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.jdbc.support.JdbcUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;


public class ResultSet2Json {


    public static ObjectNode convert(ResultSet rs)
            throws SQLException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode base = mapper.createObjectNode();
        ArrayNode json = mapper.createArrayNode();

        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();

        ArrayNode columnsNode = mapper.createArrayNode();
        for (int index = 1; index <= numColumns; index++) {
            String column = JdbcUtils.lookupColumnName(rsmd, index);
                columnsNode.add(column);
        }
        base.putPOJO("columns",columnsNode);

        while (rs.next()) {
            ObjectNode objectNode = mapper.createObjectNode();

            for (int index = 1; index <= numColumns; index++) {
                String column = JdbcUtils.lookupColumnName(rsmd, index);

                Object value = rs.getObject(column);
                if (value == null) {
                    objectNode.putNull(column);
                } else if (value instanceof Integer) {
                    objectNode.put(column, (Integer) value);
                } else if (value instanceof Timestamp) {
                    objectNode.put(column, value.toString());
                } else if (value instanceof String) {
                    objectNode.put(column, (String) value);
                } else if (value instanceof Boolean) {
                    objectNode.put(column, (Boolean) value);
                } else if (value instanceof Date) {
                    objectNode.put(column, ((Date) value).getTime());
                } else if (value instanceof Long) {
                    objectNode.put(column, (Long) value);
                } else if (value instanceof Double) {
                    objectNode.put(column, (Double) value);
                } else if (value instanceof Float) {
                    objectNode.put(column, (Float) value);
                } else if (value instanceof BigDecimal) {
                    objectNode.put(column, (BigDecimal) value);
                } else if (value instanceof Byte) {
                    objectNode.put(column, (Byte) value);
                } else if (value instanceof byte[]) {
                    objectNode.put(column, (byte[]) value);
                } else {
                    throw new IllegalArgumentException("Unable to map object type: " + value.getClass());
                }

            }

            json.add(objectNode);
        }
        base.putPOJO("records", json);
        return base;
    }

}
