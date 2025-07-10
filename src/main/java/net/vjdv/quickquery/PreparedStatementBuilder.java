package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Function;

/**
 * A builder class to create a PreparedStatement with parameters
 */
public class PreparedStatementBuilder {
    private final PreparedStatement stmt;
    private int index = 1;

    /**
     * Create a PreparedStatementBuilder instance
     *
     * @param stmt the prepared statement to be used
     */
    public PreparedStatementBuilder(PreparedStatement stmt) {
        this.stmt = stmt;
    }

    /**
     * Validates constructor parameters for a record class
     *
     * @param constructor the constructor
     * @param components  the record components
     * @return true if the constructor matches the components
     */
    private static boolean matchesParameterTypes(Constructor<?> constructor, RecordComponent[] components) {
        Class<?>[] expectedTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            expectedTypes[i] = components[i].getType();
        }
        Class<?>[] paramTypes = constructor.getParameterTypes();
        if (paramTypes.length != expectedTypes.length) {
            return false;
        }
        for (int i = 0; i < paramTypes.length; i++) {
            if (!paramTypes[i].equals(expectedTypes[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Set an array parameter to consecutive parameter index
     *
     * @param value the array value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setArray(Array value) {
        try {
            stmt.setArray(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting array parameter", ex);
        }
    }

    /**
     * Set an ascii stream parameter to consecutive parameter index
     *
     * @param value the ascii stream value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setAsciiStream(java.io.InputStream value) {
        try {
            stmt.setAsciiStream(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting ascii stream parameter", ex);
        }
    }

    /**
     * Set a big decimal parameter to consecutive parameter index
     *
     * @param value the big decimal value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBigDecimal(java.math.BigDecimal value) {
        try {
            stmt.setBigDecimal(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting bigdecimal parameter", ex);
        }
    }

    /**
     * Set a binary stream parameter to consecutive parameter index
     *
     * @param value the binary stream value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBinaryStream(java.io.InputStream value) {
        try {
            stmt.setBinaryStream(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting binary stream parameter", ex);
        }
    }

    /**
     * Set a blob parameter to consecutive parameter index
     *
     * @param value the blob value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBlob(Blob value) {
        try {
            stmt.setBlob(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting blob parameter", ex);
        }
    }

    /**
     * Set a boolean parameter to consecutive parameter index
     *
     * @param value the boolean value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBoolean(boolean value) {
        try {
            stmt.setBoolean(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting boolean parameter", ex);
        }
    }

    /**
     * Set a byte parameter to consecutive parameter index
     *
     * @param value the byte value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setByte(byte value) {
        try {
            stmt.setByte(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting byte parameter", ex);
        }
    }

    /**
     * Set a byte array parameter to consecutive parameter index
     *
     * @param value the byte array value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBytes(byte[] value) {
        try {
            stmt.setBytes(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting bytes parameter", ex);
        }
    }

    /**
     * Set a character stream parameter to consecutive parameter index
     *
     * @param value the character stream value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setCharacterStream(java.io.Reader value) {
        try {
            stmt.setCharacterStream(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting character stream parameter", ex);
        }
    }

    /**
     * Set a clob parameter to consecutive parameter index
     *
     * @param value the clob value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setClob(Clob value) {
        try {
            stmt.setClob(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting clob parameter", ex);
        }
    }

    /**
     * Set a date parameter to consecutive parameter index
     *
     * @param value the date value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setDate(Date value) {
        try {
            stmt.setDate(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting date parameter", ex);
        }
    }

    /**
     * Set a double parameter to consecutive parameter index
     *
     * @param value the double value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setDouble(double value) {
        try {
            stmt.setDouble(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting double parameter", ex);
        }
    }

    /**
     * Set a float parameter to consecutive parameter index
     *
     * @param value the float value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setFloat(float value) {
        try {
            stmt.setFloat(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting float parameter", ex);
        }
    }

    /**
     * Set an int parameter to consecutive parameter index
     *
     * @param value the int value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setInt(int value) {
        try {
            stmt.setInt(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting int parameter", ex);
        }
    }

    /**
     * Set a LocalDateTime parameter converted to utc millis to consecutive parameter index, nano precision is lost
     *
     * @param value the LocalDateTime value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setLocalDateTimeLong(LocalDateTime value) {
        return setZonedDateTimeLong(value.atZone(ZoneId.systemDefault()));
    }

    /**
     * Set a ZonedDateTime parameter converted to utc millis to consecutive parameter index, nano precision is lost
     *
     * @param value the ZonedDateTime value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setZonedDateTimeLong(ZonedDateTime value) {
        return setLong(value.withZoneSameInstant(ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    /**
     * Set a long parameter to consecutive parameter index
     *
     * @param value the long value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setLong(long value) {
        try {
            stmt.setLong(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting long parameter", ex);
        }
    }

    /**
     * Set a ncharacter stream parameter to consecutive parameter index
     *
     * @param value the ncharacter stream value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNCharacterStream(java.io.Reader value) {
        try {
            stmt.setNCharacterStream(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting ncharacter stream parameter", ex);
        }
    }

    /**
     * Set a nclob parameter to consecutive parameter index
     *
     * @param value the nclob value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNClob(NClob value) {
        try {
            stmt.setNClob(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting nclob parameter", ex);
        }
    }

    /**
     * Set a nclob parameter to consecutive parameter index
     *
     * @param value the nclob value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNClob(java.io.Reader value) {
        try {
            stmt.setNClob(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting nclob parameter", ex);
        }
    }

    /**
     * Set a nstring parameter to consecutive parameter index
     *
     * @param value the nstring value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNString(String value) {
        try {
            stmt.setNString(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting nstring parameter", ex);
        }
    }

    /**
     * Set a null parameter to consecutive parameter index
     *
     * @param sqlType the sql type
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNull(int sqlType) {
        try {
            stmt.setNull(index++, sqlType);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting null parameter", ex);
        }
    }

    /**
     * Set a null parameter to consecutive parameter index
     *
     * @param sqlType  the sql type
     * @param typeName the type name
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNull(int sqlType, String typeName) {
        try {
            stmt.setNull(index++, sqlType, typeName);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting null parameter", ex);
        }
    }

    /**
     * Set an object parameter to consecutive parameter index
     *
     * @param value the object value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setObject(Object value) {
        try {
            stmt.setObject(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting object parameter", ex);
        }
    }

    /**
     * Set an ref parameter to consecutive parameter index
     *
     * @param value the ref value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setRef(Ref value) {
        try {
            stmt.setRef(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting ref parameter", ex);
        }
    }

    /**
     * Set a rowid parameter to consecutive parameter index
     *
     * @param value the rowid value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setRowId(RowId value) {
        try {
            stmt.setRowId(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting rowid parameter", ex);
        }
    }

    /**
     * Set a short parameter to consecutive parameter index
     *
     * @param value the short value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setShort(short value) {
        try {
            stmt.setShort(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting short parameter", ex);
        }
    }

    /**
     * Set a sqlxml parameter to consecutive parameter index
     *
     * @param value the sqlxml value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setSQLXML(SQLXML value) {
        try {
            stmt.setSQLXML(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting sqlxml parameter", ex);
        }
    }

    /**
     * Set a string parameter to consecutive parameter index
     *
     * @param value the string value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setString(String value) {
        try {
            stmt.setString(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting string parameter", ex);
        }
    }

    /**
     * Set a time parameter to consecutive parameter index
     *
     * @param value the time value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTime(Time value) {
        try {
            stmt.setTime(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting time parameter", ex);
        }
    }

    /**
     * Set a timestamp parameter to consecutive parameter index
     *
     * @param value the timestamp value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTimestamp(Timestamp value) {
        try {
            stmt.setTimestamp(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting timestamp parameter", ex);
        }
    }

    /**
     * Set a url parameter to consecutive parameter index
     *
     * @param value the url value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setURL(java.net.URL value) {
        try {
            stmt.setURL(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting url parameter", ex);
        }
    }

    /**
     * Set parameters from a map where the key is the parameter index and the value is the parameter value
     *
     * @param parameters a map of parameters
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setParameters(Map<Integer, Object> parameters) {
        try {
            for (var entry : parameters.entrySet()) {
                int index = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String x) {
                    this.stmt.setString(index, x);
                } else if (value instanceof Integer x) {
                    this.stmt.setInt(index, x);
                } else if (value instanceof Long x) {
                    this.stmt.setLong(index, x);
                } else if (value instanceof Boolean x) {
                    this.stmt.setBoolean(index, x);
                } else if (value instanceof Double x) {
                    this.stmt.setDouble(index, x);
                } else if (value instanceof Float x) {
                    this.stmt.setFloat(index, x);
                } else if (value instanceof Short x) {
                    this.stmt.setShort(index, x);
                } else if (value instanceof Byte x) {
                    this.stmt.setByte(index, x);
                } else if (value instanceof byte[] x) {
                    this.stmt.setBytes(index, x);
                } else if (value == null) {
                    this.stmt.setNull(index, Types.NULL);
                } else {
                    this.stmt.setObject(index, value);
                }
            }
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting parameters", ex);
        }
    }

    /**
     * Sets a result mapper function to be used in the query execution
     *
     * @param function the function to map the result
     * @param <T>      the type of the result
     * @return a PreparedStatementExecutor instance
     */
    public <T> PreparedStatementExecutor<T> resultMapper(Function<ResultSetWrapper, T> function) {
        return new PreparedStatementExecutor<>(stmt, function);
    }

    /**
     * Uses a record class to map the result set to a record instance. Uses record components so components name must match the column names
     *
     * @param clazz the record class
     * @param <T>   the type of the record
     * @return a PreparedStatementExecutor instance
     */
    public <T> PreparedStatementExecutor<T> resultMapper(Class<T> clazz) {
        if (!clazz.isRecord()) {
            throw new DataAccessException("Class " + clazz.getName() + " is not a record");
        }
        RecordComponent[] components = clazz.getRecordComponents();
        Constructor<?> constructor = null;
        for (var c : clazz.getDeclaredConstructors()) {
            if (matchesParameterTypes(c, components)) {
                constructor = c;
                break;
            }
        }
        if (constructor == null) {
            throw new DataAccessException("No matching constructor found for record class " + clazz.getName());
        }
        Constructor<?> finalConstructor = constructor;
        Function<ResultSetWrapper, T> function = (rs) -> {
            Object[] values = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                var c = components[i];
                if (c.getType() == String.class) {
                    values[i] = rs.getString(c.getName());
                } else if (c.getType() == long.class) {
                    values[i] = rs.getLong(c.getName());
                } else if (c.getType() == int.class) {
                    values[i] = rs.getInt(c.getName());
                } else if ((c.getType() == boolean.class)) {
                    values[i] = rs.getBoolean(c.getName());
                } else if (c.getType() == double.class) {
                    values[i] = rs.getDouble(c.getName());
                } else if (c.getType() == float.class) {
                    values[i] = rs.getFloat(c.getName());
                } else if (c.getType() == short.class) {
                    values[i] = rs.getShort(c.getName());
                } else if (c.getType() == byte.class) {
                    values[i] = rs.getByte(c.getName());
                } else if (c.getType() == byte[].class) {
                    values[i] = rs.getBytes(c.getName());
                } else if (c.getType() == LocalDateTime.class) {
                    values[i] = rs.getLocalDateTimeLong(c.getName());
                } else if (c.getType() == ZonedDateTime.class) {
                    values[i] = rs.getZonedDateTimeLong(c.getName());
                } else {
                    throw new DataAccessException("Type " + c.getType().getSimpleName() + " not supported");
                }
            }
            try {
                return (T) finalConstructor.newInstance(values);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                throw new DataAccessException("Error creating record instance", ex);
            }
        };
        return new PreparedStatementExecutor<>(stmt, function);
    }

    /**
     * Execute query without result using PreparedStatement.execute
     */
    public void execute() {
        try (stmt) {
            stmt.execute();
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing query", ex);
        }
    }

    /**
     * Execute an update statement using PreparedStatement.executeUpdate
     *
     * @return the number of rows affected
     */
    public int executeUpdate() {
        try (stmt) {
            return stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing update", ex);
        }
    }

    /**
     * Execute a query statement using PreparedStatement.executeQuery and wrap the result in a ResultSetWrapper
     *
     * @return the ResultSetWrapper
     */
    public ResultSetWrapper executeQuery() {
        try {
            return new ResultSetWrapper(stmt.executeQuery());
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing query", ex);
        }
    }

    /**
     * Execute an insert statement and return the autoincremented id
     *
     * @return the autoincremented id
     */
    public int insertAutoincrement() {
        try (stmt) {
            stmt.execute();
            try (var rs = stmt.getGeneratedKeys()) {
                return rs.getInt(1);
            }
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing insert", ex);
        }
    }

}
