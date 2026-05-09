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
    private final Connection conn;
    private final PreparedStatement stmt;
    private int index = 1;

    /**
     * Create a PreparedStatementBuilder instance
     *
     * @param stmt the prepared statement to be used
     */
    public PreparedStatementBuilder(Connection conn, PreparedStatement stmt) {
        this.conn = conn;
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
        return setArray(value, index++);
    }

    /**
     * Set an array parameter to specific parameter index
     *
     * @param value          the array value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setArray(Array value, int parameterIndex) {
        try {
            stmt.setArray(parameterIndex, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting array parameter", ex);
        }
    }

    /**
     * Set a text array parameter to consecutive parameter index
     *
     * @param values the text array values
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTextArray(String[] values) {
        return setTextArray(values, index++);
    }

    /**
     * Set a text array parameter to specific parameter index
     *
     * @param values         the text array values
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTextArray(String[] values, int parameterIndex) {
        try {
            Array array = conn.createArrayOf("text", values);
            stmt.setArray(parameterIndex, array);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting text array parameter", ex);
        }
    }

    /**
     * Set an ascii stream parameter to consecutive parameter index
     *
     * @param value the ascii stream value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setAsciiStream(java.io.InputStream value) {
        return setAsciiStream(value, index++);
    }

    /**
     * Set an ascii stream parameter to specific parameter index
     *
     * @param value          the ascii stream value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setAsciiStream(java.io.InputStream value, int parameterIndex) {
        try {
            stmt.setAsciiStream(parameterIndex, value);
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
        return setBigDecimal(value, index++);
    }

    /**
     * Set a big decimal parameter to specific parameter index
     *
     * @param value          the big decimal value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBigDecimal(java.math.BigDecimal value, int parameterIndex) {
        try {
            stmt.setBigDecimal(parameterIndex, value);
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
        return setBinaryStream(value, index++);
    }

    /**
     * Set a binary stream parameter to specific parameter index
     *
     * @param value          the binary stream value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBinaryStream(java.io.InputStream value, int parameterIndex) {
        try {
            stmt.setBinaryStream(parameterIndex, value);
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
        return setBlob(value, index++);
    }

    /**
     * Set a blob parameter to specific parameter index
     *
     * @param value          the blob value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBlob(Blob value, int parameterIndex) {
        try {
            stmt.setBlob(parameterIndex, value);
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
        return setBoolean(value, index++);
    }

    /**
     * Set a boolean parameter to specific parameter index
     *
     * @param value          the boolean value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBoolean(boolean value, int parameterIndex) {
        try {
            stmt.setBoolean(parameterIndex, value);
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
        return setByte(value, index++);
    }

    /**
     * Set a byte parameter to specific parameter index
     *
     * @param value          the byte value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setByte(byte value, int parameterIndex) {
        try {
            stmt.setByte(parameterIndex, value);
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
        return setBytes(value, index++);
    }

    /**
     * Set a byte array parameter to specific parameter index
     *
     * @param value          the byte array value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBytes(byte[] value, int parameterIndex) {
        try {
            stmt.setBytes(parameterIndex, value);
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
        return setCharacterStream(value, index++);
    }

    /**
     * Set a character stream parameter to specific parameter index
     *
     * @param value          the character stream value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setCharacterStream(java.io.Reader value, int parameterIndex) {
        try {
            stmt.setCharacterStream(parameterIndex, value);
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
        return setClob(value, index++);
    }

    /**
     * Set a clob parameter to specific parameter index
     *
     * @param value          the clob value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setClob(Clob value, int parameterIndex) {
        try {
            stmt.setClob(parameterIndex, value);
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
        return setDate(value, index++);
    }

    /**
     * Set a date parameter to specific parameter index
     *
     * @param value          the date value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setDate(Date value, int parameterIndex) {
        try {
            stmt.setDate(parameterIndex, value);
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
        return setDouble(value, index++);
    }

    /**
     * Set a double parameter to specific parameter index
     *
     * @param value          the double value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setDouble(double value, int parameterIndex) {
        try {
            stmt.setDouble(parameterIndex, value);
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
        return setFloat(value, index++);
    }

    /**
     * Set a float parameter to specific parameter index
     *
     * @param value          the float value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setFloat(float value, int parameterIndex) {
        try {
            stmt.setFloat(parameterIndex, value);
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
        return setInt(value, index++);
    }

    /**
     * Set an int parameter to specific parameter index
     *
     * @param value          the int value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setInt(int value, int parameterIndex) {
        try {
            stmt.setInt(parameterIndex, value);
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
        return setLocalDateTimeLong(value, index++);
    }

    /**
     * Set a LocalDateTime parameter converted to utc millis to specific parameter index, nano precision is lost
     *
     * @param value          the LocalDateTime value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setLocalDateTimeLong(LocalDateTime value, int parameterIndex) {
        return setZonedDateTimeLong(value.atZone(ZoneId.systemDefault()), parameterIndex);
    }

    /**
     * Set a ZonedDateTime parameter converted to utc millis to consecutive parameter index, nano precision is lost
     *
     * @param value the ZonedDateTime value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setZonedDateTimeLong(ZonedDateTime value) {
        return setZonedDateTimeLong(value, index++);
    }

    /**
     * Set a ZonedDateTime parameter converted to utc millis to specific parameter index, nano precision is lost
     *
     * @param value          the ZonedDateTime value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setZonedDateTimeLong(ZonedDateTime value, int parameterIndex) {
        return setLong(value.withZoneSameInstant(ZoneOffset.UTC).toInstant().toEpochMilli(), parameterIndex);
    }

    /**
     * Set a long parameter to consecutive parameter index
     *
     * @param value the long value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setLong(long value) {
        return setLong(value, index++);
    }

    /**
     * Set a long parameter to specific parameter index
     *
     * @param value          the long value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setLong(long value, int parameterIndex) {
        try {
            stmt.setLong(parameterIndex, value);
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
        return setNCharacterStream(value, index++);
    }

    /**
     * Set a ncharacter stream parameter to specific parameter index
     *
     * @param value          the ncharacter stream value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNCharacterStream(java.io.Reader value, int parameterIndex) {
        try {
            stmt.setNCharacterStream(parameterIndex, value);
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
        return setNClob(value, index++);
    }

    /**
     * Set a nclob parameter to specific parameter index
     *
     * @param value          the nclob value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNClob(NClob value, int parameterIndex) {
        try {
            stmt.setNClob(parameterIndex, value);
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
        return setNClob(value, index++);
    }

    /**
     * Set a nclob parameter to specific parameter index
     *
     * @param value          the nclob value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNClob(java.io.Reader value, int parameterIndex) {
        try {
            stmt.setNClob(parameterIndex, value);
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
        return setNString(value, index++);
    }

    /**
     * Set a nstring parameter to specific parameter index
     *
     * @param value          the nstring value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNString(String value, int parameterIndex) {
        try {
            stmt.setNString(parameterIndex, value);
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
        return setNull(sqlType, index++);
    }

    /**
     * Set a null parameter to specific parameter index
     *
     * @param sqlType        the sql type
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNull(int sqlType, int parameterIndex) {
        try {
            stmt.setNull(parameterIndex, sqlType);
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
        return setNull(sqlType, typeName, index++);
    }

    /**
     * Set a null parameter to specific parameter index
     *
     * @param sqlType        the sql type
     * @param typeName       the type name
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setNull(int sqlType, String typeName, int parameterIndex) {
        try {
            stmt.setNull(parameterIndex, sqlType, typeName);
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
        return setObject(value, index++);
    }

    /**
     * Set an object parameter to specific parameter index
     *
     * @param value          the object value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setObject(Object value, int parameterIndex) {
        try {
            stmt.setObject(parameterIndex, value);
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
        return setRef(value, index++);
    }

    /**
     * Set an ref parameter to specific parameter index
     *
     * @param value          the ref value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setRef(Ref value, int parameterIndex) {
        try {
            stmt.setRef(parameterIndex, value);
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
        return setRowId(value, index++);
    }

    /**
     * Set a rowid parameter to specific parameter index
     *
     * @param value          the rowid value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setRowId(RowId value, int parameterIndex) {
        try {
            stmt.setRowId(parameterIndex, value);
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
        return setShort(value, index++);
    }

    /**
     * Set a short parameter to specific parameter index
     *
     * @param value          the short value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setShort(short value, int parameterIndex) {
        try {
            stmt.setShort(parameterIndex, value);
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
        return setSQLXML(value, index++);
    }

    /**
     * Set a sqlxml parameter to specific parameter index
     *
     * @param value          the sqlxml value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setSQLXML(SQLXML value, int parameterIndex) {
        try {
            stmt.setSQLXML(parameterIndex, value);
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
        return setString(value, index++);
    }

    /**
     * Set a string parameter to specific parameter index
     *
     * @param value          the string value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setString(String value, int parameterIndex) {
        try {
            stmt.setString(parameterIndex, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting string parameter", ex);
        }
    }

    /**
     * Set multiple string parameters to consecutive parameter indexes
     *
     * @param values the string values
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setStrings(String... values) {
        for (String value : values) {
            setString(value);
        }
        return this;
    }

    /**
     * Set a time parameter to consecutive parameter index
     *
     * @param value the time value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTime(Time value) {
        return setTime(value, index++);
    }

    /**
     * Set a time parameter to specific parameter index
     *
     * @param value          the time value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTime(Time value, int parameterIndex) {
        try {
            stmt.setTime(parameterIndex, value);
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
        return setTimestamp(value, index++);
    }

    /**
     * Set a timestamp parameter to specific parameter index
     *
     * @param value          the timestamp value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setTimestamp(Timestamp value, int parameterIndex) {
        try {
            stmt.setTimestamp(parameterIndex, value);
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
        return setURL(value, index++);
    }

    /**
     * Set a url parameter to specific parameter index
     *
     * @param value          the url value
     * @param parameterIndex the parameter index
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setURL(java.net.URL value, int parameterIndex) {
        try {
            stmt.setURL(parameterIndex, value);
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

    /**
     * Adds the current set of parameters as a batch entry and resets the parameter index to 1
     *
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder addBatch() {
        try {
            stmt.addBatch();
            index = 1;
        } catch (SQLException ex) {
            throw new DataAccessException("Error adding batch", ex);
        }
        return this;
    }

    /**
     * Executes the batch of commands added using addBatch and returns an array of update counts for each command in the batch
     *
     * @return an array of update counts for each command in the batch
     */
    public int[] executeBatch() {
        try {
            return stmt.executeBatch();
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing batch", ex);
        }
    }

}
