package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
        try {
            stmt.setLong(index++, value.toInstant(ZoneOffset.UTC).toEpochMilli());
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting localdatetime parameter", ex);
        }
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
