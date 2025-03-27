package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Wrapper for ResultSet to mute exceptions, throws DataAccessException instead when reading values
 */
public class ResultSetWrapper {
    private final ResultSet rs;

    /**
     * Creates a new instance of ResultSetWrapper
     *
     * @param rs ResultSet
     */
    public ResultSetWrapper(ResultSet rs) {
        this.rs = rs;
    }

    /**
     * Returns the wrapped ResultSet
     *
     * @return ResultSet
     */
    public ResultSet getResultSet() {
        return rs;
    }

    /**
     * Moves the cursor to the next row
     *
     * @return true if there is a next row
     */
    public boolean next() {
        try {
            return rs.next();
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting next result", ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a boolean.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is false
     * @throws DataAccessException if a SQLException occurs
     */
    public boolean getBoolean(String column) {
        try {
            return rs.getBoolean(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting boolean from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a boolean.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is false
     * @throws DataAccessException if a SQLException occurs
     */
    public boolean getBoolean(int column) {
        try {
            return rs.getBoolean(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting boolean from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a byte.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public byte getByte(String column) {
        try {
            return rs.getByte(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting byte from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a byte.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public byte getByte(int column) {
        try {
            return rs.getByte(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting byte from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a byte array. The bytes represent the raw values returned by the driver.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public byte[] getBytes(String column) {
        try {
            return rs.getBytes(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting bytes from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a byte array. The bytes represent the raw values returned by the driver.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public byte[] getBytes(int column) {
        try {
            return rs.getBytes(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting bytes from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a Date object.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Date getDate(String column) {
        try {
            return rs.getDate(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting date from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a Date object.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Date getDate(int column) {
        try {
            return rs.getDate(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting date from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a double.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is 0.0
     * @throws DataAccessException if a SQLException occurs
     */
    public double getDouble(String column) {
        try {
            return rs.getDouble(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting double from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a double.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is 0.0
     * @throws DataAccessException if a SQLException occurs
     */
    public double getDouble(int column) {
        try {
            return rs.getDouble(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting double from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a float.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is 0.0f
     * @throws DataAccessException if a SQLException occurs
     */
    public float getFloat(String column) {
        try {
            return rs.getFloat(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting float from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a float.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is 0.0f
     * @throws DataAccessException if a SQLException occurs
     */
    public float getFloat(int column) {
        try {
            return rs.getFloat(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting float from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as an int
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public int getInt(String column) {
        try {
            return rs.getInt(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting int from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as an int
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public int getInt(int column) {
        try {
            return rs.getInt(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting int from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a String.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public String getString(String column) {
        try {
            return rs.getString(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting string from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a String.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public String getString(int column) {
        try {
            return rs.getString(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting string from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a long.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public long getLong(String column) {
        try {
            return rs.getLong(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting long from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a long.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public long getLong(int column) {
        try {
            return rs.getLong(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting long from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as an Object.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Object getObject(int column) {
        try {
            return rs.getObject(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting object from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as an Object of the specified type.
     *
     * @param column column name
     * @param type   Class of the type to retrieve
     * @param <T>    the type of the object
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public <T> T getObject(String column, Class<T> type) {
        try {
            return rs.getObject(column, type);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting object from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as an Object of the specified type.
     *
     * @param column column index
     * @param type   Class of the type to retrieve
     * @param <T>    the type of the object
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public <T> T getObject(int column, Class<T> type) {
        try {
            return rs.getObject(column, type);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting object from column " + column, ex);
        }
    }


    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a short.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public short getShort(String column) {
        try {
            return rs.getShort(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting short from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a short.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is 0
     * @throws DataAccessException if a SQLException occurs
     */
    public short getShort(int column) {
        try {
            return rs.getShort(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting short from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a Time object.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Time getTime(String column) {
        try {
            return rs.getTime(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting time from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a Time object.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Time getTime(int column) {
        try {
            return rs.getTime(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting time from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a Timestamp object.
     *
     * @param column column name
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Timestamp getTimestamp(String column) {
        try {
            return rs.getTimestamp(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting timestamp from column " + column, ex);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a Timestamp object.
     *
     * @param column column index
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws DataAccessException if a SQLException occurs
     */
    public Timestamp getTimestamp(int column) {
        try {
            return rs.getTimestamp(column);
        } catch (SQLException ex) {
            throw new DataAccessException("Error getting timestamp from column " + column, ex);
        }
    }
}
