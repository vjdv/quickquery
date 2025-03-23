package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.ResultSet;
import java.sql.SQLException;

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
}
