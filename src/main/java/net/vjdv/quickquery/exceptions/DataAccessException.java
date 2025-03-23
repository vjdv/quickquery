package net.vjdv.quickquery.exceptions;

/**
 * Runtime data access exception
 */
public class DataAccessException extends RuntimeException {
    /**
     * Creates a new instance of DataAccessException
     *
     * @param message exception message
     */
    public DataAccessException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of DataAccessException
     *
     * @param message exception message
     * @param cause   exception cause
     */
    public DataAccessException(String message, Throwable cause) {
        super(message, cause);
    }

}
