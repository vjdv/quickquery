package net.vjdv.quickquery;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a group of conditions for filtering data in a query.
 * Conditions can be combined using AND or OR operators.
 * The SQL representation is built incrementally as conditions are added.
 */
public class ConditionGroup {
    private final StringBuilder sql = new StringBuilder();
    private final Map<Integer, Object> indexParameters = new HashMap<>();
    private int index = 0;

    /**
     * Default constructor for ConditionGroup.
     * Initializes an empty condition group.
     */
    public ConditionGroup() {
    }

    /**
     * Creates a new ConditionGroup with the first condition.
     *
     * @param firstCondition the first condition to add to the group
     */
    public ConditionGroup(Condition firstCondition) {
        add(firstCondition);
    }

    /**
     * Creates a new ConditionGroup with a condition based on a column and value.
     * This constructor uses the default operator '='.
     *
     * @param column the column name to filter by
     * @param value  the value to compare against
     */
    public ConditionGroup(String column, Object value) {
        this(new Condition(column, value));
    }

    /**
     * Creates a ConditionGroup with provided conditions combined with OR.
     *
     * @param conditions the conditions to combine
     * @return a ConditionGroup representing the OR combination of the conditions
     */
    public static ConditionGroup or(Condition... conditions) {
        if (conditions.length == 0) {
            throw new IllegalArgumentException("At least one condition is required for OR");
        }
        ConditionGroup group = new ConditionGroup(conditions[0]);
        for (int i = 1; i < conditions.length; i++) {
            group.or(conditions[i]);
        }
        return group;
    }

    /**
     * Creates a ConditionGroup with provided conditions combined with AND.
     *
     * @param conditions the conditions to combine
     * @return a ConditionGroup representing the AND combination of the conditions
     */
    public static ConditionGroup and(Condition... conditions) {
        if (conditions.length == 0) {
            throw new IllegalArgumentException("At least one condition is required for AND");
        }
        ConditionGroup group = new ConditionGroup(conditions[0]);
        for (int i = 1; i < conditions.length; i++) {
            group.and(conditions[i]);
        }
        return group;
    }

    /**
     * Add a condition to the group
     *
     * @param c the condition to add
     * @return the current ConditionGroup instance for method chaining
     */
    private ConditionGroup add(Condition c) {
        sql.append(c.column()).append(" ").append(c.operator()).append(" ?");
        indexParameters.put(index++, c.value());
        return this;
    }

    /**
     * Adds a condition to the group with an AND operator.
     *
     * @param c the condition to add
     * @return the current ConditionGroup instance for method chaining
     */
    public ConditionGroup and(Condition c) {
        sql.append(" AND ");
        return add(c);
    }

    /**
     * Adds a condition to the group with an OR operator.
     *
     * @param c the condition to add
     * @return the current ConditionGroup instance for method chaining
     */
    public ConditionGroup or(Condition c) {
        sql.append(" OR ");
        return add(c);
    }

    /**
     * Adds a condition to the group with an AND operator based on column and value.
     *
     * @param column the column name to filter by
     * @param value  the value to compare against
     * @return the current ConditionGroup instance for method chaining
     */
    public ConditionGroup and(String column, Object value) {
        return and(new Condition(column, value));
    }

    /**
     * Adds a condition to the group with an OR operator based on column and value.
     *
     * @param column the column name to filter by
     * @param value  the value to compare against
     * @return the current ConditionGroup instance for method chaining
     */
    public ConditionGroup or(String column, Object value) {
        return or(new Condition(column, value));
    }

    /**
     * Returns the parameters for the conditions in the group.
     *
     * @return a map of index to parameter value
     */
    public Map<Integer, Object> getIndexParameters() {
        return indexParameters;
    }

    /**
     * Returns the SQL representation of the condition group.
     *
     * @return a string representing the SQL conditions
     */
    public String getSql() {
        return sql.toString();
    }
}
