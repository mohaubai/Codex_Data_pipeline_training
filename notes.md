Idempotency - By defination if we run the pipeline for once or for 100th time the results in the database should be identical.

CTEs - This are used when we need to answer a query using other query result. Or when it feels like we cannot answer the whole using one ingle query then we can use a CTE

Window functions - Instead of clubbing the rows into 1 row like GROUPBY does window function OVER() is used for adding additional detail on each group.
we have RANK() if matches/tied then same RANK is assigned 1 1 3
DENSE_RANK() if two matches then both get 1 1 but the next onw will get RANK 2
ROW_NUMBER - each row gets its own rank number like 1, 2, 3

PARTITION BY → splits into groups, ranks within each group
              keeps all rows unlike GROUP BY

LAG(column)  OVER (PARTITION BY ... ORDER BY ...) → previous row value
LEAD(column) OVER (PARTITION BY ... ORDER BY ...) → next row value
NULL appears on first row (no previous) or last row (no next) 

NULL Handling could be handled using COALESCE(..., 0) so that NULL is replaced with 0 because in SQL NULL means unknown.

CASE WHEN:
CASE 
    WHEN condition THEN 'value'
    WHEN condition THEN 'value'
    ELSE 'default'
END AS column_name

- Always close with END
- Always add ELSE or unmatched rows return NULL
- Think of it as if/elif/else in Python