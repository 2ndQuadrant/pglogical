# Frequently Asked Questions

pglogical 2.0 introduces some new features, like column filter, row filter and apply
delay. Some related discussion on them:

### The column filter

* What happens if we column filter on a table with OIDS? Can we filter on xmin?
 - For a table with OIDs, column filter works fine. No, we cannot filter system columns
like oid, xmin.

* What happens if a column being filtered on is dropped?
 - Currently in pglogical replication, even primary key can be dropped at provider.
If a column being filtered on is dropped, at provider it is removed from the column
filter too. This can be seen using `pglogical.show_repset_table_info()`.
 Columns at subscriber remain as is, which is correct and expected. At subscriber,
in this state INSERTs replicate, but UPDATEs and DELETEs do not.

* What happens if we add a column, does that automatically get included?
 - If a column is added at provider, it does not automatically get added to the column filter.

### The row filter

* Can we create `row_filter` on table with OIDS? Can we filter on xmin?
 - Yes, `row_filter` works fine for table with OIDs. No, we cannot filter on system columns like xmin.

* What types of function can we execute in a `row_filter`? Can we use a volatile sampling
function, for example?
 - We can execute immutable, stable and volatile functions in a `row_filter`. Caution must
be exercised with regard to writes as any expression which will do writes will throw error and stop replication.
   Volatile sampling function in `row_filter`: This would not work in practice as it would
not get correct snapshot of the data in live system. Theoretically with static data, it works.

* Can we test a JSONB datatype that includes some form of attribute filtering?
 - Yes, `row_filter` on attributes of JSONB datatype works fine.

### The apply delay

* Does `apply_delay` include TimeZone changes, for example Daylight Savings Time? There is a
similar mechanism in physical replication - `recovery_min_apply_delay`. However, if we set some
interval, during the daylight savings times, we might get that interval + the time change in
practice (ie instead of 1h delay you can get 2h delay because of that). This may lead to
stopping and starting the database service twice per year.
 - Yes, `apply_delay` include TimeZone changes, for example Daylight Savings Time. Value of
`apply_delay` stays the same in practice, if daylight savings time switch happens after
subscription was created.
However, we do not recommend running heavy workloads during switching time as pglogical
replication needs some time ( ~ 5 minutes) to recover fine.
