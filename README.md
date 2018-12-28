pglogical is a logical replication system implemented entirely as a PostgreSQL extension. Fully integrated, it requires no triggers or external programs. This alternative to physical replication is a highly efficient method of replicating data using a publish/subscribe model for selective replication. For more details, please visit: https://www.2ndquadrant.com/en/resources/pglogical/. 

The pglogical 2 release brings new features allowing it to be used for even more use-cases and also several bug fixes and behavior improvements listed below:

- PG 11 support
- Improved compatibility with pglogical 1.x provider
- Improved error reporting
- Row filtering on provider
- Column filtering on provider
- Delayed replication
- MS Windows support (source code only)
- Ability to convert physical standby to logical replication
- Can publish data from PostgreSQL to a Postgres-XL subscriber
- Greatly improved performance for replication of large INSERT/COPY transactions
- Improved memory management.

pglogical 2's maintenance happens on the `REL2_x_STABLE` branch.

pglogical3 is currently only available for 2ndQuadrant support customers. Follow the 2ndQuadrant blog (https://blog.2ndquadrant.com/) and info@2ndquadrant.com or the bdr/pglogical forum and mailing list at https://groups.google.com/a/2ndquadrant.com/forum/#!forum/bdr-list for updates.
