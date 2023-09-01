# Alternatives and motivation

Alembic does not track enum changes, leaving it on our shoulders.
We had to manually add plain sql to migrations. 

Later, this sql was brought into a useful function, allowing us to stop copying code from our past migrations.

The next logical step was to automate writing calls to this function.

Before doing so, existing solutions were considered.


## [Alembic Enums](https://pypi.org/project/alembic-enums/)
At first, this library was found.

It performs the same work as the self-written function - 
provides an interface for renaming enum values and changing server defaults.

Tracking the changes is left on the developer's shoulders.


## [alembic-autogenerate-enums](https://pypi.org/project/alembic-autogenerate-enums/)

Next we encountered this library. 


In contrast to the previous one it tracked changes in the SQLAlchemy schema.

It was the closest to the required ideal, but it had significant shortcomings.

It lacked the ability to delete and rename enum values, which was critical.

Also, the order of values was being lost, which made the comparison of values incorrect. 

## Unsolved problem

Another problem not solved by the above-mentioned libraries is the creation and deletion of enums themselves.

- Enum is created by op.create_table, but DOES NOT being dropped by op.drop_table
- Enum is not created when op.add_column is called with fresh enum
- Enum is not dropped when it is unused

These issues are also fixed by our library

## Conclusion

Manual enum handling becomes another feature to keep in mind, in an already complex project.
This becomes another negative factor that stops you from using enums.

This is wrong, changes to enums should not be discouraged.

Our accumulated experience and the lack of a satisfying solution led us to create this library.

Our goal is to cover up all issues with autogeneration of enums
