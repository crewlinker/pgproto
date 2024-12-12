# pgproto

## Features
- Uses pg_query for full query support (uses postgres source code)
- Well-tested, name coverage and having run the parsing on complex queries from some complex open-source projects.
- Simpler, by focussing on postgres only. And narrowly scoping the complexity by forcing "well-typed" sql as input.

## Usage
- Explain what "well-typed" means, and what the standard is
- COULD explain what types are supported explictly, and how to use "CREATE CAST" to work around this
- Explain how creating custom casts help move the complexity to postgres
- Have some sort of playground to show how the parsing works

## TODO
- SHOULD limit the types that are supported to a usefull subset for protobuf (well-known only?)
- Support MERGE and UPSERT
- Use protobuf for the parsing result
- Test and support stuff like this: https://www.postgresql.org/docs/current/queries-union.html
- Can we fuzz the parsing?