/// End-to-end tests for the Quiver ORM pipeline using SQLite.
///
/// Tests the full flow: parse .quiver schema -> generate DDL -> create tables ->
/// insert data -> query data -> verify results.
#[cfg(test)]
mod tests;
