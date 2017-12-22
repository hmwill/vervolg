pub mod sql;

#[cfg(test)]
mod tests {
    #[test]
    fn test_hello() {
        let hello_world = "Hello World";
        let result = super::sql::parse_statement(&hello_world);

        assert!(result.is_ok());
    }
}
