package migrations

// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
const MaxNameLength = 63

func validateName(name string) error {
	if len(name) > MaxNameLength {
		return InvalidNameLengthError{Name: name, Max: MaxNameLength}
	}
	return nil
}

func ValidateName(name string) error {
	return validateName(name)
}
