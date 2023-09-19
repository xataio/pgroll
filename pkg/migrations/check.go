package migrations

type CheckConstraint struct {
	Name       string `json:"name"`
	Constraint string `json:"constraint"`
}

func (c *CheckConstraint) Validate() error {
	if c.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if c.Constraint == "" {
		return FieldRequiredError{Name: "constraint"}
	}

	return nil
}
