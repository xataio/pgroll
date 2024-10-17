// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetForeignKey struct {
	Table      string              `json:"table"`
	Column     string              `json:"column"`
	References ForeignKeyReference `json:"references"`
	Up         string              `json:"up"`
	Down       string              `json:"down"`
}

var _ Operation = (*OpSetForeignKey)(nil)

func (o *OpSetForeignKey) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	// Create a NOT VALID foreign key constraint on the new column.
	if err := o.addForeignKeyConstraint(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to add foreign key constraint: %w", err)
	}

	return table, nil
}

func (o *OpSetForeignKey) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// Validate the foreign key constraint
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.References.Name)))
	if err != nil {
		return err
	}

	return nil
}

func (o *OpSetForeignKey) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	return nil
}

func (o *OpSetForeignKey) Validate(ctx context.Context, s *schema.Schema) error {
	if err := o.References.Validate(s); err != nil {
		return ColumnReferenceError{
			Table:  o.Table,
			Column: o.Column,
			Err:    err,
		}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}

func (o *OpSetForeignKey) addForeignKeyConstraint(ctx context.Context, conn db.DB) error {
	tempColumnName := TemporaryName(o.Column)

	onDelete := "NO ACTION"
	if o.References.OnDelete != "" {
		onDelete = strings.ToUpper(string(o.References.OnDelete))
	}

	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s ON DELETE %s NOT VALID",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.References.Name),
			pq.QuoteIdentifier(tempColumnName),
			pq.QuoteIdentifier(o.References.Table),
			onDelete,
		))

	return err
}
