// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"github.com/xataio/pgroll/pkg/schema"
)

type TriggerDirection string

const (
	TriggerDirectionUp   TriggerDirection = "up"
	TriggerDirectionDown TriggerDirection = "down"
)

type TriggerConfig struct {
	Name                string
	Direction           TriggerDirection
	Columns             map[string]*schema.Column
	SchemaName          string
	TableName           string
	PhysicalColumn      string
	LatestSchema        string
	SQL                 string
	NeedsBackfillColumn string
}

// TriggerFunctionName returns the name of the trigger function
// for a given table and column.
func TriggerFunctionName(tableName, columnName string) string {
	return "_pgroll_trigger_" + tableName + "_" + columnName
}

// TriggerName returns the name of the trigger for a given table and column.
func TriggerName(tableName, columnName string) string {
	return TriggerFunctionName(tableName, columnName)
}
