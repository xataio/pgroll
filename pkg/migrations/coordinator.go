// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"slices"
)

// Coordinator is responsible for executing a series of database actions in a specific orderedActions.
// It ensures that each action is executed only once, even if it is added multiple times.
type Coordinator struct {
	actions        map[string]DBAction
	orderedActions []string
}

func NewCoordinator(actions []DBAction) *Coordinator {
	actionsMap := make(map[string]DBAction, len(actions))
	orderedActions := make([]string, 0)
	for _, action := range actions {
		if act, exists := actionsMap[action.ID()]; exists {
			idx := slices.Index(orderedActions, act.ID())
			orderedActions = slices.Delete(orderedActions, idx, idx+1)
		} else {
			actionsMap[action.ID()] = action
		}
		orderedActions = append(orderedActions, action.ID())
	}
	return &Coordinator{
		actions:        actionsMap,
		orderedActions: orderedActions,
	}
}

// Execute runs all actions in the orderedActions they were added to the coordinator.
func (c *Coordinator) Execute(ctx context.Context) error {
	for _, id := range c.orderedActions {
		action, exists := c.actions[id]
		if !exists {
			return fmt.Errorf("action %s not found", id)
		}
		if err := action.Execute(ctx); err != nil {
			return fmt.Errorf("failed to execute action %s: %w", id, err)
		}
	}
	return nil
}
