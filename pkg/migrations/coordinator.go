// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"slices"
)

// Coordinator is responsible for executing a series of database actions in a specific order.
// It ensures that each action is executed only once, even if it is added multiple times.
type Coordinator struct {
	actions map[string]DBAction
	order   []string
}

func NewCoordinator(actions []DBAction) *Coordinator {
	actionsMap := make(map[string]DBAction, len(actions))
	order := make([]string, 0)
	for _, action := range actions {
		if act, exists := actionsMap[action.ID()]; exists {
			order = moveIdxToLast(order, slices.Index(order, act.ID()))
		} else {
			actionsMap[action.ID()] = action
			order = append(order, action.ID())
		}
	}
	return &Coordinator{
		actions: actionsMap,
		order:   order,
	}
}

// Execute runs all actions in the order they were added to the coordinator.
func (c *Coordinator) Execute(ctx context.Context) error {
	for _, id := range c.order {
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

func moveIdxToLast(actions []string, idx int) []string {
	if idx < 0 || idx >= len(actions) {
		return actions
	}
	duplicate := actions[idx]
	actions = append(actions[:idx], actions[idx+1:]...)
	if len(actions) > 0 && actions[len(actions)-1] == duplicate {
		// If the last action is the same as the one we want to move, no need to change order
		return actions
	}
	actions = append(actions, duplicate)
	return actions
}
