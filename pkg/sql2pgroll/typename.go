// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strings"

	pgq "github.com/xataio/pg_query_go/v6"
)

// convertTypeName converts a TypeName node to a string.
func convertTypeName(typeName *pgq.TypeName) string {
	ignoredTypeParts := map[string]bool{
		"pg_catalog": true,
	}

	// Build the type name, including any schema qualifiers
	typeParts := make([]string, 0, len(typeName.Names))
	for _, node := range typeName.Names {
		typePart := node.GetString_().GetSval()
		if _, ok := ignoredTypeParts[typePart]; ok {
			continue
		}
		typeParts = append(typeParts, typePart)
	}

	// Build the type modifiers, such as precision and scale for numeric types
	var typeMods []string
	for _, node := range typeName.Typmods {
		if x, ok := node.GetAConst().Val.(*pgq.A_Const_Ival); ok {
			typeMods = append(typeMods, fmt.Sprintf("%d", x.Ival.GetIval()))
		}
	}
	var typeModifier string
	if len(typeMods) > 0 {
		typeModifier = fmt.Sprintf("(%s)", strings.Join(typeMods, ","))
	}

	// Build the array bounds for array types
	var arrayBounds string
	for _, node := range typeName.ArrayBounds {
		bound := node.GetInteger().GetIval()
		if bound == -1 {
			arrayBounds = "[]"
		} else {
			arrayBounds = fmt.Sprintf("%s[%d]", arrayBounds, bound)
		}
	}

	return strings.Join(typeParts, ".") + typeModifier + arrayBounds
}
