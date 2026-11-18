package fire

import (
	"slices"
)

func slicesClone(in []Detection) []Detection { return slices.Clone(in) }

func sortInts(a []int) { slices.Sort(a) }

func sortPoints(p []point) { slices.SortFunc(p, comparePoints) }
