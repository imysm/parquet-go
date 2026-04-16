package common

// DeepCopyMap deep-copies a map[string]any for safe storage.
// Returns a new map with all values recursively copied.
// This handles all Go types that can appear in parquet row maps.
func DeepCopyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = deepCopyValue(v)
	}
	return dst
}

// deepCopyValue recursively copies a value for deep-copy semantics.
func deepCopyValue(v any) any {
	if v == nil {
		return nil
	}
	switch vv := v.(type) {
	case map[string]any:
		return DeepCopyMap(vv)
	case []any:
		if vv == nil {
			return nil
		}
		cp := make([]any, len(vv))
		for i, e := range vv {
			cp[i] = deepCopyValue(e)
		}
		return cp
	case []byte:
		cp := make([]byte, len(vv))
		copy(cp, vv)
		return cp
	// Immutable / value types: safe to return as-is
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return v
	default:
		return v // fallback for unknown types
	}
}