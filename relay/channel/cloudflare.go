package channel

import (
	"errors"
	"strings"
)

func ValidateCloudflareTarget(value string, byok bool) error {
	parts := strings.Split(value, "/")
	if byok {
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" ||
			parts[0] != strings.TrimSpace(parts[0]) || parts[1] != strings.TrimSpace(parts[1]) {
			return errors.New("must use the exact format {account_id}/{gateway_id}")
		}
		return nil
	}
	if len(parts) != 1 || strings.TrimSpace(parts[0]) == "" || parts[0] != strings.TrimSpace(parts[0]) {
		return errors.New("must contain exactly one account ID")
	}
	return nil
}
