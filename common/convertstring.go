package common

import (
	"fmt"
	"strconv"
	"strings"
)

func Parsestringtoint64(s string, scale int) (int64, error) {
	if s == "" {
		return 0, nil
	}
	multiple := power10(scale)
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Error parse %v to int: %v", s, err)
		}
		return i * multiple, nil
	}
	if len(parts) != 2 {
		return 0, fmt.Errorf("Incorrect number of . in %v", s)
	}

	integerPart, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing integer part: %v", err)
	}
	decimalPart := parts[1]
	if len(decimalPart) > scale {
		return 0, fmt.Errorf("decimal length %v larger than %v", len(decimalPart), scale)
	} else if len(decimalPart) < scale {
		contatenationAmount := scale - len(decimalPart)
		decimalPart += strings.Repeat("0", contatenationAmount)
	}
	decimalPartInt, err := strconv.ParseInt(decimalPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing decimal part %v to int: %v", decimalPart, err)
	}
	return integerPart*multiple + decimalPartInt, nil

}

func power10(scale int) int64 {
	number := 1
	for i := 0; i < scale; i++ {
		number *= 10
	}
	return int64(number)
}
