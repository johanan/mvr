package file

import (
	"encoding/hex"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/zeebo/assert"
)

func TestDecimalToFixedBytes(t *testing.T) {
	tests := []struct {
		name        string
		decimalStr  string
		scale       int
		precision   int
		expected    string // Hex representation
		expectError bool
	}{
		{
			name:        "Positive Standard",
			decimalStr:  "639529.823302734000000",
			scale:       15,
			precision:   38,
			expected:    "0000000000000022ab4259eb6bcb2f80",
			expectError: false,
		},
	}

	for _, tt := range tests {
		tc := tt // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			value := decimal.RequireFromString(tc.decimalStr)

			decimalStr := value.StringFixed(int32(tc.scale))
			assert.Equal(t, tc.decimalStr, decimalStr)

			result, err := convertDecimalStringToUnscaledInt(decimalStr, tt.scale)
			assert.NoError(t, err)
			converted, err := bigIntToFixedBytes(result, tc.precision)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, hex.EncodeToString(converted))
			}
		})
	}
}

func TestBigIntToFixedBytes(t *testing.T) {
	tests := []struct {
		name        string
		decimalStr  string
		scale       int
		precision   int
		expected    string // Hex representation
		expectError bool
	}{
		{
			name:        "Positive Standard",
			decimalStr:  "123456789012345.123456789012345",
			scale:       15,
			precision:   38,
			expected:    "000000018ee90ff6c371e7c1d1605f79",
			expectError: false,
		},
		{
			name:        "Negative Standard",
			decimalStr:  "-123456789012345.123456789012345",
			scale:       15,
			precision:   38,
			expected:    "fffffffe7116f0093c8e183e2e9fa088",
			expectError: false,
		},
		{
			name:        "Zero Value",
			decimalStr:  "0",
			scale:       2,
			precision:   38,
			expected:    "00000000000000000000000000000000",
			expectError: false,
		},
		{
			name:        "Large Positive Number",
			decimalStr:  "12345678901234567890123456789012345678",
			scale:       0,
			precision:   38,
			expected:    "0949b0f6f0023313c4499050de38f34e",
			expectError: false,
		},
		{
			name:        "Fractional Part Exactly Scale",
			decimalStr:  "123.456",
			scale:       3,
			precision:   8,
			expected:    "0001e240", // 123456 in hex is 0x1e240
			expectError: false,
		},
	}

	for _, tt := range tests {
		tc := tt // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			unscaledInt, err := convertDecimalStringToUnscaledInt(tc.decimalStr, tc.scale)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			fixedBytes, err := bigIntToFixedBytes(unscaledInt, tc.precision)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, hex.EncodeToString(fixedBytes))
			}
		})
	}
}

func TestConvertDecimalStringToUnscaledInt(t *testing.T) {
	tests := []struct {
		name           string
		decimalStr     string
		scale          int
		expectedResult string
		expectError    bool
	}{
		{
			name:           "Standard Decimal",
			decimalStr:     "123.45",
			scale:          2,
			expectedResult: "12345",
			expectError:    false,
		},
		{
			name:           "No Decimal Point",
			decimalStr:     "123",
			scale:          2,
			expectedResult: "12300",
			expectError:    false,
		},
		{
			name:           "Fractional Part Less Than Scale",
			decimalStr:     "123.4",
			scale:          2,
			expectedResult: "12340",
			expectError:    false,
		},
		{
			name:           "Fractional Part Equals Scale",
			decimalStr:     "123.45",
			scale:          2,
			expectedResult: "12345",
			expectError:    false,
		},
		{
			name:           "Fractional Part Exceeds Scale",
			decimalStr:     "123.456",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Negative Decimal",
			decimalStr:     "-123.45",
			scale:          2,
			expectedResult: "-12345",
			expectError:    false,
		},
		{
			name:           "Zero Value",
			decimalStr:     "0",
			scale:          2,
			expectedResult: "0",
			expectError:    false,
		},
		{
			name:           "Large Number Without Decimal",
			decimalStr:     "12345678901234567890",
			scale:          5,
			expectedResult: "1234567890123456789000000",
			expectError:    false,
		},
		{
			name:           "Large Number With Decimal",
			decimalStr:     "639529.823302734000000",
			scale:          15,
			expectedResult: "639529823302734000000",
			expectError:    false,
		},
		{
			name:           "Leading Zeros",
			decimalStr:     "000123.45",
			scale:          2,
			expectedResult: "12345",
			expectError:    false,
		},
		{
			name:           "Only Fractional Part",
			decimalStr:     ".45",
			scale:          2,
			expectedResult: "45",
			expectError:    false,
		},
		{
			name:           "Empty String",
			decimalStr:     "",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Invalid Format Multiple Decimals",
			decimalStr:     "123.45.67",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Non-Numeric Characters",
			decimalStr:     "123.4a",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Negative Number Without Decimal",
			decimalStr:     "-123",
			scale:          3,
			expectedResult: "-123000",
			expectError:    false,
		},
		{
			name:           "Fractional Part Exactly Scale",
			decimalStr:     "123.456",
			scale:          3,
			expectedResult: "123456",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertDecimalStringToUnscaledInt(tt.decimalStr, tt.scale)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result.String())
			}
		})
	}
}
