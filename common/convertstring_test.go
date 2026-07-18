package common

import "testing"

func TestParsestringtoint64(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		scale   int
		want    int64
		wantErr bool
	}{
		{"空字符串", "", 8, 0, false},
		{"纯整数", "123456", 8, 12345600000000, false},
		{"位数刚刚好的小数", "123.45678912", 8, 12345678912, false},
		{"需要末尾补零的小数", "123.4", 8, 12340000000, false},
		{"0和纯小数", "0.05", 8, 5000000, false},
		// 错误捕捉测试
		{"错误：小数位数超限", "123.456789123", 8, 0, true},
		{"错误：多个小数点", "12.3.4", 8, 0, true},
		{"错误：非法字符", "abc", 8, 0, true},
		{"错误：小数点后带非数字", "12.3a", 8, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parsestringtoint64(tt.s, tt.scale)
			// 检查 error 状态是否符合预期
			if (err != nil) != tt.wantErr {
				t.Errorf("Parsestringtoint64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// 检查结果是否符合预期
			if got != tt.want {
				t.Errorf("Parsestringtoint64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseint64tostring(t *testing.T) {
	tests := []struct {
		name  string
		n     int64
		scale int
		want  string
	}{
		{"常规数字", 123456789, 8, "1.23456789"},
		{"末尾带0会被抹去", 12300000000, 8, "123"},
		{"长度小于scale需要前置补0", 12345, 8, "0.00012345"},
		{"数字为0", 0, 8, "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Parseint64tostring(tt.n, tt.scale)
			if got != tt.want {
				t.Errorf("Parseint64tostring() = %v, want %v", got, tt.want)
			}
		})
	}
}
