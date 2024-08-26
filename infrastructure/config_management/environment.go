package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type environmentConfig struct{}

func Environment() Provider {
	return &environmentConfig{}
}

func (c *environmentConfig) GetString(key string) (string, error) {
	return getEnvironmentVariable(key)
}

func (c *environmentConfig) GetInt(key string) (int, error) {
	valueStr, err := getEnvironmentVariable(key)

	if err != nil {
		return 0, err
	}

	return strconv.Atoi(valueStr)
}

func (c *environmentConfig) GetBool(key string) (bool, error) {
	valueStr, err := getEnvironmentVariable(key)

	if err != nil {
		return false, err
	}

	return strconv.ParseBool(valueStr)
}

func (c *environmentConfig) GetMap(key string) (map[string]string, error) {
	var result = make(map[string]string)
	valueStr, err := getEnvironmentVariable(key)

	if err != nil {
		return nil, err
	}
	valueStrArray := strings.Split(valueStr, ",")

	for _, valueStrItem := range valueStrArray {
		valueSplitted := strings.Split(valueStrItem, ":")
		result[valueSplitted[0]] = valueSplitted[1]
	}

	return result, nil
}

func (c *environmentConfig) GetList(key string) ([]string, error) {

	valueStr, err := getEnvironmentVariable(key)

	if err != nil {
		return nil, err
	}
	return strings.Split(valueStr, ","), nil
}

func getEnvironmentVariable(variableName string) (string, error) {
	environmentVariable := os.Getenv(variableName)
	if environmentVariable == "" {
		return "", errors.New(fmt.Sprintf("Environment Variable (%s) is not provided", variableName))
	}
	return environmentVariable, nil
}
