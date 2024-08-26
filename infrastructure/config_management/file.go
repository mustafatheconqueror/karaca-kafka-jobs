package config

import (
	"github.com/spf13/viper"
	"strconv"
)

type fileConfig struct {
	configFile string
	v          *viper.Viper
}

func (f *fileConfig) GetString(key string) (string, error) {
	return f.v.GetString(key), nil
}

func (f *fileConfig) GetInt(key string) (int, error) {
	return strconv.Atoi((f.v.Get(key)).(string))
}

func (f *fileConfig) GetBool(key string) (bool, error) {
	return strconv.ParseBool((f.v.Get(key)).(string))
}

func (f *fileConfig) GetMap(key string) (map[string]string, error) {

	return f.v.GetStringMapString(key), nil
}
func (f *fileConfig) GetList(key string) ([]string, error) {
	return f.v.GetStringSlice(key), nil
}
