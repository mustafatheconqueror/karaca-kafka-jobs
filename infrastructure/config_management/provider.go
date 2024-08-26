package config

type Provider interface {
	GetString(key string) (string, error)
	GetInt(key string) (int, error)
	GetBool(key string) (bool, error)
	GetMap(key string) (map[string]string, error)
	GetList(key string) ([]string, error)
}

func From(provider Provider) *builderFactory {
	return &builderFactory{provider}
}

type builderFactory struct {
	provider Provider
}

func (b *builderFactory) StringParam(paramName string) *stringParamBuilder {
	return newStringParamBuilder(b.provider, paramName)
}

func (b *builderFactory) IntParam(paramName string) *intParamBuilder {
	return newIntParamBuilder(b.provider, paramName)
}

func (b *builderFactory) BoolParam(paramName string) *boolParamBuilder {
	return newBoolParamBuilder(b.provider, paramName)
}
func (b *builderFactory) MapParam(paramName string) *mapParamBuilder {
	return newMapParamBuilder(b.provider, paramName)
}
func (b *builderFactory) ListParam(paramName string) *listParamBuilder {
	return newListParamBuilder(b.provider, paramName)
}
