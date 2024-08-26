package config

import "time"

type listParamBuilder struct {
	f func() ([]string, error)
	paramBuilder
}

func newListParamBuilder(provider Provider, paramName string) *listParamBuilder {
	var builder listParamBuilder

	builder.provider = provider
	builder.param(paramName)

	return &builder
}

func (t *listParamBuilder) param(paramName string) *listParamBuilder {
	t.f = func() ([]string, error) {
		v, err := t.provider.GetList(paramName)
		return v, err
	}

	return t
}

func (t *listParamBuilder) Build() func() []string {
	return func() []string {
		value, err := t.f()
		if err != nil {
			panic(err)
		} else {
			t.fetchedTime = time.Now()
		}

		return value
	}
}
