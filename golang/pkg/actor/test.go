package actor

import (
    "reflect"

    "github.com/ray-project/ray-go-worker/pkg/ray"
)

func init() {
    ray.RegisterType(reflect.TypeOf((*Count)(nil)))
}

type Count struct {
    value int
}

func (c *Count) Init() {

}

func (c *Count) Increase(i int) {
    c.value += i
}

func (c *Count) Get() int {
    return c.value
}
