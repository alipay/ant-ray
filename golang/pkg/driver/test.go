package main

import (
    "time"

    "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/util"
)
import "fmt"

func main() {
    ray.Init("127.0.0.1:6379", "5241590000000000")
    util.Logger.Infof("finish init")
    actor_ref := ray.Actor((*actor.Count)(nil)).Remote()
    if actor_ref == nil {
        util.Logger.Infof("failed to create actor ref")
    }
    util.Logger.Infof("created actor ref")
    //var f ray.Convert = actor.Count.Increase
    //reflect.ValueOf(actor.Count)
    // reflect.ValueOf(actor.Count.Get)
    _ = actor_ref.Task((*actor.Count).Increase1).Remote().Get()
    values := actor_ref.Task((*actor.Count).Get).Remote().Get()
    for _, v := range values {
        fmt.Println("v:", v)
    }
    fmt.Println("ok!")
    values = actor_ref.Task((*actor.Count).Hello).Remote().Get()
    hello, ok := values[0].(*string)
    if !ok {
        fmt.Println("failed to get string:")
    }
    fmt.Println("get string:%s", *hello)

    time.Sleep(time.Minute * 5)
}

type Summable int

func (s Summable) Add(n int) int {
    return int(s) + n
}
