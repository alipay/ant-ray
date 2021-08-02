package main

import (
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
    actor_ref.Task("Get")
    fmt.Println("ok!")
}

type Summable int

func (s Summable) Add(n int) int {
    return int(s) + n
}
