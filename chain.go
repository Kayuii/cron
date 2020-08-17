package cron

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// JobWrapper decorates the given Job with some behavior.
type JobWrapper func(Job) Job

// Chain is a sequence of JobWrappers that decorates submitted jobs with
// cross-cutting behaviors like logging or synchronization.
type Chain struct {
	wrappers []JobWrapper
}

// NewChain returns a Chain consisting of the given JobWrappers.
func NewChain(c ...JobWrapper) Chain {
	return Chain{c}
}

// Then decorates the given job with all JobWrappers in the chain.
//
// This:
//     NewChain(m1, m2, m3).Then(job)
// is equivalent to:
//     m1(m2(m3(job)))
// Then 主要的作用，是将job放入所需要的wrappers里;
// 当执行时，返回传入job在Wrapper里的真实执行逻辑，
// wrapper里会调用下一个wrapper的Run()，最终调用job的Run() 或者
func (c Chain) Then(j Job) Job {
	for i := range c.wrappers {
		// 将传入的job，做为最后一个的输入，最后一个做为它前一个的输入，一直往前推
		// 这里已经执行了wrappers，得到的j是FuncJob()
		j = c.wrappers[len(c.wrappers)-i-1](j)
	}
	return j
}

// TODO 是否还有其他JobWrapper
// Recover panics in wrapped jobs and log them with the provided logger.
func Recover(logger Logger) JobWrapper {
	return func(j Job) Job {
		return FuncJob(func() {
			defer func() {
				if r := recover(); r != nil {
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					logger.Error(err, "panic", "stack", "...\n"+string(buf))
				}
			}()
			j.Run()
		})
	}
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
func DelayIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		var mu sync.Mutex
		return FuncJob(func() {
			start := time.Now()
			mu.Lock()
			defer mu.Unlock()
			if dur := time.Since(start); dur > time.Minute {
				logger.Info("delay", "duration", dur)
			}
			j.Run()
		})
	}
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is
// still running. It logs skips to the given logger at Info level.
func SkipIfStillRunning(logger Logger) JobWrapper {
	// 返回Job
	return func(j Job) Job {
		var ch = make(chan struct{}, 1)
		ch <- struct{}{} // bug, 一个chain给两个job用，2个job用的是同一个ch，所以只有一个job收到这个ch，另外一个不会真的跑
		fmt.Println("func job", ch)
		// FuncJob 依然是func(),进行这个转换只是为了实现Run方法，符合Job interface
		return FuncJob(func() {
			select {
			case v := <-ch:
				j.Run()
				ch <- v
			default:
				logger.Info("skip")
				fmt.Println("skip")
			}
		})
	}
}
