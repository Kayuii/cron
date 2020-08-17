package cron

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries []*Entry // job执行实体
	// chain 用来定义entry里的warppedJob使用什么逻辑（e.g. skipIfLastRunning）
	// 即一个cron里所有entry只有一个封装逻辑
	chain     Chain
	stop      chan struct{}     // 停止整个cron
	add       chan *Entry       // 增加一个entry
	remove    chan EntryID      // 移除一个entry
	snapshot  chan chan []Entry // 获取entry整体快照
	running   bool              // 代表是否已经在执行，是cron为使用者提供的动态修改entry的接口准备的
	logger    Logger            // 封装golang的log包
	runningMu sync.Mutex        // 用来修改运行中的cron数据，比如增加entry，移除entry
	location  *time.Location    //
	parser    ScheduleParser    // 对时间格式的解析，为interface, 可以定制自己的时间规则。
	nextID    EntryID           // entry的全局ID，新增一个entry就加1
	jobWaiter sync.WaitGroup    // run job时会进行add(1)， job 结束会done()，stop整个cron，以此保证所有job都能退出
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
// Schedule 接口，方法Next返回下次执行的绝对时间，interface可用户自己定制，而Cron自己也有多种时间格式，需要通过Next统一到绝对时间
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	// 唯一id，用于查询和删除
	ID EntryID

	// Schedule on which this job should be run.
	// 本Entry的调度时间，不是绝对时间
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	// 本entry下次需要执行的绝对时间，会一直被更新
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	// 上一次被执行时间，主要用来查询
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	// WrappedJob 是真实执行的Job实体
	// 被封装的含义是Job可以多层嵌套，可以实现基于需要执行Job的额外处理
	// 比如抓取Job异常、如果Job没有返回下一个时间点的Job是还是继续执行还是delay
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	// Job 主要给用户查询
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
// byTime 用来做sort.Sort()入参
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	// 如果为0，将其放到list的最后
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   Time Zone
//     Description: The time zone in which schedules are interpreted
//     Default:     time.Local
//
//   Parser
//     Description: Parser converts cron spec strings into cron.Schedules.
//     Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//   Chain
//     Description: Wrap submitted jobs to customize behavior.
//     Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser, // TODO 怎么同时支持standardParser和customerParser
	}
	// GOOD opts用来通过用户传入的func，对c进行改写，比如parser,location等等
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	// 如果是周期固定的时间，返回是绝对年月日的时间，如果是每月循环，则月是一个自定义的bit，包含所有12个月
	// 如果是每一个时间周期重复，则返回是一个绝对时间区间，比如5分钟等
	// 不同的返回最终都是Schedule的interface，不同的返回都实现Next的方法，供上层统一调用
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
// 生成entry并注册，注意entry是以你的被调用job为粒度，一个job的多次调用策略（比如skip）体现在WrappedJob
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule, // schedule 为调度的时间格式
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		// 如果已经启动，动态加入entry，则通过channel进行加入
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	// entry 以cron job为粒度
	for _, entry := range c.entries {
		// 根据当前时间，拿到下一个调度时间
		// GOOD 此时无论Schedule的实际类型是 ConstantDelaySchedule 或者SpecSchedule，得到的都是当前时间往后的最近一次调度的绝对时间
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	for {
		// Determine the next entry to run.
		// GOOD 进行排序，只需要拿第一个进行时间的定时处理，也就是定时器只需要一个，这是一个典型定时器的实现
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			// 减去now，将时间duration更新到最新
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			// 这几个case，保证了在启动后，c.entries的所有读写操作都在这里进行，就不需要使用到锁
			// 外面的操作只需发channel消息进来
			case now = <-timer.C:
				// 最近的一个entry的执行时间
				now = now.In(c.location)
				c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					c.startJob(e.WrappedJob)
					e.Prev = e.Next
					// GOOD 只需要更新到时后的几个entry
					e.Next = e.Schedule.Next(now)
					c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
				}

			case newEntry := <-c.add:
				timer.Stop()
				// 为什么不再c.add的发送处将entry.next算好，因为可能发过来就过期了
				// 这里收到后，更新now，now是下次启动timer的依据，所以新的entry肯定能够进入当前的loop不会因为中间过程而过期，导致第一个loop执行不到
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				// 读取entry的快照
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				// cron停止
				timer.Stop()
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				// remove 某一个entry，注意now的更新
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)
			}

			break
		}
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}
