package client

import (
	"errors"
	"time"

	"github.com/loticket/delayer/delayqueue"
)

type Client struct {
	Ttr int64
	//设置重试次数
	popHandle func(string, string) error

	topic string

	LoopTime time.Duration

	ticker *time.Ticker

	Stop chan struct{}
}

func (c *Client) PopHandle(popHandle func(string, string) error) {
	c.popHandle = popHandle
}

//添加任务
func (c *Client) PushTask(delay int64, body string) error {
	var jobId string = GenUniqueID()
	if delay <= 0 || delay > (1<<31) {
		return errors.New("delay is error")
	}

	var job delayqueue.Job = delayqueue.Job{
		Id:    jobId,
		Delay: delay,
		TTR:   c.Ttr,
		Topic: c.topic,
		Body:  body,
	}

	return c.pushJob(job)
}

func (c *Client) pushJob(job delayqueue.Job) error {
	return delayqueue.Push(job)
}

//轮训执行数据
func (c *Client) PopTaskTicker() {
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.popTask()
			case <-c.Stop:
				break
			}

		}

	}()
}

//轮询队列获取任务
func (c *Client) popTask() {
	var topics []string = []string{c.topic}
	jobInfo, err := delayqueue.Pop(topics)
	if err != nil || jobInfo == nil {
		return
	}

	if err = c.FinishTask(jobInfo.Id); err != nil {
		return
	}

	c.popHandle(jobInfo.Topic, jobInfo.Body) //执行会掉函数

}

//删除任务任务
func (c *Client) deleteTask(id string) error {
	return delayqueue.Remove(id)
}

//完成任务
func (c *Client) FinishTask(id string) error {
	return c.deleteTask(id)
}

//查询任务
func (c *Client) GetTask() {

}

func NewClient(topics string) *Client {
	return &Client{
		Ttr:   3,
		topic: topics,

		LoopTime: time.Second * 1,

		ticker: time.NewTicker(time.Second),

		Stop: make(chan struct{}),
	}
}
