package taskque

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	TSK_INIT      = iota //0初始状态，等待start后，进入排队状态。
	TSK_WAITING          //1排队状态，等待任务执行状态，由任务管理器裁决是否可进入运行状态。可由暂停/中断/初始状态进入
	TSK_RUNNING          //2运行状态，由排队状态进入
	TSK_FINISH           //3完成状态，由运行状态正常执行完成后进入此状态。
	TSK_PAUSE            //4暂停状态，由外部业务触发。可进入停止/排队状态
	TSK_INTERRUPT        //5中断状态，任务执行中出现异常时触发。可进入停止/排队状态
	TSK_END              //6终止状态，任务停止标志，任务将不会再进行
)

type WorkJob interface {
	Exec() error
}

type WorkTaskInterface interface {
	NextJob() (WorkJob, error)
}

type WorkTask struct {
	_ctime_ms int64 /*创建时间 单位ms*/
	_stime_ms int64 /*开启时间 单位ms*/
	_ftime_ms int64 /*完成时间 单位ms*/
	_etime_ms int64 /*中止时间 单位ms*/

	name   string            //任务名称
	taskid string            //任务唯一标识
	status int               //状态
	jobcnt int               //已执行完的子任务数
	impl   WorkTaskInterface //由上层应用实现的接口
	wg     sync.WaitGroup
}

//构造函数
func NewWorkTask(n string, i WorkTaskInterface) (*WorkTask /*任务唯一ID*/, string) {
	tsk := &WorkTask{
		name:      n,
		impl:      i,
		_stime_ms: 0,
		_ftime_ms: 0,
		_etime_ms: 0,
	}
	tsk._ctime_ms = time.Now().UnixNano() / 1e6
	encodedStr := hex.EncodeToString([]byte(n))
	tsk.taskid = fmt.Sprintf("%v-%08x-%08x", encodedStr, tsk._ctime_ms, rand.Int63())

	return tsk, tsk.taskid
}

//任务工作函数
func (this *WorkTask) run() error {
	var job WorkJob = nil
	var err error = nil
	this.wg.Add(1)
	//fmt.Printf("run() status %v\n", this.status)
	this.status = TSK_RUNNING

	for {
		job, err = this.impl.NextJob()
		if err != nil {
			//获取子任务出错，执行中断
			this.status = TSK_INTERRUPT
			break
		}

		if job == nil {
			//没有子任务了，任务执行成功
			this.status = TSK_FINISH
			fmt.Println("Exec TSK_FINISH")
			this._ftime_ms = time.Now().UnixNano() / 1e6
			break
		}

		//执行子任务
		err = job.Exec()
		if err != nil {
			//执行子任务发生错误，执行中断
			this.status = TSK_INTERRUPT
			fmt.Println("Exec TSK_INTERRUPT")
			break
		}

		this.jobcnt++

		//任务暂停或停止跳出循环
		if this.status != TSK_RUNNING {
			break
		}
	}
	//fmt.Println("run exit")
	this.wg.Done()
	return nil
}

//启动任务
func (this *WorkTask) Start() error {
	if this.status != TSK_INIT && this.status != TSK_INTERRUPT {
		return fmt.Errorf("task state is not ready,state(%d)", this.status)
	}

	if this.status == TSK_INIT {
		this._stime_ms = time.Now().UnixNano() / 1e6
	}
	this.status = TSK_WAITING
	fmt.Println("work task start")
	//go this.Run()  //由任务管理器管理启动，并发数可控
	return nil
}

func (this *WorkTask) GetTaskStatus() (string, int) {
	return this.taskid, this.status
}

//暂停任务，等待子任务执行结束
func (this *WorkTask) Pause() error {
	this.status = TSK_PAUSE
	this.wg.Wait()
	return nil
}

//结束任务，等待任务结束
func (this *WorkTask) Stop() error {
	this.status = TSK_END
	this._etime_ms = time.Now().UnixNano() / 1e6
	this.wg.Wait()
	return nil
}
