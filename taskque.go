package taskque

import (
	"fmt"
	"time"
)

/*
划拨工作任务：
* 客户划拨操作实际上是创建划拨任务
* 任务执行过程后应刷新任务记录
* 对于同一客户,同一时间仅有一个划拨任务在执行，其它任务在排队中

任务状态:
* 排队--任务排队中
* 执行--任务执行中(用户可手动暂停或中止)
* 成功--任务执行结束,执行成功
* 中止--任务手动中止
* 暂停--任务手动暂停
* 中断--任务执行中断,有错误发生

* 暂停和中断的状态下，用户可以手动
  * 继续执行 -- 任务状态变更为：排队
  * 结束任务 -- 任务状态变更为：

* 划拨任务成功或中止后，设备批次才可查询...
*/

type tsk struct {
	Wt []*WorkTask
}

type TaskMgr struct {
	_MgrRunning bool
	_MaxRunImpl int
	_taskMap    map[string]*tsk
}

func (this *TaskMgr) AddWorkTask(t *WorkTask) error {
	if t == nil {
		return fmt.Errorf("cannot add nil task")
	}
	if len(t.name) < 1 {
		return fmt.Errorf("task name cannot be nil")
	}
	var ptsk *tsk = nil

	if this._taskMap[t.name] == nil {
		this._taskMap[t.name] = &tsk{}
	}
	ptsk = this._taskMap[t.name]
	ptsk.Wt = append(ptsk.Wt, t)
	return nil
}

func (this *TaskMgr) SetMaxRunningTsk(max int) error {
	if max < 1 || max > 256 {
		return fmt.Errorf("max running task invalid")
	}
	this._MaxRunImpl = max
	return nil
}

func (this *TaskMgr) GetMaxRunningTsk() int {
	return this._MaxRunImpl
}

func (this *TaskMgr) Run() error {
	if this._MgrRunning == true {
		return fmt.Errorf("task mgr is running...")
	}
	this._MgrRunning = true
	for {
		for _, tsks := range this._taskMap {
			runningCnt := 0
			waitting := 0
			for _, t := range tsks.Wt {
				if t.status == TSK_WAITING {
					waitting++
				} else if t.status == TSK_RUNNING {
					runningCnt++
				}
			}
			if runningCnt >= this._MaxRunImpl {
				break
			}
			if waitting > 0 {
				for _, t := range tsks.Wt {
					if t.status == TSK_WAITING {
						//fmt.Printf("TSK_WAITING(%v)->Run\n", TSK_WAITING)
						go t.run() //启动排队中的程序
						runningCnt++
						if runningCnt >= this._MaxRunImpl {
							break
						}
					}
				}
			}
		}
		this.clearExpireTask()
		time.Sleep(1000 * time.Millisecond)
	}
	this._MgrRunning = false
	return nil
}

func (this *TaskMgr) clearExpireTask() error {
	cur := time.Now().UnixNano() / 1e6
	for _, tsks := range this._taskMap {
		for i, t := range tsks.Wt {
			if (t._ftime_ms != 0 && t._ftime_ms+3600 < cur) || (t._etime_ms != 0 && t._etime_ms+3600 < cur) {
				//this._taskMap[t.name] = append(this._taskMap[t.name][:i], this._taskMap[t.name][i+1:]...)
				tsks.Wt = append(tsks.Wt[:i], tsks.Wt[i+1:]...)
				i = i - 1
			}
		}
	}
	return nil
}

//
var defMgr = TaskMgr{
	_taskMap: make(map[string]*tsk, 0),
}

func AddWorkTask(t *WorkTask) error {
	return defMgr.AddWorkTask(t)
}

func WorkMgrRun() error {
	defMgr._MaxRunImpl = 1 //一个任务队列，一次最大运行一个实例
	return defMgr.Run()
}

func SetMaxRunningTsk(max int) error {
	return defMgr.SetMaxRunningTsk(max)
}

func GetMaxRunningTsk() int {
	return defMgr.GetMaxRunningTsk()
}

func GetTasks(name string) []*WorkTask {
	return defMgr._taskMap[name].Wt
}

/*
func main() {
	fmt.Println("hello")
}
*/
