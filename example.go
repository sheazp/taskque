package taskque

import (
	"fmt"
	"strconv"
)

type WorkJobImpl struct {
	Nums  []string
	Total int
}

func (this *WorkJobImpl) Exec() error {
	if len(this.Nums) > 0 {
		num := this.Nums[0]
		this.Nums = this.Nums[1:len(this.Nums)]
		v, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			fmt.Println("Work occur err when doing job: ", num)
			return err
		}
		this.Total += int(v)
		fmt.Printf("Work Add %v ,total %d\n", v, this.Total)
	}
	return nil
}

type WorkTaskImpl struct {
	wj *WorkJobImpl
}

func (this WorkTaskImpl) NextJob() (WorkJob, error) {
	if len(this.wj.Nums) > 0 {
		//var x tq.WorkJob
		//x = this.wj
		return this.wj, nil
	}
	return nil, nil
}

func main() {
	t := WorkTaskImpl{}

	t.wj = &WorkJobImpl{}
	t.wj.Nums = make([]string, 0)
	t.wj.Nums = append(t.wj.Nums, "100")
	t.wj.Nums = append(t.wj.Nums, "78")
	t.wj.Nums = append(t.wj.Nums, "abc")
	t.wj.Nums = append(t.wj.Nums, "56")
	t.wj.Total = 0
	tsk, tskid := NewWorkTask("add", t)
	fmt.Printf("New Task : id = %s\n", tskid)
	AddWorkTask(tsk)

	tsk.Start()
	go WorkMgrRun()

	for {
		var reader string
		fmt.Scanln(&reader)
		fmt.Println("Scan:", reader)
		if reader == "exit" {
			break
		} else if reader == "c" {
			tsk.Start() // continue
		} else if reader == "g" {
			tid, status := tsk.GetTaskStatus()
			fmt.Println("Current status: ", tid, status) // continue
		} else if reader == "a" {

		}
	}

}
