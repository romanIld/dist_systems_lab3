package queue

import "sort"

// registry - реестр всех задач кластера. На лидере - источник истины,
// на фолловерах - реплика. Не потокобезопасен, защищается мьютексом Service.
type registry struct {
	epoch   uint64 // терм лидера, который ведет реестр
	version uint64 // растет при каждом изменении
	tasks   map[int64]*entry
}

type entry struct {
	Task
	version uint64
}

func newRegistry(epoch uint64) *registry {
	return &registry{epoch: epoch, tasks: map[int64]*entry{}}
}

func (r *registry) put(t Task) {
	r.version++
	r.tasks[t.ID] = &entry{Task: t, version: r.version}
}

func (r *registry) get(id int64) (Task, bool) {
	e, ok := r.tasks[id]
	if !ok {
		return Task{}, false
	}
	return e.Task, true
}

// since возвращает задачи, измененные после версии v, по возрастанию ID.
func (r *registry) since(v uint64) []Task {
	out := make([]Task, 0)
	for _, e := range r.tasks {
		if e.version > v {
			out = append(out, e.Task)
		}
	}
	sortTasks(out)
	return out
}

func (r *registry) all() []Task { return r.since(0) }

func (r *registry) maxID() int64 {
	var m int64
	for id := range r.tasks {
		m = max(m, id)
	}
	return m
}

func sortTasks(ts []Task) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
}
