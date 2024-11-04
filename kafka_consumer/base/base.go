package base

type Module interface {
	Init()
	Run()
	Close()
}
