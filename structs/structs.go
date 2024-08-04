package structs

type Partition struct {
	Number int32
}

type Topic struct {
	Name            string
	NumOfPartitions int
	Partitions      []Partition
}

type Group struct {
	Name   string
	Topics []Topic
}
